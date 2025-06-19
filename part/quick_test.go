package part_test

import (
	"fmt"
	"maps"
	"slices"
	"testing"
	"testing/quick"

	"github.com/cilium/statedb/part"
	"github.com/stretchr/testify/require"
)

var quickConfig = &quick.Config{
	// Use a higher count in order to hit the Node256 cases.
	MaxCount: 2000,
}

func TestQuick_InsertGetPrefix(t *testing.T) {
	var tree *part.Tree[string]
	insert := func(key, value string) any {
		txn := tree.Txn()
		_, _, watch := txn.InsertWatch([]byte(key), value)
		if watch == nil {
			return "nil watch from InsertWatch()"
		}
		val, watch2, found := txn.Get([]byte(key))
		if !found {
			return "inserted value not found"
		}
		if val != value {
			return fmt.Sprintf("mismatching value %q vs %q", val, value)
		}
		if watch != watch2 {
			return fmt.Sprintf("mismatching channels %p vs %p", watch, watch2)
		}
		tree = txn.Commit()
		return value
	}

	get := func(key, value string) any {
		val, watch, _ := tree.Get([]byte(key))
		if watch == nil {
			panic("nil watch from Get()")
		}
		if val != value {
			return val
		}

		iter, watch := tree.Prefix([]byte(key))
		if watch == nil {
			panic("nil watch from Prefix()")
		}
		_, v, _ := iter.Next()
		return v
	}

	tree = part.New[string]()
	require.NoError(t,
		quick.CheckEqual(insert, get, quickConfig),
	)
	tree = part.New[string](part.RootOnlyWatch)
	require.NoError(t,
		quick.CheckEqual(insert, get, quickConfig),
	)
}

func TestQuick_IteratorReuse(t *testing.T) {
	tree := part.New[string]()

	iterate := func(key, value string, cloneFirst bool) bool {
		_, _, tree = tree.Insert([]byte(key), value)
		v, _, ok := tree.Get([]byte(key))
		if !ok || value != v {
			return false
		}

		prefixIter, _ := tree.Prefix([]byte(key))
		iterators := []*part.Iterator[string]{
			tree.LowerBound([]byte(key)),
			prefixIter,
		}

		for _, iter := range iterators {
			iter2 := iter.Clone()

			collect := func(it *part.Iterator[string]) (out []string) {
				for k, v, ok := it.Next(); ok; k, v, ok = it.Next() {
					out = append(out, string(k)+"="+v)
				}
				return
			}

			var fst, snd []string
			if cloneFirst {
				snd = collect(iter2)
				fst = collect(iter)
			} else {
				fst = collect(iter)
				snd = collect(iter2)
			}

			if !slices.Equal(fst, snd) {
				return false
			}
		}
		return true
	}

	require.NoError(t,
		quick.Check(iterate, quickConfig),
	)
}

func TestQuick_Delete(t *testing.T) {
	tree := part.New[string]()

	do := func(key, value string, delete bool) bool {
		_, _, tree = tree.Insert([]byte(key), value)
		treeAfterInsert := tree
		v, watch, ok := tree.Get([]byte(key))
		if !ok || v != value {
			t.Logf("value not in tree after insert")
			return false
		}

		// delete some of the time to construct different variations of trees.
		if delete {
			_, _, tree = tree.Delete([]byte(key))
			_, _, ok := tree.Get([]byte(key))
			if ok {
				t.Logf("value exists after delete")
				return false
			}

			_, _, ok = treeAfterInsert.Get([]byte(key))
			if !ok {
				t.Logf("value deleted from original")
			}

			// Check that watch channel closed.
			select {
			case <-watch:
			default:
				t.Logf("watch channel not closed")
				return false
			}
		}
		return true
	}

	require.NoError(t, quick.Check(do, quickConfig))
}

func TestQuick_ClosedWatch(t *testing.T) {
	tree := part.New[string]()
	insert := func(key, value string) bool {
		_, _, tree = tree.Insert([]byte(key), value)
		treeAfterInsert := tree

		val, watch, ok := tree.Get([]byte(key))
		if !ok {
			return false
		}
		if val != value {
			return false
		}

		select {
		case <-watch:
			return false
		default:
		}

		// Changing the key makes the channel close.
		_, _, tree = tree.Insert([]byte(key), "x")
		select {
		case <-watch:
		default:
			t.Logf("channel not closed!")
			return false
		}

		// Original tree unaffected.
		val, _, ok = treeAfterInsert.Get([]byte(key))
		if !ok || val != value {
			t.Logf("original changed!")
			return false
		}

		val, _, ok = tree.Get([]byte(key))
		if !ok || val != "x" {
			t.Logf("new tree does not have x!")
			return false
		}

		return true
	}

	require.NoError(t, quick.Check(insert, quickConfig))
}

func TestQuick_Map(t *testing.T) {
	type result struct {
		old, new int
	}

	partMap := part.Map[uint8, int]{}
	partCheck := func(del bool, key uint8, value int) (r result) {
		var found bool
		r.old, found = partMap.Get(key)
		newPartMap := partMap
		if del {
			newPartMap = partMap.Delete(key)
		} else {
			newPartMap = partMap.Set(key, value)
		}
		r.new, _ = newPartMap.Get(key)
		switch {
		case !del && found:
			// Keys equal when updating the value
			require.True(t, partMap.EqualKeys(newPartMap), "EqualKeys %v %v\n%v\n%v",
				r.old, r.new,
				maps.Collect(partMap.All()), maps.Collect(newPartMap.All()),
			)
			// Equal if value stays the same
			require.Equal(t, r.old == r.new, partMap.SlowEqual(newPartMap), "SlowEqual")
		case !del && !found || del && found:
			// Not equal on additions and deletions
			require.False(t, partMap.EqualKeys(newPartMap), "EqualKeys %v %v\n%v\n%v",
				r.old, r.new,
				maps.Collect(partMap.All()), maps.Collect(newPartMap.All()),
			)
			require.False(t, partMap.SlowEqual(newPartMap), "SlowEqual")
		}
		partMap = newPartMap
		return
	}

	// Compare against hash map.
	hashMap := map[uint8]int{}
	hashCheck := func(del bool, key uint8, value int) (r result) {
		r.old = hashMap[key]
		if del {
			delete(hashMap, key)
		} else {
			hashMap[key] = value
		}
		r.new = hashMap[key]
		return
	}

	require.NoError(t, quick.CheckEqual(partCheck, hashCheck, quickConfig))
}
