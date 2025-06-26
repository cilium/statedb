package part_test

import (
	"bytes"
	"fmt"
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
		watchChannels := []<-chan struct{}{}
		for i := range len(key) {
			_, watch := tree.Prefix([]byte(key)[:i])
			watchChannels = append(watchChannels, watch)
		}

		_, watchBefore, _ := tree.Get([]byte(key))
		if watchBefore == nil {
			return "nil watch from Get()"
		}

		txn := tree.Txn()
		_, _, watchInsert := txn.InsertWatch([]byte(key), value)
		if watchInsert == nil {
			return "nil watch from InsertWatch()"
		}

		val, watchAfter, found := txn.Get([]byte(key))
		if !found {
			return "inserted value not found"
		}
		if val != value {
			return fmt.Sprintf("mismatching value %q vs %q", val, value)
		}
		if watchAfter != watchInsert {
			return fmt.Sprintf("mismatching channels %p vs %p", watchAfter, watchInsert)
		}
		tree = txn.Commit()

		select {
		case <-watchBefore:
		default:
			return "Get() watch channel did not close after Insert!"
		}

		// Check that all prefix watch channels closed
		for _, ch := range watchChannels {
			select {
			case <-ch:
			default:
				return "prefix watch channel did not close"
			}
		}

		return value
	}

	get := func(key, value string) any {
		val, watch, found := tree.Get([]byte(key))
		if watch == nil {
			panic("nil watch from Get()")
		}

		for i := range len(key) {
			prefix := []byte(key)[:i]
			iter, watch := tree.Prefix(prefix)
			if watch == nil {
				panic("nil watch from Prefix()")
			}
			if found {
				// [key] exists, so each prefix search with
				// a prefix of the key must return some result.
				k, _, f := iter.Next()
				if !f {
					panic("prefix search returned no result")
				}
				if !bytes.HasPrefix(k, prefix) {
					panic(fmt.Sprintf("prefix search with %q returned key %q", k, prefix))
				}
			}
		}

		if val != value {
			return val
		}

		iter, watch := tree.Prefix([]byte(key))
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
