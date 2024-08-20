package part_test

import (
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
	tree := part.New[string]()
	insert := func(key, value string) any {
		_, _, tree = tree.Insert([]byte(key), value)
		return value
	}

	get := func(key, value string) any {
		val, _, _ := tree.Get([]byte(key))
		if val != value {
			return val
		}

		iter, _ := tree.Prefix([]byte(key))
		_, v, _ := iter.Next()
		return v
	}

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
