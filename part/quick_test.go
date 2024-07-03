package part_test

import (
	"testing"
	"testing/quick"

	"github.com/cilium/statedb/part"
	"github.com/stretchr/testify/require"
)

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
		quick.CheckEqual(insert, get, nil),
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

	require.NoError(t, quick.Check(do, nil))
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

	require.NoError(t, quick.Check(insert, nil))
}
