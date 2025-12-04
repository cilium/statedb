// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package part

import (
	"bytes"
	"encoding/json"
	"fmt"
	"iter"
	"maps"
	"slices"
	"testing"
	"testing/quick"

	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

var quickConfig = &quick.Config{
	// Use a higher count in order to hit the Node256 cases.
	MaxCount: 2000,
}

func TestQuick_InsertGetPrefix(t *testing.T) {
	t.Parallel()

	var tree *Tree[string]
	insert := func(key, value string) any {
		watchChannels := []<-chan struct{}{}
		// Add all possible watch channels for prefixes of the key
		for i := range len(key) {
			_, watch := tree.Prefix([]byte(key)[:i])
			watchChannels = append(watchChannels, watch)
		}
		// Check that root watch channel always closes on modifications.
		watchChannels = append(watchChannels, tree.rootWatch)

		_, watchBefore, _ := tree.Get([]byte(key))
		if watchBefore == nil {
			return "nil watch from Get()"
		}
		watchChannels = append(watchChannels, watchBefore)

		txn := tree.Txn()
		_, _, watchInsert := txn.InsertWatch([]byte(key), value)
		if watchInsert == nil {
			return "nil watch from InsertWatch()"
		}

		val, watchAfter, found := txn.Get([]byte(key))
		if !found {
			return fmt.Sprintf("inserted value not found for %q", key)
		}
		if val != value {
			return fmt.Sprintf("mismatching value %q vs %q", val, value)
		}
		if watchAfter != watchInsert {
			return fmt.Sprintf("mismatching channels %p vs %p", watchAfter, watchInsert)
		}
		tree = txn.CommitAndNotify()

		// Check that all watch channels closed that we expected to
		for _, ch := range watchChannels {
			select {
			case <-ch:
			default:
				return "watch channel did not close"
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

	tree = New[string]()
	require.NoError(t,
		quick.CheckEqual(insert, get, quickConfig),
	)
	tree = New[string](RootOnlyWatch)
	require.NoError(t,
		quick.CheckEqual(insert, get, quickConfig),
	)
}

func TestQuick_IteratorReuse(t *testing.T) {
	t.Parallel()

	tree := New[string]()

	iterate := func(key, value string, cloneFirst bool) bool {
		_, _, tree = tree.Insert([]byte(key), value)
		v, _, ok := tree.Get([]byte(key))
		if !ok || value != v {
			return false
		}

		prefixIter, _ := tree.Prefix([]byte(key))
		iterators := []Iterator[string]{
			tree.LowerBound([]byte(key)),
			prefixIter,
		}

		for _, it := range iterators {
			collect := func(it iter.Seq2[[]byte, string]) (out []string) {
				for k, v := range it {
					out = append(out, string(k)+"="+v)
				}
				return
			}

			fst := collect(it.All)
			snd := collect(it.All)

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
	t.Parallel()

	tree := New[string]()

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
	t.Parallel()

	tree := New[string]()
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
	t.Parallel()

	type result struct {
		old, new int
	}

	checkSingleton := func(m Map[uint8, int]) {
		switch m.Len() {
		case 0:
			require.Nil(t, m.singleton)
			require.Nil(t, m.tree)
		case 1:
			require.NotNil(t, m.singleton)
			require.Nil(t, m.tree)
		default:
			require.Nil(t, m.singleton)
			require.NotNil(t, m.tree)
		}
		if m.singleton != nil {
			require.Nil(t, m.tree, "Tree should not be set if singleton set")
		}
		if m.tree != nil {
			require.Nil(t, m.singleton, "Singleton should not be set if tree set")
		}
	}

	partMap := Map[uint8, int]{}
	partCheck := func(del bool, key uint8, value int) (r result) {
		var found bool
		r.old, found = partMap.Get(key)
		newPartMap := partMap
		if del {
			newPartMap = partMap.Delete(key)
		} else {
			newPartMap = partMap.Set(key, value)
		}
		checkSingleton(newPartMap)

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

		bs, err := json.Marshal(newPartMap)
		require.NoError(t, err, "json.Marshal")
		var m Map[uint8, int]
		require.NoError(t, json.Unmarshal(bs, &m), "json.Unmarshal")
		require.True(t, m.SlowEqual(newPartMap), "SlowEqual after json.Marshal")
		checkSingleton(m)

		m = Map[uint8, int]{}
		bs, err = yaml.Marshal(newPartMap)
		require.NoError(t, err)
		require.NoError(t, yaml.Unmarshal(bs, &m), "yaml.Unmarshal")
		require.True(t, m.SlowEqual(newPartMap), "SlowEqual after yaml.Marshal")
		checkSingleton(m)

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
