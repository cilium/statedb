// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package part

import (
	"maps"
	"runtime"
	"strings"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func TestLeafSize(t *testing.T) {
	if unsafe.Sizeof(uintptr(0)) != 8 {
		t.Skip("64-bit layout")
	}
	// StateDB stores an interface and revision in each index leaf.
	type object struct {
		data     any
		revision uint64
	}
	require.Equal(t, uintptr(48), unsafe.Sizeof(leaf[object]{}))
}

func TestLeafFullKeyAfterPrefixChanges(t *testing.T) {
	type snapshot struct {
		tree Tree[int]
		want map[string]int
	}
	var snapshots []snapshot
	tree := New[int]()
	want := map[string]int{}
	keys := []string{"abcdef", "abc", "abcdzz", "", "abd", "ab", "z"}
	for i, key := range keys {
		_, _, tree = tree.Insert([]byte(key), i)
		want[key] = i
		snapshots = append(snapshots, snapshot{tree, maps.Clone(want)})
	}
	// Remove siblings and values held by inner nodes, repeatedly merging
	// compressed prefixes back into leaves.
	for _, key := range []string{"abc", "abcdzz", "ab", "abd", "", "z"} {
		_, found, next := tree.Delete([]byte(key))
		require.True(t, found)
		tree = next
		delete(want, key)
		snapshots = append(snapshots, snapshot{tree, maps.Clone(want)})
	}
	runtime.GC()
	for _, snapshot := range snapshots {
		got := map[string]int{}
		for key, value := range snapshot.tree.All {
			got[string(key)] = value
		}
		require.Equal(t, snapshot.want, got)
		for key, wantValue := range snapshot.want {
			value, found := snapshot.tree.Get([]byte(key))
			require.True(t, found)
			require.Equal(t, wantValue, value)
		}
	}
}

func TestPrefixRangesAfterRepresentativeDeletion(t *testing.T) {
	prefix := strings.Repeat("long-prefix/", 32)
	keys := []string{prefix + "a/x", prefix + "a/y", prefix + "b"}
	tree := New[int]()
	for i, key := range keys {
		_, _, tree = tree.Insert([]byte(key), i)
	}
	before := tree
	// Inner nodes may keep their representative key after that entry has
	// been removed. Merge its branch, split above it, and then merge again.
	_, found, tree := tree.Delete([]byte(keys[0]))
	require.True(t, found)
	_, _, tree = tree.Insert([]byte("long-other"), 3)
	_, found, tree = tree.Delete([]byte("long-other"))
	require.True(t, found)
	_, found, tree = tree.Delete([]byte(keys[2]))
	require.True(t, found)
	runtime.GC()
	for i, key := range keys {
		value, found := before.Get([]byte(key))
		require.True(t, found)
		require.Equal(t, i, value)
	}
	for key, value := range tree.All {
		require.Equal(t, keys[1], string(key))
		require.Equal(t, 1, value)
	}
	require.Equal(t, 1, tree.Len())
}

// BenchmarkPrefixMerge repeatedly merges a compressed prefix into an inner
// node and splits it again. Keys are prepared outside the measured loop.
func BenchmarkPrefixMerge(b *testing.B) {
	prefix := strings.Repeat("shared/", 16)
	keys := [][]byte{[]byte(prefix + "a/x"), []byte(prefix + "a/y"), []byte(prefix + "b")}
	tree := New[int]()
	txn := tree.Txn()
	for i, key := range keys {
		txn.Insert(key, i)
	}
	tree = txn.CommitAndNotify()
	b.ReportAllocs()
	for b.Loop() {
		txn := tree.Txn()
		txn.Delete(keys[2])
		txn.Insert(keys[2], 2)
		tree = txn.CommitAndNotify()
	}
}
