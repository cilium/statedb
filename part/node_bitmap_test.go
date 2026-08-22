// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package part

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func TestBitmapNodeTransitions(t *testing.T) {
	tree := New[int](RootOnlyWatch)
	expectedPromotions := map[int]nodeKind{
		2:   nodeKind4,
		5:   nodeKind16,
		17:  nodeKind64,
		65:  nodeKind128,
		129: nodeKind256,
	}
	for i := range 256 {
		_, _, tree = tree.Insert([]byte{byte(i)}, i)
		if want, ok := expectedPromotions[i+1]; ok {
			require.Equal(t, want, tree.root.kind(), "after inserting %d keys", i+1)
		}
	}
	for i := range 256 {
		value, ok := tree.Get([]byte{byte(i)})
		require.True(t, ok)
		require.Equal(t, i, value)
	}

	expectedDemotions := map[int]nodeKind{
		128: nodeKind128,
		64:  nodeKind64,
		16:  nodeKind16,
		4:   nodeKind4,
	}
	for i := 255; i > 0; i-- {
		_, _, tree = tree.Delete([]byte{byte(i)})
		if want, ok := expectedDemotions[i]; ok {
			require.Equal(t, want, tree.root.kind(), "after deleting down to %d keys", i)
		}
		for j := range i {
			value, found := tree.Get([]byte{byte(j)})
			require.True(t, found)
			require.Equal(t, j, value)
		}
	}
}

func TestBitmapIndex(t *testing.T) {
	var bitmap [4]uint64
	keys := []byte{0, 2, 63, 64, 65, 127, 128, 190, 191, 192, 254, 255}
	for _, key := range keys {
		bitmap[key/64] |= uint64(1) << (key % 64)
	}

	for key := range 256 {
		idx, found := bitmapIndex(&bitmap, byte(key))
		wantIdx := 0
		wantFound := false
		for _, present := range keys {
			if int(present) < key {
				wantIdx++
			} else if int(present) == key {
				wantFound = true
			}
		}
		require.Equal(t, wantIdx, idx, "key %d", key)
		require.Equal(t, wantFound, found, "key %d", key)
	}
}

func TestBitmapNodeSizes(t *testing.T) {
	if unsafe.Sizeof(uintptr(0)) != 8 {
		t.Skip("node size assertions are for 64-bit platforms")
	}
	require.Equal(t, uintptr(584), unsafe.Sizeof(node64[bool]{}))
	require.Equal(t, uintptr(1096), unsafe.Sizeof(node128[bool]{}))
	require.Equal(t, uintptr(2088), unsafe.Sizeof(node256[bool]{}))
}
