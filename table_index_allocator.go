// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package statedb

// tableIndexAllocationLayout is immutable after the table is created.
// Each PART position has an offset in a single allocation. Other backends
// keep their existing allocation strategy and receive a nil destination.
type tableIndexAllocationLayout struct {
	partOffsets []int
	partCount   int
}

func newTableIndexAllocationLayout(indexes []tableIndex) *tableIndexAllocationLayout {
	layout := &tableIndexAllocationLayout{partOffsets: make([]int, len(indexes))}
	for position, idx := range indexes {
		if _, ok := idx.(*partIndex); ok {
			layout.partOffsets[position] = layout.partCount
			layout.partCount++
		} else {
			layout.partOffsets[position] = -1
		}
	}
	return layout
}

type tableIndexAllocator struct {
	layout      *tableIndexAllocationLayout
	partIndexes []partIndex
}

func (table *tableEntry) allocIndex(position int) tableIndex {
	if !table.hasIndexTxn {
		return nil
	}
	return table.indexAllocator.alloc(position)
}

func (a *tableIndexAllocator) alloc(position int) tableIndex {
	offset := a.layout.partOffsets[position]
	if offset < 0 {
		return nil
	}
	if a.partIndexes == nil {
		a.partIndexes = make([]partIndex, a.layout.partCount)
	}
	return &a.partIndexes[offset]
}
