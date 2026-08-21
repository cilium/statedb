// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package part

import (
	"fmt"
	"math/rand"
	"testing"
)

// BenchmarkSmallWriteTxn measures path-copying costs for small transactions
// against a tree whose second-level nodes have a mean fanout of 96.
func BenchmarkSmallWriteTxn(b *testing.B) {
	const numKeys = 24 * 1024
	rng := rand.New(rand.NewSource(1))
	keys := make([][]byte, 0, numKeys)
	seen := make(map[uint64]struct{}, numKeys)
	for len(keys) < numKeys {
		key := rng.Uint64()
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		keys = append(keys, uint64Key(key))
	}

	for _, batchSize := range []int{1, 2, 4, 8, 16} {
		b.Run(fmt.Sprintf("updates_%d", batchSize), func(b *testing.B) {
			tree := New[int](RootOnlyWatch)
			txn := tree.Txn()
			for i, key := range keys {
				txn.Insert(key, i)
			}
			tree = txn.CommitAndNotify()

			next := 0
			b.ResetTimer()
			for i := range b.N {
				txn := tree.Txn()
				for j := range batchSize {
					idx := (next + j*7919) % len(keys)
					txn.Insert(keys[idx], i)
				}
				tree = txn.CommitAndNotify()
				next = (next + batchSize) % len(keys)
			}
		})
	}
}
