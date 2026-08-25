// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package part

import (
	"strconv"
	"testing"
)

var (
	setBenchmarkSink  Set[[]byte]
	mapBenchmarkSink  Map[string, int]
	boolBenchmarkSink bool
)

func Benchmark_Set_Singleton_Create(b *testing.B) {
	key := []byte("value")
	for b.Loop() {
		setBenchmarkSink = NewSet(key)
	}
}

func Benchmark_Set_Singleton_Has(b *testing.B) {
	key := []byte("value")
	set := NewSet(key)
	for b.Loop() {
		boolBenchmarkSink = set.Has(key)
	}
}

func Benchmark_StringMap_Txn_Insert(b *testing.B) {
	const count = 1000
	keys := make([]string, count)
	for i := range keys {
		keys[i] = "key-" + strconv.Itoa(i)
	}

	b.ResetTimer()
	for b.Loop() {
		var m Map[string, int]
		txn := m.Txn()
		for i, key := range keys {
			txn.Set(key, i)
		}
		mapBenchmarkSink = txn.Commit()
	}
	b.ReportMetric(float64(count*b.N)/b.Elapsed().Seconds(), "items/sec")
}
