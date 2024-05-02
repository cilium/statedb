// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package part_test

import (
	"math/rand/v2"
	"testing"

	"github.com/cilium/statedb/part"
	"github.com/stretchr/testify/assert"
)

func TestStringMap(t *testing.T) {
	m := part.NewStringMap[int]()

	//
	// Operations on empty map
	//

	v, ok := m.Get("nonexisting")
	assert.False(t, ok, "Get non-existing")
	assert.Equal(t, 0, v)

	assertIterEmpty := func(iter part.MapIterator[string, int]) {
		t.Helper()
		k, v, ok := iter.Next()
		assert.False(t, ok, "expected empty iterator")
		assert.Equal(t, "", k)
		assert.Equal(t, 0, v)
	}
	assertIterEmpty(m.LowerBound(""))
	assertIterEmpty(m.Prefix(""))
	assertIterEmpty(m.All())

	//
	// Operations on non-empty map
	//

	// Ordered list of key-value pairs we're testing with.
	// Prefix set so that Map keeps them in the same order.
	kvs := []struct {
		k string
		v int
	}{
		{"1_one", 1},
		{"2_two", 2},
		{"3_three", 3},
	}

	// Set some values in two different ways.
	m = m.Set("1_one", 1)
	m = part.FromMap(m, map[string]int{
		"2_two":   2,
		"3_three": 3,
	})

	// Getting a non-existing value still does the same.
	v, ok = m.Get("nonexisting")
	assert.False(t, ok, "Get non-existing")
	assert.Equal(t, 0, v)

	for _, kv := range kvs {
		v, ok := m.Get(kv.k)
		assert.True(t, ok, "Get %q", kv.k)
		assert.Equal(t, v, kv.v)
	}

	iter := m.All()
	for _, kv := range kvs {
		k, v, ok := iter.Next()
		assert.True(t, ok, "All.Next %d", kv.v)
		assert.Equal(t, kv.k, k)
		assert.Equal(t, kv.v, v)
	}
	assert.False(t, ok, "All.Next")

	iter = m.LowerBound("2")
	for _, kv := range kvs[1:] {
		k, v, ok := iter.Next()
		assert.True(t, ok, "LowerBound.Next %d", kv.v)
		assert.Equal(t, kv.k, k)
		assert.Equal(t, kv.v, v)
	}
	_, _, ok = iter.Next()
	assert.False(t, ok, "LowerBound.Next")

	iter = m.Prefix("3")
	for _, kv := range kvs[2:] {
		k, v, ok := iter.Next()
		assert.True(t, ok, "Prefix.Next %d", kv.v)
		assert.Equal(t, kv.k, k)
		assert.Equal(t, kv.v, v)
	}
	assert.False(t, ok, "Prefix.Next")

	assert.Equal(t, 3, m.Len())

	m = m.Delete(kvs[0].k)
	_, ok = m.Get(kvs[0].k)
	assert.False(t, ok, "Get after Delete")

	assert.Equal(t, 2, m.Len())
}

func TestUint64Map(t *testing.T) {
	// TestStringMap tests most of the operations. We just check here that
	// fromBytes and toBytes work and can iterate in the right order.
	m := part.NewUint64Map[int]()
	m = m.Set(42, 42)
	m = m.Set(55, 55)
	m = m.Set(72, 72)

	v, ok := m.Get(42)
	assert.True(t, ok, "Get 42")
	assert.Equal(t, 42, v)

	iter := m.LowerBound(55)
	k, v, ok := iter.Next()
	assert.True(t, ok, "Next")
	assert.EqualValues(t, 55, k)
	assert.EqualValues(t, k, v)

	k, v, ok = iter.Next()
	assert.True(t, ok, "Next")
	assert.EqualValues(t, 72, k)

	_, _, ok = iter.Next()
	assert.False(t, ok)
}

func Benchmark_Uint64Map_Random(b *testing.B) {
	numItems := 1000
	keys := map[uint64]int{}
	for len(keys) < numItems {
		k := uint64(rand.Int64())
		keys[k] = int(k)
	}
	for n := 0; n < b.N; n++ {
		m := part.NewUint64Map[int]()
		for k, v := range keys {
			m = m.Set(k, v)
			v2, ok := m.Get(k)
			if !ok || v != v2 {
				b.Fatalf("Get did not return value")
			}
		}
	}
	b.ReportMetric(float64(numItems*b.N)/b.Elapsed().Seconds(), "items/sec")
}

func Benchmark_Uint64Map_Sequential(b *testing.B) {
	numItems := 1000

	for n := 0; n < b.N; n++ {
		m := part.NewUint64Map[int]()
		for i := 0; i < numItems; i++ {
			k := uint64(i)
			m = m.Set(k, i)
			v, ok := m.Get(k)
			if !ok || v != i {
				b.Fatalf("Get did not return value")
			}
		}
	}
	b.ReportMetric(float64(numItems*b.N)/b.Elapsed().Seconds(), "items/sec")
}
