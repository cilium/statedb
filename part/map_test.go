// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package part

import (
	"encoding/json"
	"fmt"
	"iter"
	"maps"
	"math/rand/v2"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

func TestStringMap(t *testing.T) {
	var m Map[string, int]

	//
	// Operations on empty map
	//

	v, ok := m.Get("nonexisting")
	assert.False(t, ok, "Get non-existing")
	assert.Equal(t, 0, v)

	assertIterEmpty := func(it iter.Seq2[string, int]) {
		t.Helper()
		for range it {
			t.Fatalf("expected empty iterator")
		}
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
	assert.Equal(t, 1, m.Len())
	m = FromMap(m, map[string]int{
		"2_two":   2,
		"3_three": 3,
	})
	assert.Equal(t, 3, m.Len())

	// Setting on a copy doesn't affect original
	m.Set("4_four", 4)
	_, ok = m.Get("4_four")
	assert.False(t, ok, "Get non-existing")

	// Getting a non-existing value still does the same.
	v, ok = m.Get("nonexisting")
	assert.False(t, ok, "Get non-existing")
	assert.Equal(t, 0, v)

	for _, kv := range kvs {
		v, ok := m.Get(kv.k)
		assert.True(t, ok, "Get %q", kv.k)
		assert.Equal(t, v, kv.v)
	}

	expected := kvs
	for k, v := range m.All() {
		t.Logf("%v %v", k, v)
		kv := expected[0]
		expected = expected[1:]
		assert.EqualValues(t, kv.k, k)
		assert.EqualValues(t, kv.v, v)
	}
	assert.Empty(t, expected)

	expected = kvs[1:]
	for k, v := range m.LowerBound("2") {
		kv := expected[0]
		expected = expected[1:]
		assert.EqualValues(t, kv.k, k)
		assert.EqualValues(t, kv.v, v)
	}
	assert.Empty(t, expected)

	expected = kvs[1:2]
	for k, v := range m.Prefix("2") {
		kv := expected[0]
		expected = expected[1:]
		assert.EqualValues(t, kv.k, k)
		assert.EqualValues(t, kv.v, v)
	}
	assert.Empty(t, expected)

	assert.Equal(t, 3, m.Len())

	mOld := m
	m = m.Delete(kvs[0].k)
	_, ok = m.Get(kvs[0].k)
	assert.False(t, ok, "Get after Delete")

	_, ok = mOld.Get(kvs[0].k)
	assert.True(t, ok, "Original modified by Delete")
	mOld = mOld.Delete(kvs[0].k)
	_, ok = mOld.Get(kvs[0].k)
	assert.False(t, ok, "Get after Delete")

	assert.Equal(t, 2, m.Len())

	var m3 Map[string, int]
	bs, err := m.MarshalJSON()
	assert.NoError(t, err)
	assert.NoError(t, m3.UnmarshalJSON(bs))
	assert.Equal(t, 2, m3.Len())
	assert.True(t, m.SlowEqual(m3))

	m3 = Map[string, int]{}
	bs, err = yaml.Marshal(m)
	assert.NoError(t, err)
	assert.NoError(t, yaml.Unmarshal(bs, &m3))
	assert.Equal(t, 2, m3.Len())
	assert.True(t, m.SlowEqual(m3))
}

func TestSingletonMap(t *testing.T) {
	var m Map[string, int]

	// check singleton properties
	check := func(m Map[string, int]) {
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
	check(m)

	m = m.Set("one", 1)
	check(m)
	assert.Equal(t, 1, m.Len())
	assert.False(t, m.SlowEqual(Map[string, int]{}))
	assert.True(t, m.SlowEqual(m))

	v, found := m.Get("nope")
	assert.False(t, found)
	assert.Zero(t, v)

	v, found = m.Get("one")
	assert.True(t, found)
	assert.Equal(t, 1, v)

	m2 := m.Set("one", 2)
	check(m2)
	v, found = m.Get("one")
	assert.True(t, found)
	assert.Equal(t, 1, v)
	v, found = m2.Get("one")
	assert.True(t, found)
	assert.Equal(t, 2, v)
	assert.True(t, m.EqualKeys(m2))
	assert.True(t, m2.EqualKeys(m))
	assert.False(t, m.SlowEqual(m2))
	assert.False(t, m2.SlowEqual(m))
	assert.True(t, m2.SlowEqual(m2))
	m2 = m2.Delete("nope")
	check(m2)
	m2 = m2.Delete("one")
	check(m2)
	assert.Equal(t, 0, m2.Len())
	_, found = m2.Get("one")
	assert.False(t, found)
	assert.False(t, m.EqualKeys(m2))
	assert.False(t, m2.EqualKeys(m))
	assert.False(t, m.SlowEqual(m2))
	assert.False(t, m2.SlowEqual(m))
	assert.Equal(t, 0, m2.Len())

	m2 = m2.Set("one", 1)
	check(m2)
	m2 = m2.Set("two", 2)
	check(m2)
	assert.Equal(t, 2, m2.Len())
	m2 = m2.Delete("one")
	check(m2)
	assert.Equal(t, 1, m2.Len())
	m2 = m2.Delete("two")
	check(m2)
	assert.Equal(t, 0, m2.Len())
	check(m2)

	x := maps.Collect(m.Prefix(""))
	assert.Equal(t, 1, x["one"])
	x = maps.Collect(m.Prefix("o"))
	assert.Equal(t, 1, x["one"])
	x = maps.Collect(m.Prefix("one"))
	assert.Equal(t, 1, x["one"])
	x = maps.Collect(m.Prefix("one1"))
	assert.Len(t, x, 0)

	x = maps.Collect(m.LowerBound(""))
	assert.Equal(t, 1, x["one"])
	x = maps.Collect(m.LowerBound("a"))
	assert.Equal(t, 1, x["one"])
	x = maps.Collect(m.LowerBound("one"))
	assert.Equal(t, 1, x["one"])
	x = maps.Collect(m.LowerBound("one1"))
	assert.Len(t, x, 0)

	m2 = Map[string, int]{}
	m2 = FromMap(m2, nil)
	check(m2)
	assert.Equal(t, 0, m2.Len())

	m2 = FromMap(m, nil)
	check(m2)
	assert.True(t, m.SlowEqual(m2))
	assert.True(t, m2.SlowEqual(m))

	m2 = FromMap(m, map[string]int{"one": 2})
	check(m2)
	assert.Equal(t, 1, m2.Len())
	v, found = m2.Get("one")
	assert.True(t, found)
	assert.Equal(t, 2, v)

	m2 = FromMap(m2, map[string]int{"two": 2})
	check(m2)
	assert.Equal(t, 2, m2.Len())
	v, found = m2.Get("one")
	assert.True(t, found)
	assert.Equal(t, 2, v)
	v, found = m2.Get("two")
	assert.True(t, found)
	assert.Equal(t, 2, v)

	var m3 Map[string, int]
	bs, err := m.MarshalJSON()
	assert.NoError(t, err)
	assert.NoError(t, m3.UnmarshalJSON(bs))
	assert.True(t, m.SlowEqual(m3))
	check(m3)

	m3 = Map[string, int]{}
	bs, err = yaml.Marshal(m)
	assert.NoError(t, err)
	assert.NoError(t, yaml.Unmarshal(bs, &m3))
	assert.True(t, m.SlowEqual(m3))
	check(m3)
}

func TestMapTxn(t *testing.T) {
	var m Map[string, int]

	// Empty map
	txn := m.Txn()
	v, found := txn.Get("bar")
	assert.False(t, found)
	assert.Equal(t, 0, v)
	assert.Equal(t, 0, txn.Len())

	tree := txn.Commit()
	assert.Equal(t, 0, tree.Len())
	assert.Nil(t, tree.tree)
	assert.Nil(t, tree.singleton)

	// Add foo=>42
	txn = m.Txn()
	txn.Set("foo", 42)
	assert.Equal(t, 1, txn.Len())
	v, found = txn.Get("foo")
	assert.True(t, found)
	assert.Equal(t, 42, v)
	v, found = txn.Get("bar")
	assert.False(t, found)
	assert.Equal(t, 0, v)

	tree = txn.Commit()
	assert.Equal(t, 1, tree.Len())
	assert.Nil(t, tree.tree)
	assert.NotNil(t, tree.singleton)
	v, found = tree.Get("foo")
	assert.True(t, found)
	assert.Equal(t, 42, v)

	// Set foo=>17, bar=>88
	txn = m.Txn()
	txn.Set("foo", 17)
	txn.Set("bar", 88)

	// Old value should be unmodified
	v, found = tree.Get("foo")
	assert.True(t, found)
	assert.Equal(t, 42, v)

	assert.Equal(t, 2, txn.Len())
	v, found = txn.Get("foo")
	assert.True(t, found)
	assert.Equal(t, 17, v)
	v, found = txn.Get("bar")
	assert.True(t, found)
	assert.Equal(t, 88, v)

	mp := maps.Collect(txn.Prefix(""))
	assert.Len(t, mp, 2)
	assert.Equal(t, map[string]int{"foo": 17, "bar": 88}, mp)
	mp = maps.Collect(txn.Prefix("f"))
	assert.Len(t, mp, 1)
	assert.Equal(t, map[string]int{"foo": 17}, mp)

	mp = maps.Collect(txn.LowerBound(""))
	assert.Len(t, mp, 2)
	assert.Equal(t, map[string]int{"foo": 17, "bar": 88}, mp)
	mp = maps.Collect(txn.LowerBound("c"))
	assert.Len(t, mp, 1)
	assert.Equal(t, map[string]int{"foo": 17}, mp)

	mp = maps.Collect(txn.All())
	assert.Len(t, mp, 2)
	assert.Equal(t, map[string]int{"foo": 17, "bar": 88}, mp)

	tree = txn.Commit()
	assert.Equal(t, 2, tree.Len())
	assert.NotNil(t, tree.tree)
	assert.Nil(t, tree.singleton)
	mp = maps.Collect(tree.All())
	assert.Len(t, mp, 2)
	assert.Equal(t, map[string]int{"foo": 17, "bar": 88}, mp)

	txn = m.Txn()
	txn.Set("foo", 17)
	txn.Set("bar", 88)
	txn.Delete("foo")
	assert.Equal(t, 1, txn.Len())
	v, found = txn.Get("foo")
	assert.False(t, found)
	v, found = txn.Get("bar")
	assert.True(t, found)
	assert.Equal(t, 88, v)
}

func TestUint64Map(t *testing.T) {
	// TestStringMap tests most of the operations. We just check here that
	// fromBytes and toBytes work and can iterate in the right order.
	var m Map[uint64, int]
	m = m.Set(42, 42)
	m = m.Set(55, 55)
	m = m.Set(72, 72)

	v, ok := m.Get(42)
	assert.True(t, ok, "Get 42")
	assert.Equal(t, 42, v)

	count := 0
	expected := []uint64{55, 72}
	for k, v := range m.LowerBound(55) {
		kv := expected[0]
		expected = expected[1:]
		assert.EqualValues(t, kv, k)
		assert.EqualValues(t, kv, v)
		count++
	}
	assert.Equal(t, 2, count)
}

func TestRegisterKeyType(t *testing.T) {
	type testKey struct {
		X string
	}
	RegisterKeyType(func(k testKey) []byte { return []byte(k.X) })

	var m Map[testKey, int]
	m = m.Set(testKey{"hello"}, 123)

	v, ok := m.Get(testKey{"hello"})
	assert.True(t, ok, "Get 'hello'")
	assert.Equal(t, 123, v)

	for k, v := range m.All() {
		assert.Equal(t, testKey{"hello"}, k)
		assert.Equal(t, 123, v)
	}
}

func TestMapJSON(t *testing.T) {
	var m Map[string, int]
	m = m.Set("foo", 1).Set("bar", 2).Set("baz", 3)

	bs, err := json.Marshal(m)
	require.NoError(t, err, "Marshal")

	var m2 Map[string, int]
	err = json.Unmarshal(bs, &m2)
	require.NoError(t, err, "Unmarshal")
	require.True(t, m.SlowEqual(m2), "SlowEqual")
}

func TestMapYAMLStringKey(t *testing.T) {
	var m Map[string, int]

	bs, err := yaml.Marshal(m)
	require.NoError(t, err, "Marshal")
	require.Equal(t, "[]\n", string(bs))

	m = m.Set("foo", 1).Set("bar", 2).Set("baz", 3)

	bs, err = yaml.Marshal(m)
	require.NoError(t, err, "Marshal")
	require.Equal(t, "- k: bar\n  v: 2\n- k: baz\n  v: 3\n- k: foo\n  v: 1\n", string(bs))

	var m2 Map[string, int]
	err = yaml.Unmarshal(bs, &m2)
	require.NoError(t, err, "Unmarshal")
	require.True(t, m.SlowEqual(m2), "SlowEqual")
}

func TestMapYAMLStructKey(t *testing.T) {
	type key struct {
		A int    `yaml:"a"`
		B string `yaml:"b"`
	}
	RegisterKeyType[key](func(k key) []byte {
		return fmt.Appendf(nil, "%d-%s", k.A, k.B)
	})
	var m Map[key, int]
	m = m.Set(key{1, "one"}, 1).Set(key{2, "two"}, 2).Set(key{3, "three"}, 3)

	bs, err := yaml.Marshal(m)
	require.NoError(t, err, "Marshal")

	var m2 Map[key, int]
	err = yaml.Unmarshal(bs, &m2)
	require.NoError(t, err, "Unmarshal")
	require.True(t, m.SlowEqual(m2), "SlowEqual")
}

func Benchmark_Uint64Map_Random(b *testing.B) {
	numItems := 1000
	keys := map[uint64]int{}
	for len(keys) < numItems {
		k := uint64(rand.Int64())
		keys[k] = int(k)
	}
	for b.Loop() {
		var m Map[uint64, int]
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

	for b.Loop() {
		var m Map[uint64, int]
		for i := range numItems {
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

func Benchmark_Uint64Map_Sequential_Insert(b *testing.B) {
	numItems := 1000

	for b.Loop() {
		var m Map[uint64, int]
		for i := range numItems {
			k := uint64(i)
			m = m.Set(k, i)
		}
		if m.Len() != numItems {
			b.Fatalf("expected %d items, got %d", numItems, m.Len())
		}
	}
	b.ReportMetric(float64(numItems*b.N)/b.Elapsed().Seconds(), "items/sec")
}

func Benchmark_Uint64Map_Sequential_Txn_Insert(b *testing.B) {
	numItems := 1000
	for b.Loop() {
		var m Map[uint64, int]
		txn := m.Txn()
		for i := range numItems {
			k := uint64(i)
			txn.Set(k, i)
		}
		m = txn.Commit()
		if m.Len() != numItems {
			b.Fatalf("expected %d items, got %d", numItems, m.Len())
		}
	}
	b.ReportMetric(float64(numItems*b.N)/b.Elapsed().Seconds(), "items/sec")
}

func Benchmark_Uint64Map_Random_Insert(b *testing.B) {
	numItems := 1000
	keys := map[uint64]int{}
	for len(keys) < numItems {
		k := uint64(rand.Int64())
		keys[k] = int(k)
	}
	for b.Loop() {
		var m Map[uint64, int]
		for k, v := range keys {
			m = m.Set(k, v)
		}
		if m.Len() != numItems {
			b.Fatalf("expected %d items, got %d", numItems, m.Len())
		}
	}
	b.ReportMetric(float64(numItems*b.N)/b.Elapsed().Seconds(), "items/sec")
}

func Benchmark_Uint64Map_Random_Txn_Insert(b *testing.B) {
	numItems := 1000
	keys := map[uint64]int{}
	for len(keys) < numItems {
		k := uint64(rand.Int64())
		keys[k] = int(k)
	}
	for b.Loop() {
		var m Map[uint64, int]
		txn := m.Txn()
		for k, v := range keys {
			txn.Set(k, v)
		}
		m = txn.Commit()
		if m.Len() != numItems {
			b.Fatalf("expected %d items, got %d", numItems, m.Len())
		}
	}
	b.ReportMetric(float64(numItems*b.N)/b.Elapsed().Seconds(), "items/sec")
}

func TestMapMemoryUse(t *testing.T) {
	runtime.GC()
	runtime.GC()
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	numMaps := 10000
	maps := make([]Map[uint64, int], numMaps)

	for i := range numMaps {
		maps[i] = maps[i].Set(uint64(1), 1)
	}
	runtime.GC()
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&after)

	perMap := (after.HeapAlloc - before.HeapAlloc) / uint64(len(maps))
	t.Logf("%d bytes per map", perMap)

	// Do some thing with the maps to ensure they weren't GCd.
	for _, m := range maps {
		if m.Len() != 1 {
			t.Fatalf("bad count %d", m.Len())
		}
	}
}

func TestHashMapMemoryUse(t *testing.T) {
	runtime.GC()
	runtime.GC()
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	numMaps := 10000
	maps := make([]map[uint64]int, numMaps)

	for i := range numMaps {
		maps[i] = map[uint64]int{1: 1}
	}
	runtime.GC()
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&after)

	perMap := (after.HeapAlloc - before.HeapAlloc) / uint64(len(maps))
	t.Logf("%d bytes per map", perMap)

	// Do some thing with the maps to ensure they weren't GCd.
	for _, m := range maps {
		if len(m) != 1 {
			t.Fatalf("bad count")
		}
	}
}
