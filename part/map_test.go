// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package part_test

import (
	"encoding/json"
	"fmt"
	"iter"
	"math/rand/v2"
	"testing"

	"github.com/cilium/statedb/part"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestStringMap(t *testing.T) {
	var m part.Map[string, int]

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
	m = part.FromMap(m, map[string]int{
		"2_two":   2,
		"3_three": 3,
	})

	// Setting on a copy doeen't affect original
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
}

func TestUint64Map(t *testing.T) {
	// TestStringMap tests most of the operations. We just check here that
	// fromBytes and toBytes work and can iterate in the right order.
	var m part.Map[uint64, int]
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
	part.RegisterKeyType(func(k testKey) []byte { return []byte(k.X) })

	var m part.Map[testKey, int]
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
	var m part.Map[string, int]
	m = m.Set("foo", 1).Set("bar", 2).Set("baz", 3)

	bs, err := json.Marshal(m)
	require.NoError(t, err, "Marshal")

	var m2 part.Map[string, int]
	err = json.Unmarshal(bs, &m2)
	require.NoError(t, err, "Unmarshal")
	require.True(t, m.SlowEqual(m2), "SlowEqual")
}

func TestMapYAMLStringKey(t *testing.T) {
	var m part.Map[string, int]
	m = m.Set("foo", 1).Set("bar", 2).Set("baz", 3)

	bs, err := yaml.Marshal(m)
	require.NoError(t, err, "Marshal")

	var m2 part.Map[string, int]
	err = yaml.Unmarshal(bs, &m2)
	require.NoError(t, err, "Unmarshal")
	require.True(t, m.SlowEqual(m2), "SlowEqual")
}

func TestMapYAMLStructKey(t *testing.T) {
	type key struct {
		A int    `yaml:"a"`
		B string `yaml:"b"`
	}
	part.RegisterKeyType[key](func(k key) []byte {
		return []byte(fmt.Sprintf("%d-%s", k.A, k.B))
	})
	var m part.Map[key, int]
	m = m.Set(key{1, "one"}, 1).Set(key{2, "two"}, 2).Set(key{3, "three"}, 3)

	bs, err := yaml.Marshal(m)
	require.NoError(t, err, "Marshal")

	var m2 part.Map[key, int]
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
	for n := 0; n < b.N; n++ {
		var m part.Map[uint64, int]
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
		var m part.Map[uint64, int]
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
