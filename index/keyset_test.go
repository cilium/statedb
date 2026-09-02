// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package index_test

import (
	"runtime"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/cilium/statedb/index"
	"github.com/cilium/statedb/part"
)

type testStringer string

func (s testStringer) String() string { return string(s) }

func TestKeySet_Single(t *testing.T) {
	ks := index.NewKeySet([]byte("baz"))
	first, ok := ks.First()
	require.True(t, ok)
	require.EqualValues(t, "baz", first)
	require.True(t, ks.Exists([]byte("baz")))
	require.False(t, ks.Exists([]byte("foo")))
	vs := []index.Key{}
	ks.Foreach(func(bs index.Key) {
		vs = append(vs, bs)
	})
	require.ElementsMatch(t, vs, []index.Key{index.Key("baz")})
}

func TestKeySet_Multi(t *testing.T) {
	ks := index.NewKeySet([]byte("baz"), []byte("quux"))
	first, ok := ks.First()
	require.True(t, ok)
	require.EqualValues(t, "baz", first)
	require.True(t, ks.Exists([]byte("baz")))
	require.True(t, ks.Exists([]byte("quux")))
	require.False(t, ks.Exists([]byte("foo")))
	vs := [][]byte{}
	ks.Foreach(func(bs index.Key) {
		vs = append(vs, bs)
	})
	require.ElementsMatch(t, vs, [][]byte{[]byte("baz"), []byte("quux")})
}

func TestKeySet_DuplicateKeys(t *testing.T) {
	ks := index.NewKeySet([]byte("baz"), []byte("quux"), []byte("baz"))
	first, ok := ks.First()
	require.True(t, ok)
	require.EqualValues(t, "baz", first)
	require.True(t, ks.Exists([]byte("baz")))
	require.True(t, ks.Exists([]byte("quux")))
	require.False(t, ks.Exists([]byte("foo")))
	vs := [][]byte{}
	ks.Foreach(func(bs index.Key) {
		vs = append(vs, bs)
	})
	require.ElementsMatch(t, vs, [][]byte{[]byte("baz"), []byte("quux"), []byte("baz")})
}

func TestKeySet_Empty(t *testing.T) {
	ks := index.NewKeySet()
	require.Zero(t, ks.Len())
	_, ok := ks.First()
	require.False(t, ok)
	require.False(t, ks.Exists(index.Key{}))
	count := 0
	ks.Foreach(func(index.Key) { count++ })
	require.Zero(t, count)
}

func TestKeySet_EmptyKeys(t *testing.T) {
	tests := []struct {
		name string
		key  index.Key
	}{
		{name: "nil", key: nil},
		{name: "non-nil", key: make(index.Key, 0)},
		{name: "String", key: index.String("")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ks := index.NewKeySet(tt.key)
			require.Equal(t, 1, ks.Len())
			require.True(t, ks.Exists(nil))
			require.True(t, ks.Exists(make(index.Key, 0)))

			var keys []index.Key
			ks.Foreach(func(key index.Key) { keys = append(keys, key) })
			require.Len(t, keys, 1)
			require.Len(t, keys[0], 0)
		})
	}
}

func TestKeySet_EmptyKeyWithTail(t *testing.T) {
	for _, emptyKey := range []index.Key{nil, make(index.Key, 0), index.String("")} {
		ks := index.NewKeySet(emptyKey, index.String("foo"))
		var keys []index.Key
		ks.Foreach(func(key index.Key) { keys = append(keys, key) })
		require.Len(t, keys, 2)
		require.True(t, ks.Exists(emptyKey))
		require.True(t, ks.Exists(index.String("foo")))
	}
}

func TestString_EmptyRepresentations(t *testing.T) {
	var backing byte
	withBacking := unsafe.String(&backing, 0)

	for _, s := range []string{"", string([]byte{}), withBacking} {
		key := index.String(s)
		require.Len(t, key, 0)
		ks := index.NewKeySet(key)
		require.True(t, ks.Exists(index.Key{}))
		first, ok := ks.First()
		require.True(t, ok)
		require.True(t, first.Equal(key))
	}
	runtime.KeepAlive(&backing)
}

func TestKeySet_EmptyKeyConstructors(t *testing.T) {
	for _, ks := range []index.KeySet{
		index.StringSlice([]string{"", "foo"}),
		index.StringerSlice([]testStringer{"", "foo"}),
		index.Set(part.NewSet[[]byte](nil)),
	} {
		require.True(t, ks.Exists(nil))
		var keys []index.Key
		ks.Foreach(func(key index.Key) { keys = append(keys, key) })
		require.NotEmpty(t, keys)
	}
}
