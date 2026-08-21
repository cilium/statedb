// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package part_test

import (
	"encoding/json"
	"slices"
	"testing"

	"github.com/cilium/statedb/part"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

func TestStringSet(t *testing.T) {
	t.Parallel()

	var s part.Set[string]

	assert.False(t, s.Has("nothing"), "Has nothing")
	_, ok := s.First()
	assert.False(t, ok, "empty set has no first value")

	s = s.Set("foo")
	assert.True(t, s.Has("foo"), "Has foo")
	first, ok := s.First()
	assert.True(t, ok, "non-empty set has a first value")
	assert.Equal(t, "foo", first)

	count := 0
	for v := range s.All() {
		assert.Equal(t, "foo", v)
		count++
	}
	assert.Equal(t, 1, count)

	s2 := part.NewSet("bar")

	s3 := s.Union(s2)
	assert.False(t, s.Has("bar"), "s has no bar")
	assert.False(t, s2.Has("foo"), "s2 has no foo")
	assert.True(t, s3.Has("foo"), "s3 has foo")
	assert.True(t, s3.Has("bar"), "s3 has bar")

	values := slices.Collect(s3.All())
	assert.ElementsMatch(t, []string{"foo", "bar"}, values)
	count = 0
	for range s3.All() {
		count++
		break
	}
	assert.Equal(t, 1, count, "iteration stops when the caller breaks")
	first, ok = s3.First()
	assert.True(t, ok)
	assert.Equal(t, "bar", first)

	s4 := s3.Difference(s2)
	assert.False(t, s4.Has("bar"), "s4 has no bar")
	assert.True(t, s4.Has("foo"), "s4 has foo")

	assert.Equal(t, 2, s3.Len())

	s5 := s3.Delete("foo")
	assert.True(t, s3.Has("foo"), "s3 has foo")
	assert.False(t, s5.Has("foo"), "s3 has no foo")

	// Deleting again does the same.
	s5 = s3.Delete("foo")
	assert.False(t, s5.Has("foo"), "s3 has no foo")

	assert.Equal(t, 2, s3.Len())
	assert.Equal(t, 1, s5.Len())
}

func TestSetAllStopsEarly(t *testing.T) {
	t.Parallel()

	count := 0
	for range part.NewSet("first", "second").All() {
		count++
		break
	}
	require.Equal(t, 1, count)
}

func TestNewSetCopiesSingleton(t *testing.T) {
	values := []string{"original"}
	s := part.NewSet(values...)
	values[0] = "changed"

	value, ok := s.First()
	require.True(t, ok)
	require.Equal(t, "original", value)
}

func TestSetJSON(t *testing.T) {
	t.Parallel()
	for _, s := range []part.Set[string]{
		{},
		part.NewSet("foo"),
		part.NewSet("foo", "bar", "baz"),
	} {
		bs, err := json.Marshal(s)
		require.NoError(t, err, "Marshal")

		var s2 part.Set[string]
		err = json.Unmarshal(bs, &s2)
		require.NoError(t, err, "Unmarshal")
		require.True(t, s.Equal(s2), "Equal")
	}
}

func TestSetYAML(t *testing.T) {
	t.Parallel()
	s := part.NewSet("foo", "bar", "baz")

	bs, err := yaml.Marshal(s)
	require.NoError(t, err, "Marshal")
	require.Equal(t, "- bar\n- baz\n- foo\n", string(bs))

	var s2 part.Set[string]
	err = yaml.Unmarshal(bs, &s2)
	require.NoError(t, err, "Unmarshal")
	require.True(t, s.Equal(s2), "Equal")

	var empty part.Set[string]
	bs, err = yaml.Marshal(empty)
	require.NoError(t, err, "Unmarshal")
	require.Equal(t, "[]\n", string(bs))
	require.True(t, s.Equal(s2), "Equal")
}
