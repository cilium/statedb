package part_test

import (
	"testing"

	"github.com/cilium/statedb/part"
	"github.com/stretchr/testify/assert"
)

func TestStringSet(t *testing.T) {
	s := part.StringSet

	assert.False(t, s.Has("nothing"), "Has nothing")

	s = s.Set("foo")
	assert.True(t, s.Has("foo"), "Has foo")

	iter := s.All()
	v, ok := iter.Next()
	assert.True(t, ok, "Next")
	assert.Equal(t, "foo", v)
	v, ok = iter.Next()
	assert.False(t, ok, "Next")
	assert.Equal(t, "", v)

	s2 := part.NewStringSet("bar")

	s3 := s.Union(s2)
	assert.False(t, s.Has("bar"), "s has no bar")
	assert.False(t, s2.Has("foo"), "s2 has no foo")
	assert.True(t, s3.Has("foo"), "s3 has foo")
	assert.True(t, s3.Has("bar"), "s3 has bar")

	s4 := s3.Difference(s2)
	assert.False(t, s4.Has("bar"), "s4 has no bar")
	assert.True(t, s4.Has("foo"), "s4 has foo")

	assert.Equal(t, 2, s3.Len())

	s3 = s3.Delete("foo")
	assert.False(t, s3.Has("foo"), "s3 has no foo")

	assert.Equal(t, 1, s3.Len())

	xs := s3.Slice()
	assert.Len(t, xs, 1)
	assert.Equal(t, "bar", xs[0])
}
