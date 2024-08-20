// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package index_test

import (
	"maps"
	"slices"
	"testing"

	"github.com/cilium/statedb/index"
	"github.com/stretchr/testify/assert"
)

func TestSeq(t *testing.T) {
	tests := [][]uint64{
		{},
		{1},
		{1, 2, 3},
	}
	for _, keys := range tests {
		expected := []index.Key{}
		for _, x := range keys {
			expected = append(expected, index.Uint64(x))
		}
		keySet := index.Seq(index.Uint64, slices.Values(keys))
		actual := []index.Key{}
		keySet.Foreach(func(k index.Key) {
			actual = append(actual, k)
		})
		assert.ElementsMatch(t, expected, actual)
	}
}

func TestSeq2(t *testing.T) {
	tests := []map[uint64]int{
		nil,
		map[uint64]int{},
		map[uint64]int{1: 1},
		map[uint64]int{1: 1, 2: 2, 3: 3},
	}
	for _, m := range tests {
		expected := []index.Key{}
		for x := range m {
			expected = append(expected, index.Uint64(x))
		}
		keySet := index.Seq2(index.Uint64, maps.All(m))
		actual := []index.Key{}
		keySet.Foreach(func(k index.Key) {
			actual = append(actual, k)
		})
		assert.ElementsMatch(t, expected, actual)
	}
}

type testObj struct {
	x string
}

func (t testObj) String() string {
	return t.x
}

func TestStringerSeq(t *testing.T) {
	tests := [][]testObj{
		{},
		{testObj{"foo"}},
		{testObj{"foo"}, testObj{"bar"}},
	}
	for _, objs := range tests {
		expected := []index.Key{}
		for _, o := range objs {
			expected = append(expected, index.String(o.x))
		}
		keySet := index.StringerSeq(slices.Values(objs))
		actual := []index.Key{}
		keySet.Foreach(func(k index.Key) {
			actual = append(actual, k)
		})
		assert.ElementsMatch(t, expected, actual)
	}
}

func TestStringerSeq2(t *testing.T) {
	tests := []map[testObj]int{
		{},
		{testObj{"foo"}: 1},
		{testObj{"foo"}: 1, testObj{"bar"}: 2},
	}
	for _, m := range tests {
		expected := []index.Key{}
		for o := range m {
			expected = append(expected, index.String(o.x))
		}
		keySet := index.StringerSeq2(maps.All(m))
		actual := []index.Key{}
		keySet.Foreach(func(k index.Key) {
			actual = append(actual, k)
		})
		assert.ElementsMatch(t, expected, actual)
	}
}
