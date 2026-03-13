// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package hive

import (
	"encoding/json"
	"fmt"
	"iter"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cilium/statedb"
	"github.com/cilium/statedb/index"
	"github.com/cilium/statedb/part"
)

type testObject struct {
	ID     uint64          `json:"id" yaml:"id"`
	Key    string          `json:"key,omitempty" yaml:"key,omitempty"`
	Prefix netip.Prefix    `yaml:"prefix"`
	Tags   part.Set[string] `json:"tags" yaml:"tags"`
}

func (t *testObject) TableHeader() []string {
	return []string{"ID", "Key", "Prefix", "Tags"}
}

func (t *testObject) TableRow() []string {
	return []string{
		strconv.FormatUint(uint64(t.ID), 10),
		t.Key,
		t.Prefix.String(),
		strings.Join(slices.Collect(t.Tags.All()), ", "),
	}
}

func (t *testObject) MarshalJSON() ([]byte, error) {
	t2 := struct {
		ID   uint64
		Tags part.Set[string]
	}{t.ID, t.Tags}
	return json.Marshal(t2)
}

func (t *testObject) String() string {
	return fmt.Sprintf("testObject{ID: %d, Tags: %v}", t.ID, t.Tags)
}

var (
	idIndex = statedb.Index[*testObject, uint64]{
		Name: "id",
		FromObject: func(t *testObject) index.KeySet {
			return index.NewKeySet(index.Uint64(t.ID))
		},
		FromKey:    index.Uint64,
		FromString: index.Uint64String,
		Unique:     true,
	}

	tagsIndex = statedb.Index[*testObject, string]{
		Name: "tags",
		FromObject: func(t *testObject) index.KeySet {
			return index.Set(t.Tags)
		},
		FromKey:    index.String,
		FromString: index.FromString,
		Unique:     false,
	}

	prefixIndex = statedb.NetIPPrefixIndex[*testObject]{
		Name: "prefix",
		FromObject: func(obj *testObject) iter.Seq[netip.Prefix] {
			return statedb.Just(obj.Prefix)
		},
		Unique: true,
	}
)

func newTestTable(t testing.TB, db *statedb.DB, name string, secondaryIndexers ...statedb.Indexer[*testObject]) statedb.RWTable[*testObject] {
	table, err := statedb.NewTable(
		db,
		name,
		idIndex,
		secondaryIndexers...,
	)
	require.NoError(t, err, "NewTable[testObject]")
	return table
}
