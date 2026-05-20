// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package statedb

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cilium/statedb/index"
)

type partIndexDuplicateKeyObject struct {
	ID   uint64
	Tags []string
}

func (partIndexDuplicateKeyObject) TableHeader() []string {
	return []string{"ID", "Tags"}
}

func (obj partIndexDuplicateKeyObject) TableRow() []string {
	return []string{strconv.FormatUint(obj.ID, 10), strings.Join(obj.Tags, ",")}
}

func TestPartIndex_ReindexDuplicateKeys(t *testing.T) {
	idIndex := Index[partIndexDuplicateKeyObject, uint64]{
		Name: "id",
		FromObject: func(obj partIndexDuplicateKeyObject) index.KeySet {
			return index.NewKeySet(index.Uint64(obj.ID))
		},
		FromKey: index.Uint64,
		Unique:  true,
	}
	tagIndex := Index[partIndexDuplicateKeyObject, string]{
		Name: "tag",
		FromObject: func(obj partIndexDuplicateKeyObject) index.KeySet {
			return index.StringSlice(obj.Tags)
		},
		FromKey: index.String,
		Unique:  false,
	}

	db := New()
	table, err := NewTable(db, "part-index-test", idIndex, tagIndex)
	require.NoError(t, err)

	wtxn := db.WriteTxn(table)
	_, _, err = table.Insert(wtxn, partIndexDuplicateKeyObject{
		ID:   1,
		Tags: []string{"duplicate", "duplicate"},
	})
	require.NoError(t, err)
	txn := wtxn.Commit()

	require.Len(t, Collect(table.List(txn, tagIndex.Query("duplicate"))), 1)

	var (
		oldObj partIndexDuplicateKeyObject
		hadOld bool
	)
	require.NotPanics(t, func() {
		wtxn = db.WriteTxn(table)
		defer wtxn.Abort()

		oldObj, hadOld, err = table.Insert(wtxn, partIndexDuplicateKeyObject{
			ID:   1,
			Tags: []string{"replacement"},
		})
		if err == nil {
			txn = wtxn.Commit()
		}
	})
	require.NoError(t, err)
	require.True(t, hadOld)
	require.Equal(t, []string{"duplicate", "duplicate"}, oldObj.Tags)

	require.Empty(t, Collect(table.List(txn, tagIndex.Query("duplicate"))))
	objs := Collect(table.List(txn, tagIndex.Query("replacement")))
	require.Len(t, objs, 1)
	require.Equal(t, uint64(1), objs[0].ID)
}
