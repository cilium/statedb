// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package statedb

import (
	"testing"

	"github.com/cilium/statedb/index"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFilter(t *testing.T) {
	type testObject struct {
		ID int
	}

	db, _ := NewDB(nil, NewExpVarMetrics(false))
	idIndex := Index[*testObject, int]{
		Name: "id",
		FromObject: func(t *testObject) index.KeySet {
			return index.NewKeySet(index.Int(t.ID))
		},
		FromKey: index.Int,
		Unique:  true,
	}
	table, _ := NewTable("test", idIndex)
	require.NoError(t, db.RegisterTable(table))

	txn := db.WriteTxn(table)
	table.Insert(txn, &testObject{ID: 1})
	table.Insert(txn, &testObject{ID: 2})
	table.Insert(txn, &testObject{ID: 3})
	table.Insert(txn, &testObject{ID: 4})
	table.Insert(txn, &testObject{ID: 5})
	txn.Commit()

	iter, _ := table.All(db.ReadTxn())
	filtered := Collect(
		Map(
			Filter(
				iter,
				func(obj *testObject) bool {
					return obj.ID%2 == 0
				},
			),
			func(obj *testObject) int {
				return obj.ID
			},
		),
	)
	assert.Len(t, filtered, 2)
	assert.Equal(t, filtered, []int{2, 4})
}
