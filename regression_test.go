// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package statedb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cilium/statedb/index"
)

// Test_Regression_29324 tests that Get() on a index.String-based
// unique index only returns exact matches.
// https://github.com/cilium/cilium/issues/29324
func Test_Regression_29324(t *testing.T) {
	type object struct {
		ID  string
		Tag string
	}
	idIndex := Index[object, string]{
		Name: "id",
		FromObject: func(t object) index.KeySet {
			return index.NewKeySet(index.String(t.ID))
		},
		FromKey: index.String,
		Unique:  true,
	}
	tagIndex := Index[object, string]{
		Name: "tag",
		FromObject: func(t object) index.KeySet {
			return index.NewKeySet(index.String(t.Tag))
		},
		FromKey: index.String,
		Unique:  false,
	}

	db, _, _ := newTestDB(t)
	table, err := NewTable("objects", idIndex, tagIndex)
	require.NoError(t, err)
	require.NoError(t, db.RegisterTable(table))

	wtxn := db.WriteTxn(table)
	table.Insert(wtxn, object{"foo", "aa"})
	table.Insert(wtxn, object{"foobar", "aaa"})
	table.Insert(wtxn, object{"baz", "aaaa"})
	wtxn.Commit()

	// Exact match should only return "foo"
	txn := db.ReadTxn()
	iter := table.List(txn, idIndex.Query("foo"))
	items := Collect(iter)
	if assert.Len(t, items, 1, "Get(\"foo\") should return one match") {
		assert.EqualValues(t, "foo", items[0].ID)
	}

	// Partial match on prefix should not return anything
	iter = table.List(txn, idIndex.Query("foob"))
	items = Collect(iter)
	assert.Len(t, items, 0, "Get(\"foob\") should return nothing")

	// Query on non-unique index should only return exact match
	iter = table.List(txn, tagIndex.Query("aa"))
	items = Collect(iter)
	if assert.Len(t, items, 1, "Get(\"aa\") on tags should return one match") {
		assert.EqualValues(t, "foo", items[0].ID)
	}

	// Partial match on prefix should not return anything on non-unique index
	iter = table.List(txn, idIndex.Query("a"))
	items = Collect(iter)
	assert.Len(t, items, 0, "Get(\"a\") should return nothing")
}

// The watch channel returned by Changes() must be a closed one if there
// is anything left to iterate over. Otherwise on partial iteration we'll
// wait on a watch channel that reflects the changes of a full iteration
// and we might be stuck waiting even when there's unprocessed changes.
func Test_Regression_Changes_Watch(t *testing.T) {
	db, table, _ := newTestDB(t)

	wtxn := db.WriteTxn(table)
	changes, err := table.Changes(wtxn)
	require.NoError(t, err, "Changes")
	wtxn.Commit()

	n := 0
	_, _, ok := changes.Next()
	require.False(t, ok, "expected no changes")

	// On first call to Watch() the iterator is refreshed and a closed watch channel
	// is returned.
	watch := changes.Watch(db.ReadTxn())
	select {
	case <-watch:
		t.Fatalf("Changes() watch channel closed, expected it to block until commit")
	default:
	}

	wtxn = db.WriteTxn(table)
	table.Insert(wtxn, testObject{ID: 1})
	table.Insert(wtxn, testObject{ID: 2})
	table.Insert(wtxn, testObject{ID: 3})
	wtxn.Commit()

	// Observe the objects.
	select {
	case <-watch:
	case <-time.After(time.Second):
		t.Fatalf("Changes() watch channel not closed after inserts")
	}
	watch = changes.Watch(db.ReadTxn())
	n = 0
	for change, _, ok := changes.Next(); ok; change, _, ok = changes.Next() {
		require.False(t, change.Deleted, "not deleted")
		n++
	}
	require.Equal(t, 3, n, "expected 3 objects")

	// Delete the objects
	wtxn = db.WriteTxn(table)
	require.NoError(t, table.DeleteAll(wtxn), "DeleteAll")
	wtxn.Commit()

	// Partially observe the changes
	<-watch
	changes.Watch(db.ReadTxn())
	change, _, ok := changes.Next()
	require.True(t, ok)
	require.True(t, change.Deleted, "expected Deleted")

	// Calling Watch again after partially consuming the iterator
	// should return a closed watch channel.
	select {
	case <-changes.Watch(db.ReadTxn()):
	case <-time.After(time.Second):
		t.Fatalf("Changes() watch channel not closed!")
	}

	// Consume the rest of the deletions.
	n = 1
	for change, _, ok := changes.Next(); ok; change, _, ok = changes.Next() {
		require.True(t, change.Deleted, "expected Deleted")
		n++
	}
	require.Equal(t, 3, n, "expected 3 deletions")

}
