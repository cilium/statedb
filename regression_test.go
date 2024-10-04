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
	changeIter, err := table.Changes(wtxn)
	require.NoError(t, err, "Changes")
	wtxn.Commit()

	n := 0
	changes, watch := changeIter.Next(db.ReadTxn())
	for change := range changes {
		t.Fatalf("did not expect changes, got: %v", change)
	}

	// The returned watch channel is closed on the first call to Next()
	// as there may have been changes to iterate and we want it to be
	// safe to either partially consume the changes or even block first
	// on the watch channel and only then consume.
	select {
	case <-watch:
	default:
		t.Fatalf("Changes() watch channel not closed")
	}

	// Calling Next() again now will get a proper non-closed watch channel.
	changes, watch = changeIter.Next(db.ReadTxn())
	for change := range changes {
		t.Fatalf("did not expect changes, got: %v", change)
	}
	select {
	case <-watch:
		t.Fatalf("Changes() watch channel unexpectedly closed")
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

	changes, watch = changeIter.Next(db.ReadTxn())
	n = 0
	for change := range changes {
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
	changes, watch = changeIter.Next(db.ReadTxn())
	for change := range changes {
		require.True(t, change.Deleted, "expected Deleted")
		break
	}

	// Calling Next again after partially consuming the iterator
	// should return a closed watch channel.
	changes, watch = changeIter.Next(db.ReadTxn())
	select {
	case <-watch:
	case <-time.After(time.Second):
		t.Fatalf("Changes() watch channel not closed!")
	}

	// Consume the rest of the deletions.
	n = 1
	for change := range changes {
		require.True(t, change.Deleted, "expected Deleted")
		n++
	}
	require.Equal(t, 3, n, "expected 3 deletions")
}

// Prefix and LowerBound searches on non-unique indexes did not properly check
// whether the object was a false positive due to matching on the primary key part
// of the composite key (<secondary><primary><secondary length>). E.g. if the
// composite keys were <a><aa><1> and <aa><b><2> then Prefix("aa") incorrectly
// yielded the <a><aa><1> as it matched partially the primary key <aa>.
//
// Also another issue existed with the ordering of the results due to there being
// no separator between <secondary> and <primary> parts of the composite key.
// E.g. <a><z><1> and <aa><a><2> were yielded in the incorrect order
// <aa><a><2> and <a><z><1>, which implied "aa" < "a"!
func Test_Regression_Prefix_NonUnique(t *testing.T) {
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
	table.Insert(wtxn, object{"aa", "a"})
	table.Insert(wtxn, object{"b", "bb"})
	table.Insert(wtxn, object{"z", "b"})
	wtxn.Commit()

	// The tag index has one object with tag "a", prefix searching
	// "aa" should return nothing.
	txn := db.ReadTxn()
	iter := table.Prefix(txn, tagIndex.Query("aa"))
	items := Collect(iter)
	assert.Len(t, items, 0, "Prefix(\"aa\") should return nothing")

	iter = table.Prefix(txn, tagIndex.Query("a"))
	items = Collect(iter)
	if assert.Len(t, items, 1, "Prefix(\"a\") on tags should return one match") {
		assert.EqualValues(t, "aa", items[0].ID)
	}

	// Check prefix search ordering: should be fully defined by the secondary key.
	iter = table.Prefix(txn, tagIndex.Query("b"))
	items = Collect(iter)
	if assert.Len(t, items, 2, "Prefix(\"b\") on tags should return two matches") {
		assert.EqualValues(t, "z", items[0].ID)
		assert.EqualValues(t, "b", items[1].ID)
	}

	// With LowerBound search on "aa" we should see tags "b" and "bb" (in that order)
	iter = table.LowerBound(txn, tagIndex.Query("aa"))
	items = Collect(iter)
	if assert.Len(t, items, 2, "LowerBound(\"aa\") on tags should return two matches") {
		assert.EqualValues(t, "z", items[0].ID)
		assert.EqualValues(t, "b", items[1].ID)
	}

}
