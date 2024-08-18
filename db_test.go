// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package statedb

import (
	"bytes"
	"context"
	"expvar"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/cilium/hive"
	"github.com/cilium/hive/cell"
	"github.com/cilium/statedb/index"
	"github.com/cilium/statedb/part"
	"github.com/cilium/stream"
)

// Amount of time to wait for the watch channel to close in tests
const watchCloseTimeout = 30 * time.Second

func TestMain(m *testing.M) {
	// Catch any leaks of goroutines from these tests.
	goleak.VerifyTestMain(m)
}

type testObject struct {
	ID   uint64
	Tags part.Set[string]
}

func (t testObject) getID() uint64 {
	return t.ID
}

func (t testObject) String() string {
	return fmt.Sprintf("testObject{ID: %d, Tags: %v}", t.ID, t.Tags)
}

var (
	idIndex = Index[testObject, uint64]{
		Name: "id",
		FromObject: func(t testObject) index.KeySet {
			return index.NewKeySet(index.Uint64(t.ID))
		},
		FromKey: index.Uint64,
		Unique:  true,
	}

	tagsIndex = Index[testObject, string]{
		Name: "tags",
		FromObject: func(t testObject) index.KeySet {
			return index.Set(t.Tags)
		},
		FromKey: index.String,
		Unique:  false,
	}
)

const (
	INDEX_TAGS    = true
	NO_INDEX_TAGS = false
)

// Do not log debug&info level logs in tests.
var logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
	Level: slog.LevelError,
}))

func newTestDB(t testing.TB, secondaryIndexers ...Indexer[testObject]) (*DB, RWTable[testObject], *ExpVarMetrics) {
	metrics := NewExpVarMetrics(false)
	db, table := newTestDBWithMetrics(t, metrics, secondaryIndexers...)
	return db, table, metrics
}

func newTestDBWithMetrics(t testing.TB, metrics Metrics, secondaryIndexers ...Indexer[testObject]) (*DB, RWTable[testObject]) {
	var (
		db *DB
	)
	table, err := NewTable(
		"test",
		idIndex,
		secondaryIndexers...,
	)
	require.NoError(t, err, "NewTable[testObject]")

	h := hive.NewWithOptions(
		hive.Options{Logger: logger},

		cell.Provide(func() Metrics { return metrics }),
		Cell, // DB
		cell.Invoke(func(db_ *DB) {
			err := db_.RegisterTable(table)
			require.NoError(t, err, "RegisterTable failed")

			// Use a short GC interval.
			db_.setGCRateLimitInterval(50 * time.Millisecond)

			db = db_
		}),
	)

	require.NoError(t, h.Start(context.TODO()))
	t.Cleanup(func() {
		assert.NoError(t, h.Stop(context.TODO()))
	})
	return db, table
}

func TestDB_Insert_SamePointer(t *testing.T) {
	db := New()
	require.NoError(t, db.Start(), "Start")
	defer func() { require.NoError(t, db.Stop(), "Stop") }()

	idIndex := Index[*testObject, uint64]{
		Name: "id",
		FromObject: func(t *testObject) index.KeySet {
			return index.NewKeySet(index.Uint64(t.ID))
		},
		FromKey: index.Uint64,
		Unique:  true,
	}
	table, _ := NewTable("test", idIndex)
	require.NoError(t, db.RegisterTable(table), "RegisterTable")

	txn := db.WriteTxn(table)
	obj := &testObject{ID: 1}
	_, _, err := table.Insert(txn, obj)
	require.NoError(t, err, "Insert failed")
	txn.Commit()

	defer func() {
		txn.Abort()
		if err := recover(); err == nil {
			t.Fatalf("Inserting the same object again didn't fatal")
		}
	}()

	// Try to insert the same again. This will panic.
	txn = db.WriteTxn(table)
	_, _, err = table.Insert(txn, obj)
	require.NoError(t, err, "Insert failed")
}

func TestDB_LowerBound_ByRevision(t *testing.T) {
	t.Parallel()

	db, table := newTestDBWithMetrics(t, &NopMetrics{}, tagsIndex)

	{
		txn := db.WriteTxn(table)
		_, _, err := table.Insert(txn, testObject{ID: 42, Tags: part.NewSet("hello", "world")})
		require.NoError(t, err, "Insert failed")
		txn.Commit()

		txn = db.WriteTxn(table)
		_, _, err = table.Insert(txn, testObject{ID: 71, Tags: part.NewSet("foo")})
		require.NoError(t, err, "Insert failed")
		txn.Commit()
	}

	txn := db.ReadTxn()

	iter, watch := table.LowerBoundWatch(txn, ByRevision[testObject](0))
	obj, rev, ok := iter.Next()
	require.True(t, ok, "expected ByRevision(rev1) to return results")
	require.EqualValues(t, 42, obj.ID)
	prevRev := rev
	obj, rev, ok = iter.Next()
	require.True(t, ok)
	require.EqualValues(t, 71, obj.ID)
	require.Greater(t, rev, prevRev)
	_, _, ok = iter.Next()
	require.False(t, ok)

	iter, _ = table.LowerBoundWatch(txn, ByRevision[testObject](prevRev+1))
	obj, _, ok = iter.Next()
	require.True(t, ok, "expected ByRevision(rev2) to return results")
	require.EqualValues(t, 71, obj.ID)
	_, _, ok = iter.Next()
	require.False(t, ok)

	select {
	case <-watch:
		t.Fatalf("expected LowerBound watch to not be closed before changes")
	default:
	}

	{
		txn := db.WriteTxn(table)
		_, _, err := table.Insert(txn, testObject{ID: 71, Tags: part.NewSet("foo", "modified")})
		require.NoError(t, err, "Insert failed")
		txn.Commit()
	}

	select {
	case <-watch:
	case <-time.After(watchCloseTimeout):
		t.Fatalf("expected LowerBound watch to close after changes")
	}

	txn = db.ReadTxn()
	iter, _ = table.LowerBoundWatch(txn, ByRevision[testObject](rev+1))
	obj, _, ok = iter.Next()
	require.True(t, ok, "expected ByRevision(rev2+1) to return results")
	require.EqualValues(t, 71, obj.ID)
	_, _, ok = iter.Next()
	require.False(t, ok)
}

func TestDB_Prefix(t *testing.T) {
	t.Parallel()

	db, table := newTestDBWithMetrics(t, &NopMetrics{}, tagsIndex)

	{
		txn := db.WriteTxn(table)
		_, _, err := table.Insert(txn, testObject{ID: 42, Tags: part.NewSet("a", "b")})
		require.NoError(t, err, "Insert failed")
		_, _, err = table.Insert(txn, testObject{ID: 82, Tags: part.NewSet("abc")})
		require.NoError(t, err, "Insert failed")
		_, _, err = table.Insert(txn, testObject{ID: 71, Tags: part.NewSet("ab")})
		require.NoError(t, err, "Insert failed")
		txn.Commit()
	}

	txn := db.ReadTxn()

	iter, watch := table.PrefixWatch(txn, tagsIndex.Query("ab"))
	require.Equal(t, Collect(Map(iter, testObject.getID)), []uint64{71, 82})

	select {
	case <-watch:
		t.Fatalf("expected Prefix watch to not be closed before any changes")
	default:
	}

	{
		txn := db.WriteTxn(table)
		_, _, err := table.Insert(txn, testObject{ID: 12, Tags: part.NewSet("bc")})
		require.NoError(t, err, "Insert failed")
		txn.Commit()
	}

	select {
	case <-watch:
		t.Fatalf("expected Prefix watch to not be closed before relevant changes")
	default:
	}

	{
		txn := db.WriteTxn(table)
		_, _, err := table.Insert(txn, testObject{ID: 99, Tags: part.NewSet("abcd")})
		require.NoError(t, err, "Insert failed")
		txn.Commit()
	}

	select {
	case <-watch:
	case <-time.After(watchCloseTimeout):
		t.Fatalf("expected Prefix watch to close after relevant changes")
	}

	txn = db.ReadTxn()
	iter = table.Prefix(txn, tagsIndex.Query("ab"))
	require.Equal(t, Collect(Map(iter, testObject.getID)), []uint64{71, 82, 99})
}

func TestDB_Changes(t *testing.T) {
	t.Parallel()

	db, table, metrics := newTestDB(t, tagsIndex)

	{
		txn := db.WriteTxn(table)
		_, _, err := table.Insert(txn, testObject{ID: 42, Tags: part.NewSet("hello", "world")})
		require.NoError(t, err, "Insert failed")
		_, _, err = table.Insert(txn, testObject{ID: 71, Tags: part.NewSet("foo")})
		require.NoError(t, err, "Insert failed")
		_, _, err = table.Insert(txn, testObject{ID: 83, Tags: part.NewSet("bar")})
		require.NoError(t, err, "Insert failed")
		txn.Commit()
	}

	assert.EqualValues(t, table.Revision(db.ReadTxn()), expvarInt(metrics.RevisionVar.Get("test")), "Revision")
	assert.EqualValues(t, 3, expvarInt(metrics.ObjectCountVar.Get("test")), "ObjectCount")
	assert.EqualValues(t, 0, expvarInt(metrics.GraveyardObjectCountVar.Get("test")), "GraveyardObjectCount")

	// Create two change iterators
	wtxn := db.WriteTxn(table)
	iter, err := table.Changes(wtxn)
	require.NoError(t, err, "failed to create ChangeIterator")
	iter2, err := table.Changes(wtxn)
	require.NoError(t, err, "failed to create ChangeIterator")
	wtxn.Commit()

	assert.EqualValues(t, 2, expvarInt(metrics.DeleteTrackerCountVar.Get("test")), "DeleteTrackerCount")

	// The initial watch channel is closed, so users can either iterate first or watch first.
	<-iter.Watch(db.ReadTxn())

	// Delete 2/3 objects
	{
		txn := db.WriteTxn(table)
		old, deleted, err := table.Delete(txn, testObject{ID: 42})
		require.True(t, deleted)
		require.EqualValues(t, 42, old.ID)
		require.NoError(t, err)
		old, deleted, err = table.Delete(txn, testObject{ID: 71})
		require.True(t, deleted)
		require.EqualValues(t, 71, old.ID)
		require.NoError(t, err)
		txn.Commit()

		// Reinsert and redelete to test updating graveyard with existing object.
		txn = db.WriteTxn(table)
		_, _, err = table.Insert(txn, testObject{ID: 71, Tags: part.NewSet("foo")})
		require.NoError(t, err, "Insert failed")
		txn.Commit()

		txn = db.WriteTxn(table)
		_, deleted, err = table.Delete(txn, testObject{ID: 71})
		require.True(t, deleted)
		require.NoError(t, err, "Delete failed")
		txn.Commit()
	}

	// 1 object should exist.
	txn := db.ReadTxn()
	iterAll := table.All(txn)
	objs := Collect(iterAll)
	require.Len(t, objs, 1)

	assert.EqualValues(t, 1, expvarInt(metrics.ObjectCountVar.Get("test")), "ObjectCount")
	assert.EqualValues(t, 2, expvarInt(metrics.GraveyardObjectCountVar.Get("test")), "GraveyardObjectCount")

	// Consume the deletions using the first delete tracker.
	nExist := 0
	nDeleted := 0

	// Observe the objects that existed when the tracker was created.
	for ev, _, ok := iter.Next(); ok; ev, _, ok = iter.Next() {
		if ev.Deleted {
			nDeleted++
		} else {
			nExist++
		}
	}
	assert.Equal(t, 0, nDeleted)
	assert.Equal(t, 3, nExist)

	// Wait for the new changes.
	<-iter.Watch(txn)
	for ev, _, ok := iter.Next(); ok; ev, _, ok = iter.Next() {
		if ev.Deleted {
			nDeleted++
			nExist--
		} else {
			nExist++
		}
	}
	assert.Equal(t, 2, nDeleted)
	assert.Equal(t, 1, nExist)

	// Since the second iterator has not processed the deletions,
	// the graveyard index should still hold them.
	require.False(t, db.graveyardIsEmpty())

	// Consume the deletions using the second iterator.
	nExist = 0
	nDeleted = 0

	for ev, _, ok := iter2.Next(); ok; ev, _, ok = iter2.Next() {
		if ev.Deleted {
			nDeleted++
		} else {
			nExist++
		}
	}
	<-iter2.Watch(txn)

	for ev, _, ok := iter2.Next(); ok; ev, _, ok = iter2.Next() {
		if ev.Deleted {
			nDeleted++
			nExist--
		} else {
			nExist++
		}
	}

	assert.Equal(t, 1, nExist)
	assert.Equal(t, 2, nDeleted)

	// Graveyard will now be GCd.
	eventuallyGraveyardIsEmpty(t, db)

	assert.EqualValues(t, table.Revision(db.ReadTxn()), expvarInt(metrics.RevisionVar.Get("test")), "Revision")
	assert.EqualValues(t, 1, expvarInt(metrics.ObjectCountVar.Get("test")), "ObjectCount")
	assert.EqualValues(t, 0, expvarInt(metrics.GraveyardObjectCountVar.Get("test")), "GraveyardObjectCount")

	// Insert a new object and consume the event
	{
		wtxn := db.WriteTxn(table)
		_, _, err := table.Insert(wtxn, testObject{ID: 88, Tags: part.NewSet("foo")})
		require.NoError(t, err, "Insert failed")
		wtxn.Commit()
	}

	txn = db.ReadTxn()
	<-iter.Watch(txn)
	<-iter2.Watch(txn)

	ev, _, ok := iter.Next()
	assert.True(t, ok)
	assert.EqualValues(t, 88, ev.Object.ID)
	assert.False(t, ev.Deleted)

	ev, _, ok = iter2.Next()
	assert.True(t, ok)
	assert.EqualValues(t, 88, ev.Object.ID)
	assert.False(t, ev.Deleted)

	ev, _, ok = iter.Next()
	assert.False(t, ok)

	ev, _, ok = iter2.Next()
	assert.False(t, ok)

	// After closing the first iterator, deletes are still tracked for second one.
	// Delete the remaining objects.
	iter.Close()

	{
		txn := db.WriteTxn(table)
		require.NoError(t, table.DeleteAll(txn), "DeleteAll failed")
		txn.Commit()
	}
	require.False(t, db.graveyardIsEmpty())

	assert.EqualValues(t, 0, expvarInt(metrics.ObjectCountVar.Get("test")), "ObjectCount")
	assert.EqualValues(t, 2, expvarInt(metrics.GraveyardObjectCountVar.Get("test")), "GraveyardObjectCount")

	// Consume the deletions using the second iterator.
	txn = db.ReadTxn()
	<-iter2.Watch(txn)

	ev, _, ok = iter2.Next()
	assert.True(t, ok)
	assert.True(t, ev.Deleted)

	ev, _, ok = iter2.Next()
	assert.True(t, ok)
	assert.True(t, ev.Deleted)

	ev, _, ok = iter2.Next()
	assert.False(t, ok)

	eventuallyGraveyardIsEmpty(t, db)

	assert.EqualValues(t, 0, expvarInt(metrics.ObjectCountVar.Get("test")), "ObjectCount")
	assert.EqualValues(t, 0, expvarInt(metrics.GraveyardObjectCountVar.Get("test")), "GraveyardObjectCount")

	// After closing the second iterator the deletions no longer go into graveyard.
	iter2.Close()
	{
		txn := db.WriteTxn(table)
		_, _, err := table.Insert(txn, testObject{ID: 78, Tags: part.NewSet("world")})
		require.NoError(t, err, "Insert failed")
		txn.Commit()
		txn = db.WriteTxn(table)
		require.NoError(t, table.DeleteAll(txn), "DeleteAll failed")
		txn.Commit()
	}
	require.True(t, db.graveyardIsEmpty())

	assert.EqualValues(t, 0, expvarInt(metrics.ObjectCountVar.Get("test")), "ObjectCount")
	assert.EqualValues(t, 0, expvarInt(metrics.GraveyardObjectCountVar.Get("test")), "GraveyardObjectCount")
}

func TestDB_Observable(t *testing.T) {
	t.Parallel()

	db, table, _ := newTestDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	events := stream.ToChannel(ctx, Observable(db, table))

	txn := db.WriteTxn(table)
	_, hadOld, err := table.Insert(txn, testObject{ID: uint64(1)})
	require.False(t, hadOld, "Expected no prior object")
	require.NoError(t, err, "Insert failed")
	_, hadOld, err = table.Insert(txn, testObject{ID: uint64(2)})
	require.False(t, hadOld, "Expected no prior object")
	require.NoError(t, err, "Insert failed")
	txn.Commit()

	event := <-events
	require.False(t, event.Deleted, "expected insert")
	require.Equal(t, uint64(1), event.Object.ID)
	event = <-events
	require.False(t, event.Deleted, "expected insert")
	require.Equal(t, uint64(2), event.Object.ID)

	txn = db.WriteTxn(table)
	_, hadOld, err = table.Delete(txn, testObject{ID: uint64(1)})
	require.True(t, hadOld, "Expected that object was deleted")
	require.NoError(t, err, "Delete failed")
	_, hadOld, err = table.Delete(txn, testObject{ID: uint64(2)})
	require.True(t, hadOld, "Expected that object was deleted")
	require.NoError(t, err, "Delete failed")
	txn.Commit()

	event = <-events
	require.True(t, event.Deleted, "expected delete")
	require.Equal(t, uint64(1), event.Object.ID)
	event = <-events
	require.True(t, event.Deleted, "expected delete")
	require.Equal(t, uint64(2), event.Object.ID)

	cancel()
	ev, ok := <-events
	require.False(t, ok, "expected channel to close, got event: %+v", ev)
}

func TestDB_NumObjects(t *testing.T) {
	t.Parallel()

	db, table, _ := newTestDB(t)
	rtxn := db.ReadTxn()
	assert.Equal(t, 0, table.NumObjects(rtxn))

	txn := db.WriteTxn(table)
	assert.Equal(t, 0, table.NumObjects(txn))
	table.Insert(txn, testObject{ID: uint64(1)})
	assert.Equal(t, 1, table.NumObjects(txn))
	table.Insert(txn, testObject{ID: uint64(1)})
	table.Insert(txn, testObject{ID: uint64(2)})
	assert.Equal(t, 2, table.NumObjects(txn))

	assert.Equal(t, 0, table.NumObjects(rtxn))
	txn.Commit()
	assert.Equal(t, 0, table.NumObjects(rtxn))

	rtxn = db.ReadTxn()
	assert.Equal(t, 2, table.NumObjects(rtxn))
}

func TestDB_All(t *testing.T) {
	t.Parallel()

	db, table, _ := newTestDB(t, tagsIndex)

	{
		txn := db.WriteTxn(table)
		_, _, err := table.Insert(txn, testObject{ID: uint64(1)})
		require.NoError(t, err, "Insert failed")
		_, _, err = table.Insert(txn, testObject{ID: uint64(2)})
		require.NoError(t, err, "Insert failed")
		_, _, err = table.Insert(txn, testObject{ID: uint64(3)})
		require.NoError(t, err, "Insert failed")
		iter := table.All(txn)
		objs := Collect(iter)
		require.Len(t, objs, 3)
		require.EqualValues(t, 1, objs[0].ID)
		require.EqualValues(t, 2, objs[1].ID)
		require.EqualValues(t, 3, objs[2].ID)
		txn.Commit()
	}

	txn := db.ReadTxn()
	iter, watch := table.AllWatch(txn)
	objs := Collect(iter)
	require.Len(t, objs, 3)
	require.EqualValues(t, 1, objs[0].ID)
	require.EqualValues(t, 2, objs[1].ID)
	require.EqualValues(t, 3, objs[2].ID)

	select {
	case <-watch:
		t.Fatalf("expected All() watch channel to not close before delete")
	default:
	}

	{
		txn := db.WriteTxn(table)
		_, hadOld, err := table.Delete(txn, testObject{ID: uint64(1)})
		require.True(t, hadOld, "expected object to be deleted")
		require.NoError(t, err, "Delete failed")
		txn.Commit()
	}

	// Prior read transaction not affected by delete.
	iter = table.All(txn)
	objs = Collect(iter)
	require.Len(t, objs, 3)

	select {
	case <-watch:
	case <-time.After(watchCloseTimeout):
		t.Fatalf("expected All() watch channel to close after delete")
	}
}

func TestDB_Modify(t *testing.T) {
	t.Parallel()

	db, table, _ := newTestDB(t, tagsIndex)

	txn := db.WriteTxn(table)

	// Modifying a non-existing object is effectively an Insert.
	_, hadOld, err := table.Modify(txn, testObject{ID: uint64(1), Tags: part.NewSet("foo")}, func(old, new testObject) testObject {
		t.Fatalf("merge unepectedly called")
		return new
	})
	require.NoError(t, err, "Modify failed")
	require.False(t, hadOld, "expected hadOld to be false")

	mergeCalled := false
	_, hadOld, err = table.Modify(txn, testObject{ID: uint64(1)}, func(old, new testObject) testObject {
		mergeCalled = true
		// Merge the old and new tags.
		new.Tags = old.Tags.Set("bar")
		return new
	})
	require.NoError(t, err, "Modify failed")
	require.True(t, hadOld, "expected hadOld to be true")
	require.True(t, mergeCalled, "expected merge() to be called")

	obj, _, found := table.Get(txn, idIndex.Query(1))
	require.True(t, found)
	require.True(t, obj.Tags.Has("foo"))
	require.True(t, obj.Tags.Has("bar"))

	txn.Commit()

	objs := Collect(table.All(db.ReadTxn()))
	require.Len(t, objs, 1)
	require.EqualValues(t, 1, objs[0].ID)
}

func TestDB_Revision(t *testing.T) {
	t.Parallel()

	db, table, _ := newTestDB(t, tagsIndex)

	startRevision := table.Revision(db.ReadTxn())

	// On aborted write transactions the revision remains unchanged.
	txn := db.WriteTxn(table)
	_, _, err := table.Insert(txn, testObject{ID: 1})
	require.NoError(t, err)
	writeRevision := table.Revision(txn) // Returns new, but uncommitted revision
	txn.Abort()
	require.Equal(t, writeRevision, startRevision+1, "revision incremented on Insert")
	readRevision := table.Revision(db.ReadTxn())
	require.Equal(t, startRevision, readRevision, "aborted transaction does not change revision")

	// Committed write transactions increment the revision
	txn = db.WriteTxn(table)
	_, _, err = table.Insert(txn, testObject{ID: 1})
	require.NoError(t, err)
	writeRevision = table.Revision(txn)
	txn.Commit()
	require.Equal(t, writeRevision, startRevision+1, "revision incremented on Insert")
	readRevision = table.Revision(db.ReadTxn())
	require.Equal(t, writeRevision, readRevision, "committed transaction changed revision")
}

func TestDB_GetList(t *testing.T) {
	t.Parallel()

	db, table, _ := newTestDB(t, tagsIndex)

	// Write test objects 1..10 to table with odd/even/odd/... tags.
	{
		txn := db.WriteTxn(table)
		for i := 1; i <= 10; i++ {
			tag := "odd"
			if i%2 == 0 {
				tag = "even"
			}
			_, _, err := table.Insert(txn, testObject{ID: uint64(i), Tags: part.NewSet(tag)})
			require.NoError(t, err)
		}
		// Check that we can query the not-yet-committed write transaction.
		obj, rev, ok := table.Get(txn, idIndex.Query(1))
		require.True(t, ok, "expected Get(1) to return result")
		require.NotZero(t, rev, "expected non-zero revision")
		require.EqualValues(t, obj.ID, 1, "expected first obj.ID to equal 1")
		txn.Commit()
	}

	txn := db.ReadTxn()

	// Test List against the ID index.
	iter := table.List(txn, idIndex.Query(0))
	items := Collect(iter)
	require.Len(t, items, 0, "expected Get(0) to not return results")

	iter = table.List(txn, idIndex.Query(1))
	items = Collect(iter)
	require.Len(t, items, 1, "expected Get(1) to return result")
	require.EqualValues(t, items[0].ID, 1, "expected items[0].ID to equal 1")

	iter, listWatch := table.ListWatch(txn, idIndex.Query(2))
	items = Collect(iter)
	require.Len(t, items, 1, "expected Get(2) to return result")
	require.EqualValues(t, items[0].ID, 2, "expected items[0].ID to equal 2")

	// Test Get/GetWatch against the ID index.
	_, _, ok := table.Get(txn, idIndex.Query(0))
	require.False(t, ok, "expected Get(0) to not return result")

	obj, rev, ok := table.Get(txn, idIndex.Query(1))
	require.True(t, ok, "expected Get(1) to return result")
	require.NotZero(t, rev, "expected non-zero revision")
	require.EqualValues(t, obj.ID, 1, "expected first obj.ID to equal 1")

	obj, rev, getWatch, ok := table.GetWatch(txn, idIndex.Query(2))
	require.True(t, ok, "expected GetWatch(2) to return result")
	require.NotZero(t, rev, "expected non-zero revision")
	require.EqualValues(t, obj.ID, 2, "expected obj.ID to equal 2")

	select {
	case <-getWatch:
		t.Fatalf("GetWatch channel closed before changes")
	case <-listWatch:
		t.Fatalf("List channel closed before changes")
	default:
	}

	// Modify the testObject(2) to trigger closing of the watch channels.
	wtxn := db.WriteTxn(table)
	_, hadOld, err := table.Insert(wtxn, testObject{ID: uint64(2), Tags: part.NewSet("even", "modified")})
	require.True(t, hadOld)
	require.NoError(t, err)
	wtxn.Commit()

	select {
	case <-getWatch:
	case <-time.After(watchCloseTimeout):
		t.Fatalf("GetWatch channel not closed after change")
	}
	select {
	case <-listWatch:
	case <-time.After(watchCloseTimeout):
		t.Fatalf("List channel not closed after change")
	}

	// Since we modified the database, grab a fresh read transaction.
	txn = db.ReadTxn()

	// Test Get and Last against the tags multi-index which will
	// return multiple results.
	obj, rev, _, ok = table.GetWatch(txn, tagsIndex.Query("even"))
	require.True(t, ok, "expected Get(even) to return result")
	require.NotZero(t, rev, "expected non-zero revision")
	require.ElementsMatch(t, obj.Tags.Slice(), []string{"even", "modified"})
	require.EqualValues(t, 2, obj.ID)

	iter = table.List(txn, tagsIndex.Query("odd"))
	items = Collect(iter)
	require.Len(t, items, 5, "expected Get(odd) to return 5 items")
	for i, item := range items {
		require.EqualValues(t, item.ID, i*2+1, "expected items[%d].ID to equal %d", i, i*2+1)
	}
}

func TestDB_CommitAbort(t *testing.T) {
	t.Parallel()

	dbX, table, metrics := newTestDB(t, tagsIndex)
	db := dbX.NewHandle("test-handle")

	txn := db.WriteTxn(table)
	_, _, err := table.Insert(txn, testObject{ID: 123})
	require.NoError(t, err)
	txn.Commit()

	assert.EqualValues(t, table.Revision(db.ReadTxn()), expvarInt(metrics.RevisionVar.Get("test")), "Revision")
	assert.EqualValues(t, 1, expvarInt(metrics.ObjectCountVar.Get("test")), "ObjectCount")
	assert.Greater(t, expvarFloat(metrics.WriteTxnAcquisitionVar.Get("test-handle/test")), 0.0, "WriteTxnAcquisition")
	assert.Greater(t, expvarFloat(metrics.WriteTxnDurationVar.Get("test-handle/test")), 0.0, "WriteTxnDuration")

	obj, rev, ok := table.Get(db.ReadTxn(), idIndex.Query(123))
	require.True(t, ok, "expected Get(1) to return result")
	require.NotZero(t, rev, "expected non-zero revision")
	require.EqualValues(t, obj.ID, 123, "expected obj.ID to equal 123")
	require.Zero(t, obj.Tags.Len(), "expected no tags")

	_, _, err = table.Insert(txn, testObject{ID: 123, Tags: part.NewSet("insert-after-commit")})
	require.ErrorIs(t, err, ErrTransactionClosed)
	txn.Commit() // should be no-op

	txn = db.WriteTxn(table)
	txn.Abort()

	_, _, err = table.Insert(txn, testObject{ID: 123, Tags: part.NewSet("insert-after-abort")})
	require.ErrorIs(t, err, ErrTransactionClosed)
	txn.Commit() // should be no-op

	// Check that insert after commit and insert after abort do not change the
	// table.
	obj, newRev, ok := table.Get(db.ReadTxn(), idIndex.Query(123))
	require.True(t, ok, "expected object to exist")
	require.Equal(t, rev, newRev, "expected unchanged revision")
	require.EqualValues(t, obj.ID, 123, "expected obj.ID to equal 123")
	require.Zero(t, obj.Tags.Len(), "expected no tags")
}

func TestDB_CompareAndSwap_CompareAndDelete(t *testing.T) {
	t.Parallel()

	db, table, _ := newTestDB(t, tagsIndex)

	// Updating a non-existing object fails and nothing is inserted.
	wtxn := db.WriteTxn(table)
	{
		_, hadOld, err := table.CompareAndSwap(wtxn, 1, testObject{ID: 1})
		require.ErrorIs(t, ErrObjectNotFound, err)
		require.False(t, hadOld)

		objs := table.All(wtxn)
		require.Len(t, Collect(objs), 0)

		wtxn.Abort()
	}

	// Insert a test object and retrieve it.
	wtxn = db.WriteTxn(table)
	_, hadOld, err := table.Insert(wtxn, testObject{ID: 1})
	require.False(t, hadOld, "expected Insert to not replace object")
	require.NoError(t, err, "Insert failed")
	wtxn.Commit()

	obj, rev1, ok := table.Get(db.ReadTxn(), idIndex.Query(1))
	require.True(t, ok)

	// Updating an object with matching revision number works
	wtxn = db.WriteTxn(table)
	obj.Tags = part.NewSet("updated") // NOTE: testObject stored by value so no explicit copy needed.
	oldObj, hadOld, err := table.CompareAndSwap(wtxn, rev1, obj)
	require.NoError(t, err)
	require.True(t, hadOld)
	require.EqualValues(t, 1, oldObj.ID)
	wtxn.Commit()

	obj, _, ok = table.Get(db.ReadTxn(), idIndex.Query(1))
	require.True(t, ok)
	require.Equal(t, 1, obj.Tags.Len())
	v, _ := obj.Tags.All().Next()
	require.Equal(t, "updated", v)

	// Updating an object with mismatching revision number fails
	wtxn = db.WriteTxn(table)
	obj.Tags = part.NewSet("mismatch")
	oldObj, hadOld, err = table.CompareAndSwap(wtxn, rev1, obj)
	require.ErrorIs(t, ErrRevisionNotEqual, err)
	require.True(t, hadOld)
	require.EqualValues(t, 1, oldObj.ID)
	wtxn.Commit()

	obj, _, ok = table.Get(db.ReadTxn(), idIndex.Query(1))
	require.True(t, ok)
	require.Equal(t, 1, obj.Tags.Len())
	v, _ = obj.Tags.All().Next()
	require.Equal(t, "updated", v)

	// Deleting an object with mismatching revision number fails
	wtxn = db.WriteTxn(table)
	obj.Tags = part.NewSet("mismatch")
	oldObj, hadOld, err = table.CompareAndDelete(wtxn, rev1, obj)
	require.ErrorIs(t, ErrRevisionNotEqual, err)
	require.True(t, hadOld)
	require.EqualValues(t, 1, oldObj.ID)
	wtxn.Commit()

	obj, rev2, ok := table.Get(db.ReadTxn(), idIndex.Query(1))
	require.True(t, ok)
	require.Equal(t, 1, obj.Tags.Len())
	v, _ = obj.Tags.All().Next()
	require.Equal(t, "updated", v)

	// Deleting with matching revision number works
	wtxn = db.WriteTxn(table)
	obj.Tags = part.NewSet("mismatch")
	oldObj, hadOld, err = table.CompareAndDelete(wtxn, rev2, obj)
	require.NoError(t, err)
	require.True(t, hadOld)
	require.EqualValues(t, 1, oldObj.ID)
	wtxn.Commit()

	_, _, ok = table.Get(db.ReadTxn(), idIndex.Query(1))
	require.False(t, ok)

	// Deleting non-existing object yields not found
	wtxn = db.WriteTxn(table)
	_, hadOld, err = table.CompareAndDelete(wtxn, rev2, obj)
	require.NoError(t, err)
	require.False(t, hadOld)
	wtxn.Abort()
}

func TestDB_ReadAfterWrite(t *testing.T) {
	t.Parallel()

	db, table, _ := newTestDB(t, tagsIndex)

	txn := db.WriteTxn(table)

	require.Len(t, Collect(table.All(txn)), 0)

	_, _, err := table.Insert(txn, testObject{ID: 1})
	require.NoError(t, err, "Insert failed")

	require.Len(t, Collect(table.All(txn)), 1)

	_, hadOld, _ := table.Delete(txn, testObject{ID: 1})
	require.True(t, hadOld)
	require.Len(t, Collect(table.All(txn)), 0)

	_, _, err = table.Insert(txn, testObject{ID: 2})
	require.NoError(t, err, "Insert failed")
	require.Len(t, Collect(table.All(txn)), 1)

	txn.Commit()

	require.Len(t, Collect(table.All(db.ReadTxn())), 1)
}

func TestDB_Initialization(t *testing.T) {
	t.Parallel()

	db, table, _ := newTestDB(t, tagsIndex)

	wtxn := db.WriteTxn(table)
	done1 := table.RegisterInitializer(wtxn, "test1")
	done2 := table.RegisterInitializer(wtxn, "test2")
	wtxn.Commit()

	txn := db.ReadTxn()
	require.False(t, table.Initialized(txn), "Initialized should be false")
	require.Equal(t, []string{"test1", "test2"}, table.PendingInitializers(txn), "test1, test2 should be pending")

	wtxn = db.WriteTxn(table)
	done1(wtxn)
	require.False(t, table.Initialized(txn), "Initialized should be false")
	wtxn.Commit()

	// Old read transaction unaffected.
	require.False(t, table.Initialized(txn), "Initialized should be false")
	require.Equal(t, []string{"test1", "test2"}, table.PendingInitializers(txn), "test1, test2 should be pending")

	txn = db.ReadTxn()
	require.False(t, table.Initialized(txn), "Initialized should be false")
	require.Equal(t, []string{"test2"}, table.PendingInitializers(txn), "test2 should be pending")

	wtxn = db.WriteTxn(table)
	done2(wtxn)
	assert.True(t, table.Initialized(wtxn), "Initialized should be true")
	wtxn.Commit()

	txn = db.ReadTxn()
	require.True(t, table.Initialized(txn), "Initialized should be true")
	require.Empty(t, table.PendingInitializers(txn), "There should be no pending initializers")
}

func TestWriteJSON(t *testing.T) {
	t.Parallel()

	db, table, _ := newTestDB(t, tagsIndex)

	buf := new(bytes.Buffer)
	err := db.ReadTxn().WriteJSON(buf)
	require.NoError(t, err)

	txn := db.WriteTxn(table)
	for i := 1; i <= 10; i++ {
		_, _, err := table.Insert(txn, testObject{ID: uint64(i)})
		require.NoError(t, err)
	}
	txn.Commit()
}

func Test_nonUniqueKey(t *testing.T) {
	// empty keys
	key := encodeNonUniqueKey(nil, nil)
	primary, secondary := decodeNonUniqueKey(key)
	assert.Len(t, primary, 0)
	assert.Len(t, secondary, 0)

	// empty primary
	key = encodeNonUniqueKey(nil, []byte("foo"))
	primary, secondary = decodeNonUniqueKey(key)
	assert.Len(t, primary, 0)
	assert.Equal(t, string(secondary), "foo")

	// empty secondary
	key = encodeNonUniqueKey([]byte("quux"), []byte{})
	primary, secondary = decodeNonUniqueKey(key)
	assert.Equal(t, string(primary), "quux")
	assert.Len(t, secondary, 0)

	// non-empty
	key = encodeNonUniqueKey([]byte("foo"), []byte("quux"))
	primary, secondary = decodeNonUniqueKey(key)
	assert.EqualValues(t, primary, "foo")
	assert.EqualValues(t, secondary, "quux")
}

func eventuallyGraveyardIsEmpty(t testing.TB, db *DB) {
	require.Eventually(t,
		db.graveyardIsEmpty,
		5*time.Second,
		100*time.Millisecond,
		"graveyard not garbage collected")
}

func expvarInt(v expvar.Var) int64 {
	if v, ok := v.(*expvar.Int); ok && v != nil {
		return v.Value()
	}
	return -1
}

func expvarFloat(v expvar.Var) float64 {
	if v, ok := v.(*expvar.Float); ok && v != nil {
		return v.Value()
	}
	return -1
}
