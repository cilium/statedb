// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package statedb

import (
	"bytes"
	"context"
	"errors"
	"expvar"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"go.uber.org/goleak"

	"github.com/cilium/hive"
	"github.com/cilium/hive/cell"
	"github.com/cilium/statedb/index"
	"github.com/cilium/stream"
)

func TestMain(m *testing.M) {
	// Catch any leaks of goroutines from these tests.
	goleak.VerifyTestMain(m)
}

type testObject struct {
	ID   uint64
	Tags []string
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
			return index.StringSlice(t.Tags)
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
	table, err := NewTable[testObject](
		"test",
		idIndex,
		secondaryIndexers...,
	)
	assert.NoError(t, err, "NewTable[testObject]")

	h := hive.NewWithOptions(
		hive.Options{Logger: logger},

		cell.Provide(func() Metrics { return metrics }),
		Cell, // DB
		cell.Invoke(func(db_ *DB) {
			db_.RegisterTable(table)

			// Use a short GC interval.
			db_.setGCRateLimitInterval(50 * time.Millisecond)

			db = db_
		}),
	)

	assert.NoError(t, h.Start(context.TODO()))
	t.Cleanup(func() {
		assert.NoError(t, h.Stop(context.TODO()))
	})
	return db, table
}

func TestDB_Insert_SamePointer(t *testing.T) {
	db, _ := NewDB(nil, NewExpVarMetrics(false))
	idIndex := Index[*testObject, uint64]{
		Name: "id",
		FromObject: func(t *testObject) index.KeySet {
			return index.NewKeySet(index.Uint64(t.ID))
		},
		FromKey: index.Uint64,
		Unique:  true,
	}
	table, _ := NewTable[*testObject]("test", idIndex)
	db.RegisterTable(table)

	txn := db.WriteTxn(table)
	obj := &testObject{ID: 1}
	table.Insert(txn, obj)
	txn.Commit()

	defer func() {
		txn.Abort()
		if err := recover(); err == nil {
			t.Fatalf("Inserting the same object again didn't fatal")
		}
	}()

	// Try to insert the same again. This will panic.
	txn = db.WriteTxn(table)
	table.Insert(txn, obj)
}

func TestDB_LowerBound_ByRevision(t *testing.T) {
	t.Parallel()

	db, table := newTestDBWithMetrics(t, &NopMetrics{}, tagsIndex)

	{
		txn := db.WriteTxn(table)
		table.Insert(txn, testObject{ID: 42, Tags: []string{"hello", "world"}})
		txn.Commit()

		txn = db.WriteTxn(table)
		table.Insert(txn, testObject{ID: 71, Tags: []string{"foo"}})
		txn.Commit()
	}

	txn := db.ReadTxn()

	iter, watch := table.LowerBound(txn, ByRevision[testObject](0))
	obj, rev, ok := iter.Next()
	assert.True(t, ok, "expected ByRevision(rev1) to return results")
	assert.Equal(t, 42, obj.ID)
	prevRev := rev
	obj, rev, ok = iter.Next()
	assert.True(t, ok)
	assert.Equal(t, 71, obj.ID)
	assert.True(t, rev > prevRev)
	_, _, ok = iter.Next()
	assert.False(t, ok)

	iter, _ = table.LowerBound(txn, ByRevision[testObject](prevRev+1))
	obj, _, ok = iter.Next()
	assert.True(t, ok, "expected ByRevision(rev2) to return results")
	assert.Equal(t, 71, obj.ID)
	_, _, ok = iter.Next()
	assert.False(t, ok)

	select {
	case <-watch:
		t.Fatalf("expected LowerBound watch to not be closed before changes")
	default:
	}

	{
		txn := db.WriteTxn(table)
		table.Insert(txn, testObject{ID: 71, Tags: []string{"foo", "modified"}})
		txn.Commit()
	}

	select {
	case <-watch:
	case <-time.After(time.Second):
		t.Fatalf("expected LowerBound watch to close after changes")
	}

	txn = db.ReadTxn()
	iter, _ = table.LowerBound(txn, ByRevision[testObject](rev+1))
	obj, _, ok = iter.Next()
	assert.True(t, ok, "expected ByRevision(rev2+1) to return results")
	assert.Equal(t, 71, obj.ID)
	_, _, ok = iter.Next()
	assert.False(t, ok)

}

func TestDB_DeleteTracker(t *testing.T) {
	t.Parallel()

	db, table, metrics := newTestDB(t, tagsIndex)

	{
		txn := db.WriteTxn(table)
		table.Insert(txn, testObject{ID: 42, Tags: []string{"hello", "world"}})
		table.Insert(txn, testObject{ID: 71, Tags: []string{"foo"}})
		table.Insert(txn, testObject{ID: 83, Tags: []string{"bar"}})
		txn.Commit()
	}

	assert.Equal(t, int64(table.Revision(db.ReadTxn())), expvarInt64(metrics.RevisionVar.Get("test")), "Revision")
	assert.Equal(t, 3, expvarInt64(metrics.ObjectCountVar.Get("test")), "ObjectCount")
	assert.Equal(t, 0, expvarInt64(metrics.GraveyardObjectCountVar.Get("test")), "GraveyardObjectCount")

	// Create two delete trackers
	wtxn := db.WriteTxn(table)
	deleteTracker, err := table.DeleteTracker(wtxn, "test")
	assert.NoError(t, err, "failed to create DeleteTracker")
	deleteTracker2, err := table.DeleteTracker(wtxn, "test2")
	assert.NoError(t, err, "failed to create DeleteTracker")
	wtxn.Commit()

	assert.Equal(t, 2, expvarInt64(metrics.DeleteTrackerCountVar.Get("test")), "DeleteTrackerCount")

	// Delete 2/3 objects
	{
		txn := db.WriteTxn(table)
		old, deleted, err := table.Delete(txn, testObject{ID: 42})
		assert.True(t, deleted)
		assert.Equal(t, 42, old.ID)
		assert.NoError(t, err)
		old, deleted, err = table.Delete(txn, testObject{ID: 71})
		assert.True(t, deleted)
		assert.Equal(t, 71, old.ID)
		assert.NoError(t, err)
		txn.Commit()

		// Reinsert and redelete to test updating graveyard with existing object.
		txn = db.WriteTxn(table)
		table.Insert(txn, testObject{ID: 71, Tags: []string{"foo"}})
		txn.Commit()

		txn = db.WriteTxn(table)
		_, deleted, err = table.Delete(txn, testObject{ID: 71})
		assert.True(t, deleted)
		assert.NoError(t, err)
		txn.Commit()
	}

	// 1 object should exist.
	txn := db.ReadTxn()
	iter, _ := table.All(txn)
	objs := Collect(iter)
	assert.Equal(t, 1, len(objs))

	assert.Equal(t, 1, expvarInt64(metrics.ObjectCountVar.Get("test")), "ObjectCount")
	assert.Equal(t, 2, expvarInt64(metrics.GraveyardObjectCountVar.Get("test")), "GraveyardObjectCount")

	// Consume the deletions using the first delete tracker.
	nExist := 0
	nDeleted := 0
	_, err = deleteTracker.IterateWithError(
		txn,
		func(obj testObject, deleted bool, _ Revision) error {
			if deleted {
				nDeleted++
			} else {
				nExist++
			}
			return nil
		})
	assert.NoError(t, err)
	assert.Equal(t, nDeleted, 2)
	assert.Equal(t, nExist, 1)

	// Since the second delete tracker has not processed the deletions,
	// the graveyard index should still hold them.
	assert.False(t, db.graveyardIsEmpty())

	// Consume the deletions using the second delete tracker, but
	// with a failure first.
	nExist = 0
	nDeleted = 0
	failErr := errors.New("fail")
	_, err = deleteTracker2.IterateWithError(
		txn,
		func(obj testObject, deleted bool, _ Revision) error {
			if deleted {
				nDeleted++
				return failErr
			}
			nExist++
			return nil
		})
	assert.IsError(t, err, failErr)
	assert.Equal(t, nExist, 1) // Existing objects are iterated first.
	assert.Equal(t, nDeleted, 1)
	nExist = 0
	nDeleted = 0

	// Process again, but this time using Iterate (retrying the failed revision)
	_ = deleteTracker2.Iterate(
		txn,
		func(obj testObject, deleted bool, _ Revision) {
			if deleted {
				nDeleted++
			} else {
				nExist++
			}
		})
	assert.Equal(t, nDeleted, 2)
	assert.Equal(t, nExist, 0) // This was already processed.

	// Graveyard will now be GCd.
	eventuallyGraveyardIsEmpty(t, db)

	assert.Equal(t, int64(table.Revision(db.ReadTxn())), expvarInt64(metrics.RevisionVar.Get("test")), "Revision")
	assert.Equal(t, 1, expvarInt64(metrics.ObjectCountVar.Get("test")), "ObjectCount")
	assert.Equal(t, 0, expvarInt64(metrics.GraveyardObjectCountVar.Get("test")), "GraveyardObjectCount")

	// After closing the first delete tracker, deletes are still tracked for second one.
	// Delete the last remaining object.
	deleteTracker.Close()
	{
		txn := db.WriteTxn(table)
		table.DeleteAll(txn)
		txn.Commit()
	}
	assert.False(t, db.graveyardIsEmpty())

	assert.Equal(t, 0, expvarInt64(metrics.ObjectCountVar.Get("test")), "ObjectCount")
	assert.Equal(t, 1, expvarInt64(metrics.GraveyardObjectCountVar.Get("test")), "GraveyardObjectCount")

	// And finally after closing the second tracker deletions are no longer tracked.
	deleteTracker2.Mark(table.Revision(db.ReadTxn()))
	eventuallyGraveyardIsEmpty(t, db)

	assert.Equal(t, 0, expvarInt64(metrics.ObjectCountVar.Get("test")), "ObjectCount")
	assert.Equal(t, 0, expvarInt64(metrics.GraveyardObjectCountVar.Get("test")), "GraveyardObjectCount")

	deleteTracker2.Close()
	{
		txn := db.WriteTxn(table)
		table.Insert(txn, testObject{ID: 78, Tags: []string{"world"}})
		txn.Commit()
		txn = db.WriteTxn(table)
		table.DeleteAll(txn)
		txn.Commit()
	}
	assert.True(t, db.graveyardIsEmpty())

	assert.Equal(t, 0, expvarInt64(metrics.ObjectCountVar.Get("test")), "ObjectCount")
	assert.Equal(t, 0, expvarInt64(metrics.GraveyardObjectCountVar.Get("test")), "GraveyardObjectCount")
}

func TestDB_Observable(t *testing.T) {
	t.Parallel()

	db, table, _ := newTestDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	events := stream.ToChannel(ctx, Observable[testObject](db, table))

	txn := db.WriteTxn(table)
	table.Insert(txn, testObject{ID: uint64(1)})
	table.Insert(txn, testObject{ID: uint64(2)})
	txn.Commit()

	event := <-events
	assert.False(t, event.Deleted, "expected insert")
	assert.Equal(t, uint64(1), event.Object.ID)
	event = <-events
	assert.False(t, event.Deleted, "expected insert")
	assert.Equal(t, uint64(2), event.Object.ID)

	txn = db.WriteTxn(table)
	table.Delete(txn, testObject{ID: uint64(1)})
	table.Delete(txn, testObject{ID: uint64(2)})
	txn.Commit()

	event = <-events
	assert.True(t, event.Deleted, "expected delete")
	assert.Equal(t, uint64(1), event.Object.ID)
	event = <-events
	assert.True(t, event.Deleted, "expected delete")
	assert.Equal(t, uint64(2), event.Object.ID)

	cancel()
	ev, ok := <-events
	assert.False(t, ok, "expected channel to close, got event: %+v", ev)
}

func TestDB_All(t *testing.T) {
	t.Parallel()

	db, table, _ := newTestDB(t, tagsIndex)

	{
		txn := db.WriteTxn(table)
		table.Insert(txn, testObject{ID: uint64(1)})
		table.Insert(txn, testObject{ID: uint64(2)})
		table.Insert(txn, testObject{ID: uint64(3)})
		iter, _ := table.All(txn)
		objs := Collect(iter)
		assert.Equal(t, 3, len(objs))
		assert.Equal(t, 1, objs[0].ID)
		assert.Equal(t, 2, objs[1].ID)
		assert.Equal(t, 3, objs[2].ID)
		txn.Commit()
	}

	txn := db.ReadTxn()
	iter, watch := table.All(txn)
	objs := Collect(iter)
	assert.Equal(t, 3, len(objs))
	assert.Equal(t, 1, objs[0].ID)
	assert.Equal(t, 2, objs[1].ID)
	assert.Equal(t, 3, objs[2].ID)

	select {
	case <-watch:
		t.Fatalf("expected All() watch channel to not close before delete")
	default:
	}

	{
		txn := db.WriteTxn(table)
		table.Delete(txn, testObject{ID: uint64(1)})
		txn.Commit()
	}

	// Prior read transaction not affected by delete.
	iter, _ = table.All(txn)
	objs = Collect(iter)
	assert.Equal(t, 3, len(objs))

	select {
	case <-watch:
	case <-time.After(time.Second):
		t.Fatalf("expected All() watch channel to close after delete")
	}
}

func TestDB_Revision(t *testing.T) {
	t.Parallel()

	db, table, _ := newTestDB(t, tagsIndex)

	startRevision := table.Revision(db.ReadTxn())

	// On aborted write transactions the revision remains unchanged.
	txn := db.WriteTxn(table)
	_, _, err := table.Insert(txn, testObject{ID: 1})
	assert.NoError(t, err)
	writeRevision := table.Revision(txn) // Returns new, but uncommitted revision
	txn.Abort()
	assert.Equal(t, writeRevision, startRevision+1, "revision incremented on Insert")
	readRevision := table.Revision(db.ReadTxn())
	assert.Equal(t, startRevision, readRevision, "aborted transaction does not change revision")

	// Committed write transactions increment the revision
	txn = db.WriteTxn(table)
	_, _, err = table.Insert(txn, testObject{ID: 1})
	assert.NoError(t, err)
	writeRevision = table.Revision(txn)
	txn.Commit()
	assert.Equal(t, writeRevision, startRevision+1, "revision incremented on Insert")
	readRevision = table.Revision(db.ReadTxn())
	assert.Equal(t, writeRevision, readRevision, "committed transaction changed revision")
}

func TestDB_GetFirstLast(t *testing.T) {
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
			_, _, err := table.Insert(txn, testObject{ID: uint64(i), Tags: []string{tag}})
			assert.NoError(t, err)
		}
		// Check that we can query the not-yet-committed write transaction.
		obj, rev, ok := table.First(txn, idIndex.Query(1))
		assert.True(t, ok, "expected First(1) to return result")
		assert.NotZero(t, rev, "expected non-zero revision")
		assert.Equal(t, obj.ID, 1, "expected first obj.ID to equal 1")
		obj, rev, ok = table.Last(txn, idIndex.Query(1))
		assert.True(t, ok, "expected Last(1) to return result")
		assert.NotZero(t, rev, "expected non-zero revision")
		assert.Equal(t, obj.ID, 1, "expected last obj.ID to equal 1")
		txn.Commit()
	}

	txn := db.ReadTxn()

	// Test Get against the ID index.
	iter, _ := table.Get(txn, idIndex.Query(0))
	items := Collect(iter)
	assert.Zero(t, len(items), "expected Get(0) to not return results")

	iter, _ = table.Get(txn, idIndex.Query(1))
	items = Collect(iter)
	assert.Equal(t, 1, len(items), "expected Get(1) to return result")
	assert.Equal(t, items[0].ID, 1, "expected items[0].ID to equal 1")

	iter, getWatch := table.Get(txn, idIndex.Query(2))
	items = Collect(iter)
	assert.Equal(t, 1, len(items), "expected Get(2) to return result")
	assert.Equal(t, items[0].ID, 2, "expected items[0].ID to equal 2")

	// Test First/FirstWatch and Last/LastWatch against the ID index.
	_, _, ok := table.First(txn, idIndex.Query(0))
	assert.False(t, ok, "expected First(0) to not return result")

	_, _, ok = table.Last(txn, idIndex.Query(0))
	assert.False(t, ok, "expected Last(0) to not return result")

	obj, rev, ok := table.First(txn, idIndex.Query(1))
	assert.True(t, ok, "expected First(1) to return result")
	assert.NotZero(t, rev, "expected non-zero revision")
	assert.Equal(t, obj.ID, 1, "expected first obj.ID to equal 1")

	obj, rev, ok = table.Last(txn, idIndex.Query(1))
	assert.True(t, ok, "expected Last(1) to return result")
	assert.NotZero(t, rev, "expected non-zero revision")
	assert.Equal(t, obj.ID, 1, "expected last obj.ID to equal 1")

	obj, rev, firstWatch, ok := table.FirstWatch(txn, idIndex.Query(2))
	assert.True(t, ok, "expected FirstWatch(2) to return result")
	assert.NotZero(t, rev, "expected non-zero revision")
	assert.Equal(t, obj.ID, 2, "expected obj.ID to equal 2")

	obj, rev, lastWatch, ok := table.LastWatch(txn, idIndex.Query(2))
	assert.True(t, ok, "expected LastWatch(2) to return result")
	assert.NotZero(t, rev, "expected non-zero revision")
	assert.Equal(t, obj.ID, 2, "expected obj.ID to equal 2")

	select {
	case <-firstWatch:
		t.Fatalf("FirstWatch channel closed before changes")
	case <-lastWatch:
		t.Fatalf("LastWatch channel closed before changes")
	case <-getWatch:
		t.Fatalf("Get channel closed before changes")
	default:
	}

	// Modify the testObject(2) to trigger closing of the watch channels.
	wtxn := db.WriteTxn(table)
	_, hadOld, err := table.Insert(wtxn, testObject{ID: uint64(2), Tags: []string{"even", "modified"}})
	assert.True(t, hadOld)
	assert.NoError(t, err)
	wtxn.Commit()

	select {
	case <-firstWatch:
	case <-time.After(time.Second):
		t.Fatalf("FirstWatch channel not closed after change")
	}
	select {
	case <-lastWatch:
	case <-time.After(time.Second):
		t.Fatalf("LastWatch channel not closed after change")
	}
	select {
	case <-getWatch:
	case <-time.After(time.Second):
		t.Fatalf("Get channel not closed after change")
	}

	// Since we modified the database, grab a fresh read transaction.
	txn = db.ReadTxn()

	// Test First and Last against the tags multi-index which will
	// return multiple results.
	obj, rev, _, ok = table.FirstWatch(txn, tagsIndex.Query("even"))
	assert.True(t, ok, "expected First(even) to return result")
	assert.NotZero(t, rev, "expected non-zero revision")
	assert.Equal(t, []string{"even", "modified"}, sorted(obj.Tags))
	assert.Equal(t, 2, obj.ID)

	obj, rev, _, ok = table.LastWatch(txn, tagsIndex.Query("odd"))
	assert.True(t, ok, "expected First(even) to return result")
	assert.NotZero(t, rev, "expected non-zero revision")
	assert.Equal(t, []string{"odd"}, obj.Tags)
	assert.Equal(t, 9, obj.ID)

	iter, _ = table.Get(txn, tagsIndex.Query("odd"))
	items = Collect(iter)
	assert.Equal(t, 5, len(items), "expected Get(odd) to return 5 items")
	for i, item := range items {
		assert.Equal(t, int(item.ID), i*2+1, "expected items[%d].ID to equal %d", i, i*2+1)
	}
}

func TestDB_CommitAbort(t *testing.T) {
	t.Parallel()

	dbX, table, metrics := newTestDB(t, tagsIndex)
	db := dbX.NewHandle("test-handle")

	txn := db.WriteTxn(table)
	_, _, err := table.Insert(txn, testObject{ID: 123, Tags: nil})
	assert.NoError(t, err)
	txn.Commit()

	assert.Equal(t, int64(table.Revision(db.ReadTxn())), expvarInt64(metrics.RevisionVar.Get("test")), "Revision")
	assert.Equal(t, 1, expvarInt64(metrics.ObjectCountVar.Get("test")), "ObjectCount")
	assert.True(t, expvarFloat(metrics.WriteTxnAcquisitionVar.Get("test-handle/test")) > 0.0, "WriteTxnAcquisition")
	assert.True(t, expvarFloat(metrics.WriteTxnDurationVar.Get("test-handle/test")) > 0.0, "WriteTxnDuration")

	obj, rev, ok := table.First(db.ReadTxn(), idIndex.Query(123))
	assert.True(t, ok, "expected First(1) to return result")
	assert.NotZero(t, rev, "expected non-zero revision")
	assert.Equal(t, obj.ID, 123, "expected obj.ID to equal 123")
	assert.Zero(t, obj.Tags, "expected no tags")

	_, _, err = table.Insert(txn, testObject{ID: 123, Tags: []string{"insert-after-commit"}})
	assert.IsError(t, err, ErrTransactionClosed)
	txn.Commit() // should be no-op

	txn = db.WriteTxn(table)
	txn.Abort()

	_, _, err = table.Insert(txn, testObject{ID: 123, Tags: []string{"insert-after-abort"}})
	assert.IsError(t, err, ErrTransactionClosed)
	txn.Commit() // should be no-op

	// Check that insert after commit and insert after abort do not change the
	// table.
	obj, newRev, ok := table.First(db.ReadTxn(), idIndex.Query(123))
	assert.True(t, ok, "expected object to exist")
	assert.Equal(t, rev, newRev, "expected unchanged revision")
	assert.Equal(t, obj.ID, 123, "expected obj.ID to equal 123")
	assert.Zero(t, obj.Tags, "expected no tags")
}

func TestDB_CompareAndSwap_CompareAndDelete(t *testing.T) {
	t.Parallel()

	db, table, _ := newTestDB(t, tagsIndex)

	// Updating a non-existing object fails and nothing is inserted.
	wtxn := db.WriteTxn(table)
	{
		_, hadOld, err := table.CompareAndSwap(wtxn, 1, testObject{ID: 1})
		assert.IsError(t, ErrObjectNotFound, err)
		assert.False(t, hadOld)

		objs, _ := table.All(wtxn)
		assert.Zero(t, len(Collect(objs)))

		wtxn.Abort()
	}

	// Insert a test object and retrieve it.
	wtxn = db.WriteTxn(table)
	table.Insert(wtxn, testObject{ID: 1})
	wtxn.Commit()

	obj, rev1, ok := table.First(db.ReadTxn(), idIndex.Query(1))
	assert.True(t, ok)

	// Updating an object with matching revision number works
	wtxn = db.WriteTxn(table)
	obj.Tags = []string{"updated"} // NOTE: testObject stored by value so no explicit copy needed.
	oldObj, hadOld, err := table.CompareAndSwap(wtxn, rev1, obj)
	assert.NoError(t, err)
	assert.True(t, hadOld)
	assert.Equal(t, 1, oldObj.ID)
	wtxn.Commit()

	obj, _, ok = table.First(db.ReadTxn(), idIndex.Query(1))
	assert.True(t, ok)
	assert.Equal(t, 1, len(obj.Tags))
	assert.Equal(t, "updated", obj.Tags[0])

	// Updating an object with mismatching revision number fails
	wtxn = db.WriteTxn(table)
	obj.Tags = []string{"mismatch"}
	oldObj, hadOld, err = table.CompareAndSwap(wtxn, rev1, obj)
	assert.IsError(t, ErrRevisionNotEqual, err)
	assert.True(t, hadOld)
	assert.Equal(t, 1, oldObj.ID)
	wtxn.Commit()

	obj, _, ok = table.First(db.ReadTxn(), idIndex.Query(1))
	assert.True(t, ok)
	assert.Equal(t, 1, len(obj.Tags))
	assert.Equal(t, "updated", obj.Tags[0])

	// Deleting an object with mismatching revision number fails
	wtxn = db.WriteTxn(table)
	obj.Tags = []string{"mismatch"}
	oldObj, hadOld, err = table.CompareAndDelete(wtxn, rev1, obj)
	assert.IsError(t, ErrRevisionNotEqual, err)
	assert.True(t, hadOld)
	assert.Equal(t, 1, oldObj.ID)
	wtxn.Commit()

	obj, rev2, ok := table.First(db.ReadTxn(), idIndex.Query(1))
	assert.True(t, ok)
	assert.Equal(t, 1, len(obj.Tags))
	assert.Equal(t, "updated", obj.Tags[0])

	// Deleting with matching revision number works
	wtxn = db.WriteTxn(table)
	obj.Tags = []string{"mismatch"}
	oldObj, hadOld, err = table.CompareAndDelete(wtxn, rev2, obj)
	assert.NoError(t, err)
	assert.True(t, hadOld)
	assert.Equal(t, 1, oldObj.ID)
	wtxn.Commit()

	_, _, ok = table.First(db.ReadTxn(), idIndex.Query(1))
	assert.False(t, ok)

	// Deleting non-existing object yields not found
	wtxn = db.WriteTxn(table)
	_, hadOld, err = table.CompareAndDelete(wtxn, rev2, obj)
	assert.NoError(t, err)
	assert.False(t, hadOld)
	wtxn.Abort()
}

func TestDB_ReadAfterWrite(t *testing.T) {
	t.Parallel()

	db, table, _ := newTestDB(t, tagsIndex)

	txn := db.WriteTxn(table)

	iter, _ := table.All(txn)
	assert.Zero(t, len(Collect(iter)))

	table.Insert(txn, testObject{ID: 1})

	iter, _ = table.All(txn)
	assert.Equal(t, 1, len(Collect(iter)))

	table.Delete(txn, testObject{ID: 1})
	iter, _ = table.All(txn)
	assert.Zero(t, len(Collect(iter)))

	table.Insert(txn, testObject{ID: 2})
	iter, _ = table.All(txn)
	assert.Equal(t, 1, len(Collect(iter)))

	txn.Commit()

	iter, _ = table.All(db.ReadTxn())
	assert.Equal(t, 1, len(Collect(iter)))
}

func TestWriteJSON(t *testing.T) {
	t.Parallel()

	db, table, _ := newTestDB(t, tagsIndex)

	buf := new(bytes.Buffer)
	err := db.ReadTxn().WriteJSON(buf)
	assert.NoError(t, err)

	txn := db.WriteTxn(table)
	for i := 1; i <= 10; i++ {
		_, _, err := table.Insert(txn, testObject{ID: uint64(i)})
		assert.NoError(t, err)
	}
	txn.Commit()
}

func Test_nonUniqueKey(t *testing.T) {
	// empty keys
	key := encodeNonUniqueKey(nil, nil)
	primary, secondary := decodeNonUniqueKey(key)
	assert.Zero(t, len(primary))
	assert.Zero(t, len(secondary))

	// empty primary
	key = encodeNonUniqueKey(nil, []byte("foo"))
	primary, secondary = decodeNonUniqueKey(key)
	assert.Zero(t, len(primary))
	assert.Equal(t, string(secondary), "foo")

	// empty secondary
	key = encodeNonUniqueKey([]byte("quux"), []byte{})
	primary, secondary = decodeNonUniqueKey(key)
	assert.Equal(t, string(primary), "quux")
	assert.Zero(t, len(secondary))

	// non-empty
	key = encodeNonUniqueKey([]byte("foo"), []byte("quux"))
	primary, secondary = decodeNonUniqueKey(key)
	assert.Equal(t, []byte("foo"), primary)
	assert.Equal(t, []byte("quux"), secondary)
}

func eventuallyGraveyardIsEmpty(t testing.TB, db *DB) {
	assertEventually(t,
		db.graveyardIsEmpty,
		5*time.Second,
		100*time.Millisecond,
		"graveyard not garbage collected")
}

func expvarInt64(v expvar.Var) int64 {
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
