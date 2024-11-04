// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package statedb

import (
	"sync/atomic"
)

type deleteTracker[Obj any] struct {
	db          *DB
	trackerName string
	table       Table[Obj]

	// revision is the last observed revision. Starts out at zero
	// in which case the garbage collector will not care about this
	// tracker when considering which objects to delete.
	revision atomic.Uint64
}

// setRevision is called to set the starting low watermark when
// this deletion tracker is inserted into the table.
func (dt *deleteTracker[Obj]) setRevision(rev uint64) {
	dt.revision.Store(rev)
}

// getRevision is called by the graveyard garbage collector to
// compute the global low watermark.
func (dt *deleteTracker[Obj]) getRevision() uint64 {
	return dt.revision.Load()
}

// Mark the revision up to which deleted objects have been processed. This sets
// the low watermark for deleted object garbage collection.
func (dt *deleteTracker[Obj]) mark(upTo Revision) {
	// Store the new low watermark and trigger a round of garbage collection.
	dt.revision.Store(upTo)
}

func (dt *deleteTracker[Obj]) close() {
	if dt.db == nil {
		return
	}

	// Remove the delete tracker from the table.
	txn := dt.db.WriteTxn(dt.table).getTxn()
	dt.db = nil
	db := txn.db
	table := txn.modifiedTables[dt.table.tablePos()]
	if table == nil {
		panic("BUG: Table missing from write transaction")
	}
	_, _, table.deleteTrackers = table.deleteTrackers.Delete([]byte(dt.trackerName))
	txn.Commit()

	db.metrics.DeleteTrackerCount(dt.table.Name(), table.deleteTrackers.Len())
}

var closedWatchChannel = func() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()
