// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package statedb

import (
	"github.com/cilium/statedb/index"
	"github.com/cilium/statedb/part"
)

// Restore rebuilds in-memory indexes from persisted Pebble indexes.
// It can only be called once for the lifetime of the DB.
func (db *DB) Restore() error {
	if !db.restoreCalled.CompareAndSwap(false, true) {
		return ErrRestoreAlreadyCalled
	}

	rtxn := db.ReadTxn()
	tables := db.GetTables(rtxn)
	restoreTables := make([]TableMeta, 0, len(tables))
	for _, table := range tables {
		if !tableHasPebble(table) {
			continue
		}
		if table.NumObjects(rtxn) > 0 || table.Revision(rtxn) > 0 {
			return tableError(table.Name(), ErrRestoreAfterUse)
		}
		restoreTables = append(restoreTables, table)
	}
	if len(restoreTables) == 0 {
		return nil
	}

	wtxn := db.WriteTxn(restoreTables...)
	defer wtxn.Abort()
	for _, table := range restoreTables {
		if err := wtxn.unwrap().restoreTable(table); err != nil {
			return err
		}
	}
	wtxn.Commit()
	return nil
}

func (txn *writeTxnState) restoreTable(meta TableMeta) error {
	table := txn.tableEntries[meta.tablePos()]
	if !table.locked {
		return tableError(meta.Name(), ErrTableNotLockedForWriting)
	}
	sourcePos := restoreSourcePos(meta)
	if sourcePos < 0 {
		return nil
	}

	committed := txn.committedRoot()[meta.tablePos()]
	source := committed.indexes[sourcePos]
	newIndexes := make([]tableIndex, len(table.indexes))

	if primary := committed.indexes[PrimaryIndexPos]; isPebbleTableIndex(primary) {
		newIndexes[PrimaryIndexPos] = primary
	} else {
		newIndexes[PrimaryIndexPos] = meta.getIndexer("").newTableIndex()
	}

	for _, idx := range meta.secondary() {
		if isPebbleTableIndex(committed.indexes[idx.pos]) {
			newIndexes[idx.pos] = committed.indexes[idx.pos]
			continue
		}
		newIndexes[idx.pos] = idx.newTableIndex()
	}

	newIndexes[RevisionIndexPos] = newRevisionIndex()
	newIndexes[GraveyardRevisionIndexPos] = newRevisionIndex()
	newIndexes[GraveyardIndexPos] = newGraveyardIndex(newIndexes[PrimaryIndexPos])

	deleteTrackers := part.New[anyDeleteTracker]()
	table.deleteTrackers = &deleteTrackers
	table.indexes = newIndexes
	table.revision = 0
	table.hasPebble = committed.hasPebble

	var primaryTxn tableIndexTxn
	if !isPebbleTableIndex(newIndexes[PrimaryIndexPos]) {
		primaryTxn = txn.mustIndexWriteTxn(meta, PrimaryIndexPos)
	}
	revTxn := txn.mustIndexWriteTxn(meta, RevisionIndexPos)

	iter, _ := source.all()
	var restoreErr error
	iter.All(func(_ []byte, obj object) bool {
		primaryKey := table.indexes[PrimaryIndexPos].objectToKey(obj)
		if primaryTxn != nil {
			primaryTxn.insert(primaryKey, obj)
		}
		revTxn.insert(index.Uint64(obj.revision), obj)
		for _, idx := range meta.secondary() {
			if isPebbleTableIndex(table.indexes[idx.pos]) {
				continue
			}
			if err := txn.mustIndexWriteTxn(meta, idx.pos).reindex(primaryKey, object{}, obj); err != nil {
				restoreErr = err
				return false
			}
		}
		if obj.revision > table.revision {
			table.revision = obj.revision
		}
		return true
	})
	return restoreErr
}

func restoreSourcePos(meta TableMeta) int {
	if primary := meta.getIndexer(""); primary != nil && primary.isPebble {
		return PrimaryIndexPos
	}
	for _, idx := range meta.secondary() {
		if idx.isPebble {
			return idx.pos
		}
	}
	return -1
}

func isPebbleTableIndex(idx tableIndex) bool {
	switch idx.(type) {
	case *pebbleIndex, *pebbleIndexTxn:
		return true
	default:
		return false
	}
}
