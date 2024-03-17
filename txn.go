// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package statedb

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"runtime"
	"time"

	iradix "github.com/hashicorp/go-immutable-radix/v2"

	"github.com/cilium/statedb/index"
	"github.com/cilium/statedb/internal"
)

type txn struct {
	db             *DB
	root           dbRoot
	writeTxns      map[tableIndex]indexTxn  // opened per-index write transactions
	modifiedTables []*tableEntry            // table entries being modified
	smus           internal.SortableMutexes // the (sorted) table locks
	acquiredAt     time.Time                // the time at which the transaction acquired the locks
	packageName    string                   // name of the package that created the transaction
}

type tableIndex struct {
	tablePos int
	index    IndexName
}

type indexTxn struct {
	*iradix.Txn[object]
	entry indexEntry
}

func (i indexTxn) Clone() indexTxn {
	return indexTxn{
		i.Txn.Clone(),
		i.entry,
	}
}

var zeroTxn = txn{}

// txn fulfills the ReadTxn/WriteTxn interface.
func (txn *txn) getTxn() *txn {
	return txn
}

// txnFinalizer is called when the GC frees *txn. It checks that a WriteTxn
// has been Aborted or Committed. This is a safeguard against forgetting to
// Abort/Commit which would cause the table to be locked forever.
func txnFinalizer(txn *txn) {
	if txn.db != nil {
		panic(fmt.Sprintf("WriteTxn acquired by package %q was never Abort()'d or Commit()'d", txn.packageName))
	}
}

func (txn *txn) getRevision(meta TableMeta) Revision {
	if txn.modifiedTables != nil {
		entry := txn.modifiedTables[meta.tablePos()]
		if entry != nil {
			return entry.revision
		}
	}
	return txn.root[meta.tablePos()].revision
}

// indexReadTxn returns a transaction to read from the specific index.
// If the table or index is not found this returns nil & error.
func (txn *txn) indexReadTxn(meta TableMeta, index IndexName) (indexTxn, error) {
	if txn.writeTxns != nil {
		entry := txn.modifiedTables[meta.tablePos()]
		if entry != nil {
			itxn, err := txn.indexWriteTxn(meta, index)
			if err == nil {
				return itxn.Clone(), nil
			}
			return indexTxn{}, err
		}
	}

	table := txn.root[meta.tablePos()]
	indexEntry, ok := table.indexes.Get([]byte(index))
	if !ok {
		return indexTxn{}, fmt.Errorf("index %q not found from table %q", index, meta.Name())
	}

	return indexTxn{
		indexEntry.tree.Txn(),
		indexEntry}, nil
}

// indexWriteTxn returns a transaction to read/write to a specific index.
// The created transaction is memoized and used for subsequent reads and/or writes.
func (txn *txn) indexWriteTxn(meta TableMeta, index IndexName) (indexTxn, error) {
	if indexTreeTxn, ok := txn.writeTxns[tableIndex{meta.tablePos(), index}]; ok {
		return indexTreeTxn, nil
	}
	table := txn.modifiedTables[meta.tablePos()]
	indexEntry, ok := table.indexes.Get([]byte(index))
	if !ok {
		return indexTxn{}, fmt.Errorf("index %q not found from table %q", index, meta.Name())
	}
	itxn := indexEntry.tree.Txn()
	itxn.TrackMutate(true)
	indexWriteTxn := indexTxn{itxn, indexEntry}
	txn.writeTxns[tableIndex{meta.tablePos(), index}] = indexWriteTxn
	return indexWriteTxn, nil
}

// mustIndexReadTxn returns a transaction to read from the specific index.
// Panics if table or index are not found.
func (txn *txn) mustIndexReadTxn(meta TableMeta, index IndexName) indexTxn {
	indexTxn, err := txn.indexReadTxn(meta, index)
	if err != nil {
		panic(err)
	}
	return indexTxn
}

// mustIndexReadTxn returns a transaction to read or write from the specific index.
// Panics if table or index not found.
func (txn *txn) mustIndexWriteTxn(meta TableMeta, index IndexName) indexTxn {
	indexTxn, err := txn.indexWriteTxn(meta, index)
	if err != nil {
		panic(err)
	}
	return indexTxn
}

func (txn *txn) Insert(meta TableMeta, guardRevision Revision, data any) (object, bool, error) {
	if txn.db == nil {
		return object{}, false, ErrTransactionClosed
	}

	// Look up table and allocate a new revision.
	tableName := meta.Name()
	table := txn.modifiedTables[meta.tablePos()]
	if table == nil {
		return object{}, false, tableError(tableName, ErrTableNotLockedForWriting)
	}
	oldRevision := table.revision
	table.revision++
	revision := table.revision

	obj := object{
		revision: revision,
		data:     data,
	}

	// Update the primary index first
	idKey := meta.primary().fromObject(obj).First()
	idIndexTxn := txn.mustIndexWriteTxn(meta, meta.primary().name)
	oldObj, oldExists := idIndexTxn.Insert(idKey, obj)

	// Sanity check: is the same object being inserted back and thus the
	// immutable object is being mutated?
	if oldExists {
		val := reflect.ValueOf(data)
		if val.Kind() == reflect.Pointer {
			oldVal := reflect.ValueOf(oldObj.data)
			if val.UnsafePointer() == oldVal.UnsafePointer() {
				panic(fmt.Sprintf(
					"Insert() of the same object (%T) back into the table. Is the immutable object being mutated?",
					data))
			}
		}
	}

	// For CompareAndSwap() validate against the given guard revision
	if guardRevision > 0 {
		if !oldExists {
			// CompareAndSwap requires the object to exist. Revert
			// the insert.
			idIndexTxn.Delete(idKey)
			table.revision = oldRevision
			return object{}, false, ErrObjectNotFound
		}
		if oldObj.revision != guardRevision {
			// Revert the change. We're assuming here that it's rarer for CompareAndSwap() to
			// fail and thus we're optimizing to have only one lookup in the common case
			// (versus doing a Get() and then Insert()).
			idIndexTxn.Insert(idKey, oldObj)
			table.revision = oldRevision
			return oldObj, true, ErrRevisionNotEqual
		}
	}

	// Update revision index
	revIndexTxn := txn.mustIndexWriteTxn(meta, RevisionIndex)
	if oldExists {
		_, ok := revIndexTxn.Delete([]byte(index.Uint64(oldObj.revision)))
		if !ok {
			panic("BUG: Old revision index entry not found")
		}
	}
	revIndexTxn.Insert([]byte(index.Uint64(revision)), obj)

	// If it's new, possibly remove an older deleted object with the same
	// primary key from the graveyard.
	if !oldExists && txn.hasDeleteTrackers(meta) {
		if old, existed := txn.mustIndexWriteTxn(meta, GraveyardIndex).Delete(idKey); existed {
			txn.mustIndexWriteTxn(meta, GraveyardRevisionIndex).Delete([]byte(index.Uint64(old.revision)))
		}
	}

	// Then update secondary indexes
	for idx, indexer := range meta.secondary() {
		indexTxn := txn.mustIndexWriteTxn(meta, idx)
		newKeys := indexer.fromObject(obj)

		if oldExists {
			// If the object already existed it might've invalidated the
			// non-primary indexes. Compute the old key for this index and
			// if the new key is different delete the old entry.
			indexer.fromObject(oldObj).Foreach(func(oldKey index.Key) {
				if !indexer.unique {
					oldKey = encodeNonUniqueKey(idKey, oldKey)
				}
				if !newKeys.Exists(oldKey) {
					indexTxn.Delete(oldKey)
				}
			})
		}
		newKeys.Foreach(func(newKey index.Key) {
			// Non-unique secondary indexes are formed by concatenating them
			// with the primary key.
			if !indexer.unique {
				newKey = encodeNonUniqueKey(idKey, newKey)
			}
			indexTxn.Insert(newKey, obj)
		})
	}

	return oldObj, oldExists, nil
}

func (txn *txn) hasDeleteTrackers(meta TableMeta) bool {
	table := txn.modifiedTables[meta.tablePos()]
	if table != nil {
		return table.deleteTrackers.Len() > 0
	}
	return txn.root[meta.tablePos()].deleteTrackers.Len() > 0
}

func (txn *txn) addDeleteTracker(meta TableMeta, trackerName string, dt deleteTracker) error {
	if txn.db == nil {
		return ErrTransactionClosed
	}
	table := txn.modifiedTables[meta.tablePos()]
	table.deleteTrackers, _, _ = table.deleteTrackers.Insert([]byte(trackerName), dt)
	txn.db.metrics.DeleteTrackerCount(meta.Name(), table.deleteTrackers.Len())
	return nil

}

func (txn *txn) Delete(meta TableMeta, guardRevision Revision, data any) (object, bool, error) {
	if txn.db == nil {
		return object{}, false, ErrTransactionClosed
	}

	// Look up table and allocate a new revision.
	tableName := meta.Name()
	table := txn.modifiedTables[meta.tablePos()]
	if table == nil {
		return object{}, false, tableError(tableName, ErrTableNotLockedForWriting)
	}
	oldRevision := table.revision
	table.revision++
	revision := table.revision

	// Delete from the primary index first to grab the object.
	// We assume that "data" has only enough defined fields to
	// compute the primary key.
	idKey := meta.primary().fromObject(object{data: data}).First()
	idIndexTree := txn.mustIndexWriteTxn(meta, meta.primary().name)
	obj, existed := idIndexTree.Delete(idKey)
	if !existed {
		return object{}, false, nil
	}

	// For CompareAndDelete() validate against guard revision and if there's a mismatch,
	// revert the change.
	if guardRevision > 0 {
		if obj.revision != guardRevision {
			idIndexTree.Insert(idKey, obj)
			table.revision = oldRevision
			return obj, true, ErrRevisionNotEqual
		}
	}

	// Update revision index.
	indexTree := txn.mustIndexWriteTxn(meta, RevisionIndex)
	if _, ok := indexTree.Delete(index.Uint64(obj.revision)); !ok {
		panic("BUG: Object to be deleted not found from revision index")
	}

	// Then update secondary indexes.
	for idx, indexer := range meta.secondary() {
		indexer.fromObject(obj).Foreach(func(key index.Key) {
			if !indexer.unique {
				key = encodeNonUniqueKey(idKey, key)
			}
			txn.mustIndexWriteTxn(meta, idx).Delete(key)
		})
	}

	// And finally insert the object into the graveyard.
	if txn.hasDeleteTrackers(meta) {
		graveyardIndex := txn.mustIndexWriteTxn(meta, GraveyardIndex)
		obj.revision = revision
		if _, existed := graveyardIndex.Insert(idKey, obj); existed {
			panic("BUG: Double deletion! Deleted object already existed in graveyard")
		}
		txn.mustIndexWriteTxn(meta, GraveyardRevisionIndex).Insert(index.Uint64(revision), obj)
	}

	return obj, true, nil
}

// encodeNonUniqueKey constructs the internal key to use with non-unique indexes.
// It concatenates the secondary key with the primary key and the length of the secondary key.
// The length is stored as unsigned 16-bit big endian.
// This allows looking up from the non-unique index with the secondary key by doing a prefix
// search. The length is used to safe-guard against indexers that don't terminate the key
// properly (e.g. if secondary key is "foo", then we don't want "foobar" to match).
func encodeNonUniqueKey(primary, secondary index.Key) []byte {
	key := make([]byte, 0, len(secondary)+len(primary)+2)
	key = append(key, secondary...)
	key = append(key, primary...)
	// KeySet limits size of key to 16 bits.
	return binary.BigEndian.AppendUint16(key, uint16(len(secondary)))
}

func decodeNonUniqueKey(key []byte) (primary []byte, secondary []byte) {
	// Multi-index key is [<secondary...>, <primary...>, <secondary length>]
	if len(key) < 2 {
		return nil, nil
	}
	secondaryLength := int(binary.BigEndian.Uint16(key[len(key)-2:]))
	if len(key) < secondaryLength {
		return nil, nil
	}
	return key[secondaryLength : len(key)-2], key[:secondaryLength]
}

func (txn *txn) Abort() {
	runtime.SetFinalizer(txn, nil)

	// If writeTxns is nil, this transaction has already been committed or aborted, and
	// thus there is nothing to do. We allow this without failure to allow for defer
	// pattern:
	//
	//  txn := db.WriteTxn(...)
	//  defer txn.Abort()
	//
	//  ...
	//  if err != nil {
	//    // Transaction now aborted.
	//    return err
	//  }
	//
	//  txn.Commit()
	//
	if txn.writeTxns == nil {
		return
	}

	txn.smus.Unlock()
	txn.db.metrics.WriteTxnDuration(
		txn.packageName,
		time.Since(txn.acquiredAt))

	*txn = zeroTxn
}

func (txn *txn) Commit() {
	runtime.SetFinalizer(txn, nil)

	// We operate here under the following properties:
	//
	// - Each table that we're modifying has its SortableMutex locked and held by
	//   the caller (via WriteTxn()). Concurrent updates to other tables are
	//   allowed (but not to the root pointer), and thus there may be multiple parallel
	//   Commit()'s in progress, but each of those will only process work for tables
	//   they have locked, until root is to be updated.
	//
	// - Modifications to the root pointer (db.root) are made with the db.mu acquired,
	//   and thus changes to it are always performed sequentially. The root pointer is
	//   updated atomically, and thus readers see either an old root or a new root.
	//   Both the old root and new root are immutable after they're made available via
	//   the root pointer.
	//
	// - As the root is atomically swapped to a new immutable tree of tables of indexes,
	//   a reader can acquire an immutable snapshot of all data in the database with a
	//   simpler atomic pointer load.

	// If writeTxns is nil, this transaction has already been committed or aborted, and
	// thus there is nothing to do.
	if txn.writeTxns == nil {
		return
	}

	db := txn.db

	// Commit each individual changed index to each table.
	// We don't notify yet (CommitOnly) as the root needs to be updated
	// first as otherwise readers would wake up too early.
	for tableIndex, subTxn := range txn.writeTxns {
		table := txn.modifiedTables[tableIndex.tablePos]
		subTxn.entry.tree = subTxn.CommitOnly()
		table.indexes, _, _ =
			table.indexes.Insert([]byte(tableIndex.index), subTxn.entry)

		// Update metrics
		name := table.meta.Name()
		db.metrics.GraveyardObjectCount(name, table.numDeletedObjects())
		db.metrics.ObjectCount(name, table.numObjects())
		db.metrics.Revision(name, table.revision)
	}

	// Acquire the lock on the root tree to sequence the updates to it. We can acquire
	// it after we've built up the new table entries above, since changes to those were
	// protected by each table lock (that we're holding here).
	db.mu.Lock()

	// Since the root may have changed since the pointer was last read in WriteTxn(),
	// load it again and modify the latest version that we now have immobilised by
	// the root lock.
	root := *db.root.Load()

	// Insert the modified tables into the root tree of tables.
	for pos, table := range txn.modifiedTables {
		if table != nil {
			root[pos] = *table
		}
	}

	// Commit the transaction to build the new root tree and then
	// atomically store it.
	db.root.Store(&root)
	db.mu.Unlock()

	// With the root pointer updated, we can now release the tables for the next write transaction.
	txn.smus.Unlock()

	// Now that new root is committed, we can notify readers by closing the watch channels of
	// mutated radix tree nodes in all changed indexes and on the root itself.
	for _, subTxn := range txn.writeTxns {
		subTxn.Notify()
	}

	txn.db.metrics.WriteTxnDuration(
		txn.packageName,
		time.Since(txn.acquiredAt))

	// Zero out the transaction to make it inert.
	*txn = zeroTxn
}

// WriteJSON marshals out the whole database as JSON into the given writer.
func (txn *txn) WriteJSON(w io.Writer) error {
	buf := bufio.NewWriter(w)
	buf.WriteString("{\n")
	first := true
	for _, table := range txn.root {
		if !first {
			buf.WriteString(",\n")
		} else {
			first = false
		}

		indexTxn := txn.getTxn().mustIndexReadTxn(table.meta, table.meta.primary().name)
		root := indexTxn.Root()
		iter := root.Iterator()

		buf.WriteString("  \"" + table.meta.Name() + "\": [\n")

		_, obj, ok := iter.Next()
		for ok {
			buf.WriteString("    ")
			bs, err := json.Marshal(obj.data)
			if err != nil {
				return err
			}
			buf.Write(bs)
			_, obj, ok = iter.Next()
			if ok {
				buf.WriteString(",\n")
			} else {
				buf.WriteByte('\n')
			}
		}
		buf.WriteString("  ]")
	}
	buf.WriteString("\n}\n")
	return buf.Flush()
}
