// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package statedb

import (
	"fmt"
	"strings"
	"sync"

	"github.com/cilium/statedb/internal"
	iradix "github.com/hashicorp/go-immutable-radix/v2"

	"github.com/cilium/statedb/index"
)

// NewTable creates a new table with given name and indexes.
// Can fail if the indexes are malformed.
//
// To provide access to the table via Hive:
//
//	cell.Provide(
//		// Provide statedb.RWTable[*MyObject]. Often only provided to the module with ProvidePrivate.
//		statedb.NewTable[*MyObject]("my-objects", MyObjectIDIndex, MyObjectNameIndex),
//		// Provide the read-only statedb.Table[*MyObject].
//		statedb.RWTable[*MyObject].ToTable,
//	)
func NewTable[Obj any](
	tableName TableName,
	primaryIndexer Indexer[Obj],
	secondaryIndexers ...Indexer[Obj],
) (RWTable[Obj], error) {
	toAnyIndexer := func(idx Indexer[Obj]) anyIndexer {
		return anyIndexer{
			name: idx.indexName(),
			fromObject: func(iobj object) index.KeySet {
				return idx.fromObject(iobj.data.(Obj))
			},
			unique: idx.isUnique(),
		}
	}

	table := &genTable[Obj]{
		table:                tableName,
		smu:                  internal.NewSortableMutex(),
		primaryAnyIndexer:    toAnyIndexer(primaryIndexer),
		primaryIndexer:       primaryIndexer,
		secondaryAnyIndexers: make(map[string]anyIndexer, len(secondaryIndexers)),
		indexPositions:       make(map[string]int),
	}

	table.indexPositions[primaryIndexer.indexName()] = PrimaryIndexPos

	// Internal indexes
	table.indexPositions[RevisionIndex] = RevisionIndexPos
	table.indexPositions[GraveyardIndex] = GraveyardIndexPos
	table.indexPositions[GraveyardRevisionIndex] = GraveyardRevisionIndexPos

	indexPos := SecondaryIndexStartPos
	for _, indexer := range secondaryIndexers {
		name := indexer.indexName()
		anyIndexer := toAnyIndexer(indexer)
		anyIndexer.pos = indexPos
		table.secondaryAnyIndexers[name] = anyIndexer
		table.indexPositions[name] = indexPos
		indexPos++
	}

	// Primary index must always be unique
	if !primaryIndexer.isUnique() {
		return nil, tableError(tableName, ErrPrimaryIndexNotUnique)
	}

	// Validate that indexes have unique ids.
	indexNames := map[string]struct{}{}
	indexNames[primaryIndexer.indexName()] = struct{}{}
	for _, indexer := range secondaryIndexers {
		if _, ok := indexNames[indexer.indexName()]; ok {
			return nil, tableError(tableName, fmt.Errorf("index %q: %w", indexer.indexName(), ErrDuplicateIndex))
		}
		indexNames[indexer.indexName()] = struct{}{}
	}
	for name := range indexNames {
		if strings.HasPrefix(name, reservedIndexPrefix) {
			return nil, tableError(tableName, fmt.Errorf("index %q: %w", name, ErrReservedPrefix))
		}
	}
	return table, nil
}

// MustNewTable creates a new table with given name and indexes.
// Panics if indexes are malformed.
func MustNewTable[Obj any](
	tableName TableName,
	primaryIndexer Indexer[Obj],
	secondaryIndexers ...Indexer[Obj]) RWTable[Obj] {
	t, err := NewTable[Obj](tableName, primaryIndexer, secondaryIndexers...)
	if err != nil {
		panic(err)
	}
	return t
}

type genTable[Obj any] struct {
	pos                  int
	table                TableName
	smu                  internal.SortableMutex
	primaryIndexer       Indexer[Obj]
	primaryAnyIndexer    anyIndexer
	secondaryAnyIndexers map[string]anyIndexer
	indexPositions       map[string]int
}

func (t *genTable[Obj]) tableEntry() tableEntry {
	var entry tableEntry
	entry.meta = t
	entry.deleteTrackers = iradix.New[deleteTracker]()
	entry.indexes = make([]indexEntry, len(t.indexPositions))
	entry.indexes[t.indexPositions[t.primaryIndexer.indexName()]] = indexEntry{iradix.New[object](), nil, true}

	for index, indexer := range t.secondaryAnyIndexers {
		entry.indexes[t.indexPositions[index]] = indexEntry{iradix.New[object](), nil, indexer.unique}
	}
	entry.indexes[t.indexPositions[RevisionIndex]] = indexEntry{iradix.New[object](), nil, true}
	entry.indexes[t.indexPositions[GraveyardIndex]] = indexEntry{iradix.New[object](), nil, true}
	entry.indexes[t.indexPositions[GraveyardRevisionIndex]] = indexEntry{iradix.New[object](), nil, true}
	return entry
}

func (t *genTable[Obj]) setTablePos(pos int) {
	t.pos = pos
}

func (t *genTable[Obj]) tablePos() int {
	return t.pos
}

func (t *genTable[Obj]) tableKey() []byte {
	return []byte(t.table)
}

func (t *genTable[Obj]) indexPos(name string) int {
	if t.primaryAnyIndexer.name == name {
		return PrimaryIndexPos
	}
	return t.indexPositions[name]
}

func (t *genTable[Obj]) PrimaryIndexer() Indexer[Obj] {
	return t.primaryIndexer
}

func (t *genTable[Obj]) primary() anyIndexer {
	return t.primaryAnyIndexer
}

func (t *genTable[Obj]) secondary() map[string]anyIndexer {
	return t.secondaryAnyIndexers
}

func (t *genTable[Obj]) Name() string {
	return t.table
}

func (t *genTable[Obj]) ToTable() Table[Obj] {
	return t
}

func (t *genTable[Obj]) Initialized(txn ReadTxn) bool {
	return txn.getTxn().root[t.pos].initializers == 0
}

func (t *genTable[Obj]) RegisterInitializer(txn WriteTxn) func(WriteTxn) {
	table := txn.getTxn().modifiedTables[t.pos]
	if table != nil {
		table.initializers++
		return func(txn WriteTxn) {
			var once sync.Once
			once.Do(func() {
				if table := txn.getTxn().modifiedTables[t.pos]; table != nil {
					table.initializers--
				}
			})
		}
	} else {
		return func(WriteTxn) {}
	}
}

func (t *genTable[Obj]) Revision(txn ReadTxn) Revision {
	return txn.getTxn().getRevision(t)
}

func (t *genTable[Obj]) NumObjects(txn ReadTxn) int {
	table := &txn.getTxn().root[t.tablePos()]
	return table.indexes[PrimaryIndexPos].tree.Len()
}

func (t *genTable[Obj]) Get(txn ReadTxn, q Query[Obj]) (obj Obj, revision uint64, ok bool) {
	obj, revision, _, ok = t.GetWatch(txn, q)
	return
}

func (t *genTable[Obj]) GetWatch(txn ReadTxn, q Query[Obj]) (obj Obj, revision uint64, watch <-chan struct{}, ok bool) {
	indexTxn := txn.getTxn().mustIndexReadTxn(t, t.indexPos(q.index))
	var iobj object
	if indexTxn.unique {
		// On a unique index we can do a direct get rather than a prefix search.
		watch, iobj, ok = indexTxn.Root().GetWatch(q.key)
		if !ok {
			return
		}
		obj = iobj.data.(Obj)
		revision = iobj.revision
		return
	}

	// For a non-unique index we need to do a prefix search.
	iter := indexTxn.Root().Iterator()
	watch = iter.SeekPrefixWatch(q.key)
	for {
		var key []byte
		key, iobj, ok = iter.Next()
		if !ok {
			break
		}

		// Check that we have a full match on the key
		_, secondary := decodeNonUniqueKey(key)
		if len(secondary) == len(q.key) {
			break
		}
	}

	if ok {
		obj = iobj.data.(Obj)
		revision = iobj.revision
	}
	return
}

func (t *genTable[Obj]) Last(txn ReadTxn, q Query[Obj]) (obj Obj, revision uint64, ok bool) {
	obj, revision, _, ok = t.LastWatch(txn, q)
	return
}

func (t *genTable[Obj]) LastWatch(txn ReadTxn, q Query[Obj]) (obj Obj, revision uint64, watch <-chan struct{}, ok bool) {
	indexTxn := txn.getTxn().mustIndexReadTxn(t, t.indexPos(q.index))
	var iobj object
	if indexTxn.unique {
		// On a unique index we can do a direct get rather than a prefix search.
		watch, iobj, ok = indexTxn.Root().GetWatch(q.key)
		if !ok {
			return
		}
		obj = iobj.data.(Obj)
		revision = iobj.revision
		return
	}

	iter := indexTxn.Root().ReverseIterator()
	watch = iter.SeekPrefixWatch(q.key)
	for {
		var key []byte
		key, iobj, ok = iter.Previous()
		if !ok {
			break
		}

		// Check that we have a full match on the key
		_, secondary := decodeNonUniqueKey(key)
		if len(secondary) == len(q.key) {
			break
		}
	}

	if ok {
		obj = iobj.data.(Obj)
		revision = iobj.revision
	}
	return
}

func (t *genTable[Obj]) LowerBound(txn ReadTxn, q Query[Obj]) (Iterator[Obj], <-chan struct{}) {
	indexTxn := txn.getTxn().mustIndexReadTxn(t, t.indexPos(q.index))
	root := indexTxn.Root()

	// Since LowerBound query may be invalidated by changes in another branch
	// of the tree, we cannot just simply watch the node we seeked to. Instead
	// we watch the whole table for changes.
	watch, _, _ := root.GetWatch(nil)
	iter := root.Iterator()
	iter.SeekLowerBound(q.key)
	return &iterator[Obj]{iter}, watch
}

func (t *genTable[Obj]) Prefix(txn ReadTxn, q Query[Obj]) (Iterator[Obj], <-chan struct{}) {
	indexTxn := txn.getTxn().mustIndexReadTxn(t, t.indexPos(q.index))
	root := indexTxn.Root()
	iter := root.Iterator()
	watch := iter.SeekPrefixWatch(q.key)
	return &iterator[Obj]{iter}, watch
}

func (t *genTable[Obj]) All(txn ReadTxn) (Iterator[Obj], <-chan struct{}) {
	indexTxn := txn.getTxn().mustIndexReadTxn(t, PrimaryIndexPos)
	root := indexTxn.Root()
	// Grab the watch channel for the root node
	watchCh, _, _ := root.GetWatch(nil)
	return &iterator[Obj]{root.Iterator()}, watchCh
}

func (t *genTable[Obj]) Select(txn ReadTxn, q Query[Obj], filters ...func(Obj) bool) Iterator[Obj] {
	iter, _ := t.SelectWatch(txn, q, filters...)
	return iter
}

func (t *genTable[Obj]) SelectWatch(txn ReadTxn, q Query[Obj], filters ...func(Obj) bool) (Iterator[Obj], <-chan struct{}) {
	indexTxn := txn.getTxn().mustIndexReadTxn(t, t.indexPos(q.index))
	iter := indexTxn.Root().Iterator()
	watchCh := iter.SeekPrefixWatch(q.key)

	var it Iterator[Obj]
	if indexTxn.unique {
		it = uniqueIterator[Obj]{iter, q.key}
	} else {
		it = nonUniqueIterator[Obj]{iter, q.key}
	}
	if len(filters) > 0 {
		it = filterIterator[Obj]{it, filters}
	}
	return it, watchCh
}

func (t *genTable[Obj]) Insert(txn WriteTxn, obj Obj) (oldObj Obj, hadOld bool, err error) {
	var old object
	old, hadOld, err = txn.getTxn().Insert(t, Revision(0), obj)
	if hadOld {
		oldObj = old.data.(Obj)
	}
	return
}

func (t *genTable[Obj]) CompareAndSwap(txn WriteTxn, rev Revision, obj Obj) (oldObj Obj, hadOld bool, err error) {
	var old object
	old, hadOld, err = txn.getTxn().Insert(t, rev, obj)
	if hadOld {
		oldObj = old.data.(Obj)
	}
	return
}

func (t *genTable[Obj]) Delete(txn WriteTxn, obj Obj) (oldObj Obj, hadOld bool, err error) {
	var old object
	old, hadOld, err = txn.getTxn().Delete(t, Revision(0), obj)
	if hadOld {
		oldObj = old.data.(Obj)
	}
	return
}

func (t *genTable[Obj]) CompareAndDelete(txn WriteTxn, rev Revision, obj Obj) (oldObj Obj, hadOld bool, err error) {
	var old object
	old, hadOld, err = txn.getTxn().Delete(t, rev, obj)
	if hadOld {
		oldObj = old.data.(Obj)
	}
	return
}

func (t *genTable[Obj]) DeleteAll(txn WriteTxn) error {
	iter, _ := t.All(txn)
	itxn := txn.getTxn()
	for obj, _, ok := iter.Next(); ok; obj, _, ok = iter.Next() {
		_, _, err := itxn.Delete(t, Revision(0), obj)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *genTable[Obj]) DeleteTracker(txn WriteTxn, trackerName string) (*DeleteTracker[Obj], error) {
	dt := &DeleteTracker[Obj]{
		db:          txn.getTxn().db,
		trackerName: trackerName,
		table:       t,
	}
	err := txn.getTxn().addDeleteTracker(t, trackerName, dt)
	if err != nil {
		return nil, err
	}
	return dt, nil
}

func (t *genTable[Obj]) sortableMutex() internal.SortableMutex {
	return t.smu
}

var _ Table[bool] = &genTable[bool]{}
var _ RWTable[bool] = &genTable[bool]{}
