// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package statedb

import (
	"bytes"
	"encoding"
	"encoding/binary"
	"errors"
	"io"
	"runtime"

	"github.com/cockroachdb/pebble"

	"github.com/cilium/statedb/index"
)

// PebbleIndex stores objects durably in Pebble while exposing the normal StateDB index API.
// It is always unique and must always produce exactly one key per object.
type PebbleIndex[Obj interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}, Key any] struct {
	Name string

	FromObject func(obj Obj) index.KeySet
	FromKey    func(key Key) index.Key
	FromString func(key string) (index.Key, error)
	NewObj     func() Obj
}

func (i PebbleIndex[Obj, Key]) isPebbleIndex() {}

func (i PebbleIndex[Obj, Key]) isIndexerOf(Obj) {
	panic("isIndexerOf")
}

func (i PebbleIndex[Obj, Key]) isUnique() bool {
	return true
}

func (i PebbleIndex[Obj, Key]) fromString(s string) (index.Key, error) {
	return i.FromString(s)
}

func (i PebbleIndex[Obj, Key]) indexName() string {
	return i.Name
}

func (i PebbleIndex[Obj, Key]) Query(key Key) Query[Obj] {
	return Query[Obj]{
		index: i.Name,
		key:   i.FromKey(key),
	}
}

func (i PebbleIndex[Obj, Key]) QueryFromObject(obj Obj) Query[Obj] {
	return Query[Obj]{
		index: i.Name,
		key:   i.FromObject(obj).First(),
	}
}

func (i PebbleIndex[Obj, Key]) ObjectToKey(obj Obj) index.Key {
	return i.FromObject(obj).First()
}

func (i PebbleIndex[Obj, Key]) newTableIndex() tableIndex {
	return &pebbleIndex{
		objectToKeys: func(obj object) index.KeySet {
			return i.FromObject(obj.data.(Obj))
		},
		decodeObject: func(data []byte) (any, error) {
			obj := i.NewObj()
			if err := obj.UnmarshalBinary(data); err != nil {
				return nil, err
			}
			return obj, nil
		},
	}
}

var _ Indexer[struct {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}] = PebbleIndex[struct {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}, bool]{}

type pebbleIndex struct {
	db *DB

	table              TableMeta
	name               string
	namespace          []byte
	namespaceUpper     []byte
	objectToKeys       func(object) index.KeySet
	primaryObjectToKey func(object) index.Key
	decodeObject       func([]byte) (any, error)

	snapshot *pebble.Snapshot
	watch    chan struct{}
}

func (p *pebbleIndex) bind(db *DB, table TableMeta, name string, primaryIndex tableIndex) {
	p.db = db
	p.table = table
	p.name = name
	p.namespace = namespacePrefix(table.Name(), name)
	p.namespaceUpper = namespaceUpperBound(p.namespace)
	p.primaryObjectToKey = primaryIndex.objectToKey
	p.watch = make(chan struct{})
	if db == nil || db.pebble == nil {
		return
	}
	pebbleDB := db.pebble.get()
	if pebbleDB == nil {
		return
	}
	p.snapshot = pebbleDB.NewSnapshot()
	db.pebble.addSnapshot(p.snapshot)
	runtime.SetFinalizer(p, (*pebbleIndex).finalize)
}

func (p *pebbleIndex) finalize() {
	p.closeSnapshot()
}

func (p *pebbleIndex) closeSnapshot() {
	if p.snapshot != nil {
		if p.db != nil && p.db.pebble != nil {
			p.db.pebble.removeSnapshot(p.snapshot)
		}
		func() {
			defer func() {
				_ = recover()
			}()
			_ = p.snapshot.Close()
		}()
		p.snapshot = nil
	}
}

func (p *pebbleIndex) len() int {
	iter, err := p.newIter(&pebble.IterOptions{
		LowerBound: p.namespace,
		UpperBound: p.namespaceUpper,
	})
	if err != nil {
		panic(err)
	}
	defer iter.Close()

	n := 0
	for ok := iter.First(); ok; ok = iter.Next() {
		n++
	}
	if err := iter.Error(); err != nil {
		panic(err)
	}
	return n
}

func (p *pebbleIndex) get(key index.Key) (object, <-chan struct{}, bool) {
	obj, _, found, err := p.readRecord(p.namespacedKey(key))
	if err != nil {
		panic(err)
	}
	return obj, p.watch, found
}

func (p *pebbleIndex) list(key index.Key) (tableIndexIterator, <-chan struct{}) {
	obj, watch, found := p.get(key)
	if !found {
		return emptyTableIndexIterator, watch
	}
	return &singletonTableIndexIterator{key: key, obj: obj}, watch
}

func (p *pebbleIndex) prefix(key index.Key) (tableIndexIterator, <-chan struct{}) {
	lower := p.namespacedKey(key)
	upper := prefixUpperBound(lower)
	return newPebbleIterator(p, lower, upper), p.watch
}

func (p *pebbleIndex) lowerBound(key index.Key) (tableIndexIterator, <-chan struct{}) {
	return newPebbleIterator(p, p.namespacedKey(key), p.namespaceUpper), p.watch
}

func (p *pebbleIndex) lowerBoundNext(key index.Key) (func() ([]byte, object, bool), <-chan struct{}) {
	iter, err := p.newIter(&pebble.IterOptions{
		LowerBound: p.namespacedKey(key),
		UpperBound: p.namespaceUpper,
	})
	if err != nil {
		panic(err)
	}
	started := false
	return func() ([]byte, object, bool) {
		var ok bool
		if !started {
			ok = iter.First()
			started = true
		} else {
			ok = iter.Next()
		}
		if !ok {
			err := iter.Error()
			_ = iter.Close()
			if err != nil {
				panic(err)
			}
			return nil, object{}, false
		}
		obj, _, err := p.decodeValue(iter.Value())
		if err != nil {
			_ = iter.Close()
			panic(err)
		}
		return p.rawKey(iter.Key()), obj, true
	}, p.watch
}

func (p *pebbleIndex) all() (tableIndexIterator, <-chan struct{}) {
	return newPebbleIterator(p, p.namespace, p.namespaceUpper), p.watch
}

func (p *pebbleIndex) rootWatch() <-chan struct{} {
	return p.watch
}

func (p *pebbleIndex) objectToKey(obj object) index.Key {
	return p.objectToKeys(obj).First()
}

func (p *pebbleIndex) validatedObjectToKey(obj object) (index.Key, error) {
	return keySetSingle(p.objectToKeys(obj))
}

func (p *pebbleIndex) txn(txn tableIndexTxnContext) (tableIndexTxn, bool) {
	if txn == nil || txn.getPebbleBatch() == nil {
		return nil, false
	}
	return &pebbleIndexTxn{
		index: p,
		batch: txn.getPebbleBatch().batch,
	}, true
}

func (p *pebbleIndex) commit() (tableIndex, tableIndexTxnNotify) {
	return p, nil
}

func (p *pebbleIndex) namespacedKey(key index.Key) []byte {
	return append(append([]byte{}, p.namespace...), key...)
}

func (p *pebbleIndex) rawKey(key []byte) []byte {
	return append([]byte{}, key[len(p.namespace):]...)
}

func (p *pebbleIndex) newIter(opts *pebble.IterOptions) (*pebble.Iterator, error) {
	if p.snapshot == nil {
		return nil, ErrPebbleNotConfigured
	}
	return p.snapshot.NewIter(opts)
}

func (p *pebbleIndex) readRecord(key []byte) (object, index.Key, bool, error) {
	if p.snapshot == nil {
		return object{}, nil, false, ErrPebbleNotConfigured
	}
	value, closer, err := p.snapshot.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return object{}, nil, false, nil
	}
	if err != nil {
		return object{}, nil, false, err
	}
	defer closer.Close()
	obj, primary, err := p.decodeValue(value)
	return obj, primary, err == nil, err
}

func (p *pebbleIndex) decodeValue(data []byte) (object, index.Key, error) {
	if len(data) < 12 {
		return object{}, nil, io.ErrUnexpectedEOF
	}
	primaryLen := binary.BigEndian.Uint32(data[:4])
	if len(data) < int(12+primaryLen) {
		return object{}, nil, io.ErrUnexpectedEOF
	}
	primaryKey := append([]byte{}, data[4:4+primaryLen]...)
	revision := binary.BigEndian.Uint64(data[4+primaryLen : 12+primaryLen])
	objData, err := p.decodeObject(data[12+primaryLen:])
	if err != nil {
		return object{}, nil, err
	}
	return object{
		data:     objData,
		revision: revision,
	}, primaryKey, nil
}

func (p *pebbleIndex) encodeValue(primaryKey index.Key, obj object) ([]byte, error) {
	marshaler, ok := obj.data.(encoding.BinaryMarshaler)
	if !ok {
		return nil, ErrPebbleIndexKeyCount
	}
	data, err := marshaler.MarshalBinary()
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 0, 12+len(primaryKey)+len(data))
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(primaryKey)))
	buf = append(buf, primaryKey...)
	buf = binary.BigEndian.AppendUint64(buf, obj.revision)
	buf = append(buf, data...)
	return buf, nil
}

type pebbleIndexTxn struct {
	index     *pebbleIndex
	batch     *pebble.Batch
	dirty     bool
	nextWatch chan struct{}
}

func (p *pebbleIndexTxn) currentWatch() <-chan struct{} {
	if p.nextWatch != nil {
		return p.nextWatch
	}
	return p.index.watch
}

func (p *pebbleIndexTxn) markDirty() chan struct{} {
	if p.nextWatch == nil {
		p.nextWatch = make(chan struct{})
	}
	p.dirty = true
	return p.nextWatch
}

func (p *pebbleIndexTxn) len() int {
	return p.index.len()
}

func (p *pebbleIndexTxn) get(key index.Key) (object, <-chan struct{}, bool) {
	obj, _, found, err := p.readRecord(p.index.namespacedKey(key))
	if err != nil {
		panic(err)
	}
	return obj, p.currentWatch(), found
}

func (p *pebbleIndexTxn) list(key index.Key) (tableIndexIterator, <-chan struct{}) {
	obj, watch, found := p.get(key)
	if !found {
		return emptyTableIndexIterator, watch
	}
	return &singletonTableIndexIterator{key: key, obj: obj}, watch
}

func (p *pebbleIndexTxn) prefix(key index.Key) (tableIndexIterator, <-chan struct{}) {
	lower := p.index.namespacedKey(key)
	return newPebbleTxnIterator(p, lower, prefixUpperBound(lower)), p.currentWatch()
}

func (p *pebbleIndexTxn) lowerBound(key index.Key) (tableIndexIterator, <-chan struct{}) {
	return newPebbleTxnIterator(p, p.index.namespacedKey(key), p.index.namespaceUpper), p.currentWatch()
}

func (p *pebbleIndexTxn) lowerBoundNext(key index.Key) (func() ([]byte, object, bool), <-chan struct{}) {
	iter, err := p.batch.NewIter(&pebble.IterOptions{
		LowerBound: p.index.namespacedKey(key),
		UpperBound: p.index.namespaceUpper,
	})
	if err != nil {
		panic(err)
	}
	started := false
	return func() ([]byte, object, bool) {
		var ok bool
		if !started {
			ok = iter.First()
			started = true
		} else {
			ok = iter.Next()
		}
		if !ok {
			err := iter.Error()
			_ = iter.Close()
			if err != nil {
				panic(err)
			}
			return nil, object{}, false
		}
		obj, _, err := p.index.decodeValue(iter.Value())
		if err != nil {
			_ = iter.Close()
			panic(err)
		}
		return p.index.rawKey(iter.Key()), obj, true
	}, p.currentWatch()
}

func (p *pebbleIndexTxn) all() (tableIndexIterator, <-chan struct{}) {
	return newPebbleTxnIterator(p, p.index.namespace, p.index.namespaceUpper), p.currentWatch()
}

func (p *pebbleIndexTxn) rootWatch() <-chan struct{} {
	return p.currentWatch()
}

func (p *pebbleIndexTxn) objectToKey(obj object) index.Key {
	return p.index.objectToKey(obj)
}

func (p *pebbleIndexTxn) validatedObjectToKey(obj object) (index.Key, error) {
	return p.index.validatedObjectToKey(obj)
}

func (p *pebbleIndexTxn) insert(key index.Key, obj object) (object, bool, <-chan struct{}) {
	oldObj, _, hadOld, err := p.readRecord(p.index.namespacedKey(key))
	if err != nil {
		panic(err)
	}
	value, err := p.index.encodeValue(key, obj)
	if err != nil {
		panic(err)
	}
	if err := p.batch.Set(p.index.namespacedKey(key), value, pebble.NoSync); err != nil {
		panic(err)
	}
	return oldObj, hadOld, p.markDirty()
}

func (p *pebbleIndexTxn) modify(key index.Key, obj object, mod func(old, new object) object) (object, object, bool, <-chan struct{}) {
	oldObj, _, hadOld, err := p.readRecord(p.index.namespacedKey(key))
	if err != nil {
		panic(err)
	}
	if hadOld {
		obj = mod(oldObj, obj)
	}
	value, err := p.index.encodeValue(key, obj)
	if err != nil {
		panic(err)
	}
	if err := p.batch.Set(p.index.namespacedKey(key), value, pebble.NoSync); err != nil {
		panic(err)
	}
	return oldObj, obj, hadOld, p.markDirty()
}

func (p *pebbleIndexTxn) delete(key index.Key) (object, bool) {
	oldObj, _, hadOld, err := p.readRecord(p.index.namespacedKey(key))
	if err != nil {
		panic(err)
	}
	if !hadOld {
		return object{}, false
	}
	if err := p.batch.Delete(p.index.namespacedKey(key), pebble.NoSync); err != nil {
		panic(err)
	}
	p.markDirty()
	return oldObj, true
}

func (p *pebbleIndexTxn) reindex(primaryKey index.Key, old object, new object) error {
	var (
		oldKey index.Key
		newKey index.Key
		err    error
	)
	if old.revision != 0 {
		oldKey, err = p.validatedObjectToKey(old)
		if err != nil {
			return err
		}
	}
	if new.revision != 0 {
		newKey, err = p.validatedObjectToKey(new)
		if err != nil {
			return err
		}
		if old.revision == 0 || !bytes.Equal(oldKey, newKey) {
			_, existingPrimary, found, err := p.readRecord(p.index.namespacedKey(newKey))
			if err != nil {
				return err
			}
			if found && !bytes.Equal(existingPrimary, primaryKey) {
				return ErrPebbleIndexConflict
			}
		}
	}
	if old.revision != 0 && (new.revision == 0 || !bytes.Equal(oldKey, newKey)) {
		if err := p.batch.Delete(p.index.namespacedKey(oldKey), pebble.NoSync); err != nil {
			return err
		}
		p.markDirty()
	}
	if new.revision != 0 {
		value, err := p.index.encodeValue(primaryKey, new)
		if err != nil {
			return err
		}
		if err := p.batch.Set(p.index.namespacedKey(newKey), value, pebble.NoSync); err != nil {
			return err
		}
		p.markDirty()
	}
	return nil
}

func (p *pebbleIndexTxn) txn(_ tableIndexTxnContext) (tableIndexTxn, bool) {
	return p, false
}

func (p *pebbleIndexTxn) commitDeferred() bool {
	return true
}

func (p *pebbleIndexTxn) commit() (tableIndex, tableIndexTxnNotify) {
	if !p.dirty {
		return p.index, nil
	}
	snapshot := p.index.db.pebble.get().NewSnapshot()
	idx := &pebbleIndex{
		db:                 p.index.db,
		table:              p.index.table,
		name:               p.index.name,
		namespace:          append([]byte{}, p.index.namespace...),
		namespaceUpper:     append([]byte{}, p.index.namespaceUpper...),
		objectToKeys:       p.index.objectToKeys,
		primaryObjectToKey: p.index.primaryObjectToKey,
		decodeObject:       p.index.decodeObject,
		snapshot:           snapshot,
		watch:              p.nextWatch,
	}
	idx.db.pebble.addSnapshot(snapshot)
	runtime.SetFinalizer(idx, (*pebbleIndex).finalize)
	return idx, p
}

func (p *pebbleIndexTxn) notify() {
	if p.index.watch != nil {
		close(p.index.watch)
		p.index.watch = nil
	}
}

func (p *pebbleIndexTxn) readRecord(key []byte) (object, index.Key, bool, error) {
	value, closer, err := p.batch.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return object{}, nil, false, nil
	}
	if err != nil {
		return object{}, nil, false, err
	}
	defer closer.Close()
	obj, primaryKey, err := p.index.decodeValue(value)
	return obj, primaryKey, err == nil, err
}

var _ tableIndex = &pebbleIndex{}
var _ tableIndexTxn = &pebbleIndexTxn{}

type pebbleIterator struct {
	newIter func() (*pebble.Iterator, error)
	decode  func([]byte) (object, index.Key, error)
	rawKey  func([]byte) []byte
}

func (p *pebbleIterator) All(yield func([]byte, object) bool) {
	iter, err := p.newIter()
	if err != nil {
		panic(err)
	}
	defer iter.Close()
	for ok := iter.First(); ok; ok = iter.Next() {
		obj, _, err := p.decode(iter.Value())
		if err != nil {
			panic(err)
		}
		if !yield(p.rawKey(iter.Key()), obj) {
			return
		}
	}
	if err := iter.Error(); err != nil {
		panic(err)
	}
}

func newPebbleIterator(index *pebbleIndex, lower, upper []byte) tableIndexIterator {
	return &pebbleIterator{
		newIter: func() (*pebble.Iterator, error) {
			return index.newIter(&pebble.IterOptions{
				LowerBound: lower,
				UpperBound: upper,
			})
		},
		decode: index.decodeValue,
		rawKey: index.rawKey,
	}
}

func newPebbleTxnIterator(index *pebbleIndexTxn, lower, upper []byte) tableIndexIterator {
	return &pebbleIterator{
		newIter: func() (*pebble.Iterator, error) {
			return index.batch.NewIter(&pebble.IterOptions{
				LowerBound: lower,
				UpperBound: upper,
			})
		},
		decode: index.index.decodeValue,
		rawKey: index.index.rawKey,
	}
}

func keySetSingle(keys index.KeySet) (index.Key, error) {
	var (
		key   index.Key
		count int
	)
	keys.Foreach(func(k index.Key) {
		if count == 0 {
			key = k
		}
		count++
	})
	if count != 1 {
		return nil, ErrPebbleIndexKeyCount
	}
	return key, nil
}

func prefixUpperBound(prefix []byte) []byte {
	upper := append([]byte{}, prefix...)
	for i := len(upper) - 1; i >= 0; i-- {
		if upper[i] != 0xff {
			upper[i]++
			return upper[:i+1]
		}
	}
	return nil
}
