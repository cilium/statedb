// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package statedb

import (
	"iter"
	"net/netip"

	"github.com/cilium/statedb/index"
	"github.com/cilium/statedb/lpm"
)

// NetIPPrefixIndex indexes objects with a [netip.Prefix] in a LPM
// index.
type NetIPPrefixIndex[Obj any] struct {
	// Name of the index
	Name string

	// FromObject returns the prefixes to index. Use [Just] to
	// return a single prefix.
	FromObject func(obj Obj) iter.Seq[netip.Prefix]
}

func (a NetIPPrefixIndex[Obj]) Query(addr netip.Addr) Query[Obj] {
	addr16 := addr.As16()
	return Query[Obj]{
		index: a.Name,
		key:   lpm.EncodeLPMKey(addr16[:], 128),
	}
}

func (a NetIPPrefixIndex[Obj]) QueryPrefix(prefix netip.Prefix) Query[Obj] {
	return Query[Obj]{
		index: a.Name,
		key:   lpm.NetIPPrefixToIndexKey(prefix),
	}
}

// ObjectToKey implements Indexer.
func (a NetIPPrefixIndex[Obj]) ObjectToKey(obj Obj) index.Key {
	for prefix := range a.FromObject(obj) {
		return lpm.NetIPPrefixToIndexKey(prefix)
	}
	return nil
}

// QueryFromObject implements Indexer.
func (a NetIPPrefixIndex[Obj]) QueryFromObject(obj Obj) Query[Obj] {
	for prefix := range a.FromObject(obj) {
		return Query[Obj]{
			index: a.Name,
			key:   lpm.NetIPPrefixToIndexKey(prefix),
		}
	}
	return Query[Obj]{}
}

// fromString implements Indexer.
func (a NetIPPrefixIndex[Obj]) fromString(s string) (index.Key, error) {
	prefix, err := netip.ParsePrefix(s)
	if err == nil {
		return lpm.NetIPPrefixToIndexKey(prefix), nil
	}
	addr, err := netip.ParseAddr(s)
	if err != nil {
		return index.Key{}, err
	}
	return lpm.NetIPPrefixToIndexKey(netip.PrefixFrom(addr, addr.BitLen())), nil
}

// indexName implements Indexer.
func (a NetIPPrefixIndex[Obj]) indexName() string {
	return a.Name
}

// isIndexerOf implements Indexer.
func (a NetIPPrefixIndex[Obj]) isIndexerOf(Obj) {
	panic("isIndexerOf")
}

func (i NetIPPrefixIndex[Obj]) isUnique() bool {
	return true
}

// newTableIndex implements Indexer.
func (a NetIPPrefixIndex[Obj]) newTableIndex() tableIndex {
	return lpmIndex{
		lpm: lpm.New[object](),
		objectToKeys: func(obj object) index.KeySet {
			keys := make([]index.Key, 0, 2)
			for prefix := range a.FromObject(obj.data.(Obj)) {
				keys = append(keys, lpm.NetIPPrefixToIndexKey(prefix))
			}
			return index.NewKeySet(keys...)
		},
		watch: make(chan struct{}),
	}
}

var _ Indexer[struct{}] = NetIPPrefixIndex[struct{}]{}

type PrefixLen = lpm.PrefixLen

// LPMIndex implements Longest-Prefix Match indexing.
type LPMIndex[Obj any] struct {
	// Name of the index
	Name string

	// FromObject maps the object to a set of LPM keys
	FromObject func(obj Obj) iter.Seq2[[]byte, PrefixLen]

	FromString func(key string) ([]byte, PrefixLen, error)
}

func (l LPMIndex[Obj]) isIndexerOf(Obj) {
	panic("isIndexerOf")
}

// fromString implements Indexer.
func (l LPMIndex[Obj]) fromString(s string) (index.Key, error) {
	data, plen, err := l.FromString(s)
	if err != nil {
		return index.Key{}, err
	}
	return lpm.EncodeLPMKey(data, plen), nil
}

// indexName implements Indexer.
func (l LPMIndex[Obj]) indexName() string {
	return l.Name
}

func (l LPMIndex[Obj]) ObjectToKey(obj Obj) index.Key {
	for key := range l.FromObject(obj) {
		return key
	}
	return nil
}

func (l LPMIndex[Obj]) Query(data []byte, prefixLen lpm.PrefixLen) Query[Obj] {
	return Query[Obj]{
		index: l.Name,
		key:   lpm.EncodeLPMKey(data, prefixLen),
	}
}

func (l LPMIndex[Obj]) QueryFromObject(obj Obj) Query[Obj] {
	for key := range l.FromObject(obj) {
		return Query[Obj]{
			index: l.Name,
			key:   key,
		}
	}
	return Query[Obj]{}
}

func (l LPMIndex[Obj]) newTableIndex() tableIndex {
	return lpmIndex{
		lpm:     lpm.New[object](),
		prevTxn: nil,
		objectToKeys: func(obj object) index.KeySet {
			keys := make([]index.Key, 0, 2)
			for data, prefixLen := range l.FromObject(obj.data.(Obj)) {
				keys = append(keys, lpm.EncodeLPMKey(data, prefixLen))
			}
			return index.NewKeySet(keys...)
		},
		watch: make(chan struct{}),
	}
}

func (l LPMIndex[Obj]) isUnique() bool {
	return true
}

var _ Indexer[struct{}] = LPMIndex[struct{}]{}

type lpmIndex struct {
	lpm          lpm.Trie[object]
	prevTxn      *lpm.Txn[object]
	objectToKeys func(object) index.KeySet
	watch        chan struct{}
}

// all implements tableIndex.
func (l lpmIndex) all() (tableIndexIterator, <-chan struct{}) {
	return l.lpm.All(), l.watch
}

// get implements tableIndex.
func (l lpmIndex) get(ikey index.Key) (object, <-chan struct{}, bool) {
	obj, found := l.lpm.Lookup(ikey)
	return obj, l.watch, found
}

// len implements tableIndex.
func (l lpmIndex) len() int {
	return l.lpm.Len()
}

// list implements tableIndex.
func (l lpmIndex) list(key index.Key) (tableIndexIterator, <-chan struct{}) {
	// Since LPM is a unique index this is the same as Get().
	obj, _, found := l.get(key)
	if found {
		return &singletonTableIndexIterator{
			key: key,
			obj: obj,
		}, l.watch
	}
	return emptyTableIndexIterator, l.watch
}

// lowerBound implements tableIndex.
func (l lpmIndex) lowerBound(key index.Key) (tableIndexIterator, <-chan struct{}) {
	return l.lpm.LowerBound(key), l.watch
}

// lowerBoundNext implements tableIndexTxn.
func (l lpmIndex) lowerBoundNext(key index.Key) (func() ([]byte, object, bool), <-chan struct{}) {
	return l.lpm.LowerBound(key).Next, l.watch
}

// objectToKey implements tableIndex.
func (l lpmIndex) objectToKey(obj object) index.Key {
	return l.objectToKeys(obj).First()
}

// prefix implements tableIndex.
func (l lpmIndex) prefix(key index.Key) (tableIndexIterator, <-chan struct{}) {
	return l.lpm.Prefix(key), l.watch
}

// rootWatch implements tableIndex.
func (l lpmIndex) rootWatch() <-chan struct{} {
	return l.watch
}

// txn implements tableIndex.
func (l lpmIndex) txn() tableIndexTxn {
	if l.prevTxn != nil {
		return lpmIndexTxn{
			index: l,
			txn:   l.prevTxn.Reuse(l.lpm),
		}
	}
	return lpmIndexTxn{
		index: l,
		txn:   l.lpm.Txn(),
	}
}

var _ tableIndex = lpmIndex{}

type lpmIndexTxn struct {
	index lpmIndex
	txn   *lpm.Txn[object]
}

// all implements tableIndexTxn.
func (l lpmIndexTxn) all() (tableIndexIterator, <-chan struct{}) {
	return l.txn.All(), l.index.watch
}

// commit implements tableIndexTxn.
func (l lpmIndexTxn) commit() tableIndex {
	lpm := l.txn.Commit()
	l.txn.Clear()
	return lpmIndex{
		lpm:          lpm,
		prevTxn:      l.txn,
		objectToKeys: l.index.objectToKeys,
		watch:        make(chan struct{}),
	}
}

// delete implements tableIndexTxn.
func (l lpmIndexTxn) delete(key index.Key) (old object, hadOld bool) {
	panic("LPM index cannot be the primary index")
}

// insert implements tableIndexTxn.
func (l lpmIndexTxn) insert(key index.Key, obj object) (old object, hadOld bool, watch <-chan struct{}) {
	panic("LPM index cannot be the primary index")
}

// modify implements tableIndexTxn.
func (l lpmIndexTxn) modify(key index.Key, obj object, mod func(old object) object) (old object, hadOld bool, watch <-chan struct{}) {
	panic("LPM index cannot be the primary index")
}

// get implements tableIndexTxn.
func (l lpmIndexTxn) get(key index.Key) (object, <-chan struct{}, bool) {
	obj, found := l.txn.Lookup(key)
	return obj, l.index.watch, found
}

// len implements tableIndexTxn.
func (l lpmIndexTxn) len() int {
	return l.txn.Len()
}

// list implements tableIndexTxn.
func (l lpmIndexTxn) list(key index.Key) (tableIndexIterator, <-chan struct{}) {
	// Since LPM is a unique index this is the same as Get().
	obj, _, found := l.get(key)
	if found {
		return &singletonTableIndexIterator{
			key: key,
			obj: obj,
		}, l.index.watch
	}
	return emptyTableIndexIterator, l.index.watch
}

// lowerBound implements tableIndexTxn.
func (l lpmIndexTxn) lowerBound(key index.Key) (tableIndexIterator, <-chan struct{}) {
	return l.txn.LowerBound(key), l.index.watch
}

// lowerBoundNext implements tableIndexTxn.
func (l lpmIndexTxn) lowerBoundNext(key index.Key) (func() ([]byte, object, bool), <-chan struct{}) {
	return l.txn.LowerBound(key).Next, l.index.watch
}

// notify implements tableIndexTxn.
func (l lpmIndexTxn) notify() {
	if l.index.watch != nil {
		close(l.index.watch)
		l.index.watch = nil
	}
}

// objectToKey implements tableIndexTxn.
func (l lpmIndexTxn) objectToKey(obj object) index.Key {
	return l.index.objectToKey(obj)
}

// prefix implements tableIndexTxn.
func (l lpmIndexTxn) prefix(key index.Key) (tableIndexIterator, <-chan struct{}) {
	return l.txn.Prefix(key), l.index.watch
}

// reindex implements tableIndexTxn.
func (l lpmIndexTxn) reindex(primaryKey index.Key, old object, new object) {
	var newKeys index.KeySet
	if new.revision != 0 {
		newKeys = l.index.objectToKeys(new)
		newKeys.Foreach(func(key index.Key) {
			l.txn.Insert(key, new)
		})
	}
	if old.revision != 0 {
		// The old object existed, remove any obsolete keys
		l.index.objectToKeys(old).Foreach(func(oldKey index.Key) {
			if !newKeys.Exists(oldKey) {
				l.txn.Delete(oldKey)
			}
		})
	}
}

// rootWatch implements tableIndexTxn.
func (l lpmIndexTxn) rootWatch() <-chan struct{} {
	return l.index.watch
}

var _ tableIndexTxn = lpmIndexTxn{}
