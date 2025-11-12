package statedb

import (
	"bytes"
	"fmt"
	"iter"
	"math/bits"
	"slices"
	"unsafe"

	"github.com/cilium/statedb/index"
)

type LPMKey struct {
	Key []byte

	// PrefixLen is the length of the key in bits.
	PrefixLen int
}

// LPMIndex implements Longest-Prefix Match indexing.
type LPMIndex[Obj any] struct {
	// Name of the index
	Name string

	// Maximum prefix length
	MaxPrefixLen int

	// FromObject maps the object to a set of LPM keys
	FromObject func(obj Obj) iter.Seq[LPMKey]

	FromString func(key string) (LPMKey, error)
}

func (l LPMIndex[Obj]) isIndexerOf(Obj) {
	panic("isIndexerOf")
}

// fromString implements Indexer.
func (l LPMIndex[Obj]) fromString(s string) (indexKey, error) {
	return l.FromString(s)
}

// indexName implements Indexer.
func (l LPMIndex[Obj]) indexName() string {
	return l.Name
}

func (l LPMIndex[Obj]) ObjectToKey(obj Obj) index.Key {
	// TODO: Get rid of this method completely. Currently only used by the reconciler
	// for getting a key for retries. Figure out another way.
	panic("not supported")
}

func (l LPMIndex[Obj]) QueryFromObject(obj Obj) Query[Obj] {
	panic("do we need this?")
}

func (l LPMIndex[Obj]) newTableIndex() tableIndex {
	return lpmIndex{
		lpm: newLPM[object](l.MaxPrefixLen),
		objectToKeys: func(obj object) iter.Seq[LPMKey] {
			return l.FromObject(obj.data.(Obj))
		},
	}
}

var _ Indexer[struct{}] = LPMIndex[struct{}]{}

type lpmIndex struct {
	lpm          lpm[object]
	objectToKeys func(object) iter.Seq[LPMKey]
}

// all implements tableIndex.
func (l lpmIndex) all() (iter.Seq2[indexKey, object], <-chan struct{}) {
	panic("unimplemented")
}

// get implements tableIndex.
func (l lpmIndex) get(ikey indexKey) (object, <-chan struct{}, bool) {
	key := ikey.(LPMKey)
	obj, found := l.lpm.Lookup(key)
	return obj, nil, found
}

// len implements tableIndex.
func (l lpmIndex) len() int {
	panic("unimplemented")
}

// list implements tableIndex.
func (l lpmIndex) list(key indexKey) (iter.Seq[object], <-chan struct{}) {
	// TODO what should this actually do? e.g. if we do
	// List("10.0.0.0/8") should we return only "10.0.0.0/8" or if it does't exist
	// say 10.0.0.0/7 etc?
	obj, _, found := l.get(key)
	return func(yield func(object) bool) {
		if found {
			yield(obj)
		}
	}, nil
}

// lowerBound implements tableIndex.
func (l lpmIndex) lowerBound(key indexKey) (iter.Seq[object], <-chan struct{}) {
	panic("unimplemented")
}

// objectToKey implements tableIndex.
func (l lpmIndex) objectToKey(obj object) indexKey {
	panic("unimplemented")
}

// prefix implements tableIndex.
func (l lpmIndex) prefix(key indexKey) (iter.Seq[object], <-chan struct{}) {
	panic("unimplemented")
}

// rootWatch implements tableIndex.
func (l lpmIndex) rootWatch() <-chan struct{} {
	panic("unimplemented")
}

// txn implements tableIndex.
func (l lpmIndex) txn() tableIndexTxn {
	panic("unimplemented")
}

var _ tableIndex = lpmIndex{}

type lpmIndexTxn struct {
	index lpmIndex
	txn   *lpmTxn[object]
}

// abort implements tableIndexTxn.
func (l lpmIndexTxn) abort() {
	panic("unimplemented")
}

// all implements tableIndexTxn.
func (l lpmIndexTxn) all() (iter.Seq2[indexKey, object], <-chan struct{}) {
	panic("unimplemented")
}

// commit implements tableIndexTxn.
func (l lpmIndexTxn) commit() tableIndex {
	panic("unimplemented")
}

// delete implements tableIndexTxn.
func (l lpmIndexTxn) delete(obj object) (old object, hadOld bool) {
	panic("LPM index cannot be the primary index")
}

// deleteByKey implements tableIndexTxn.
func (l lpmIndexTxn) deleteByKey(indexKey) (old object, hadOld bool) {
	panic("LPM index cannot be the primary index")
}

// get implements tableIndexTxn.
func (l lpmIndexTxn) get(key indexKey) (object, <-chan struct{}, bool) {
	panic("unimplemented")
}

// insert implements tableIndexTxn.
func (l lpmIndexTxn) insert(obj object) (old object, hadOld bool, watch <-chan struct{}) {
	panic("LPM index cannot be the primary index")
}

// len implements tableIndexTxn.
func (l lpmIndexTxn) len() int {
	panic("unimplemented")
}

// list implements tableIndexTxn.
func (l lpmIndexTxn) list(key indexKey) (iter.Seq[object], <-chan struct{}) {
	panic("unimplemented")
}

// lowerBound implements tableIndexTxn.
func (l lpmIndexTxn) lowerBound(key indexKey) (iter.Seq[object], <-chan struct{}) {
	panic("unimplemented")
}

// modify implements tableIndexTxn.
func (l lpmIndexTxn) modify(obj object, mod func(old object) object) (old object, hadOld bool, watch <-chan struct{}) {
	panic("LPM index cannot be the primary index")
}

// notify implements tableIndexTxn.
func (l lpmIndexTxn) notify() {
	panic("unimplemented")
}

// objectToKey implements tableIndexTxn.
func (l lpmIndexTxn) objectToKey(obj object) indexKey {
	panic("unimplemented")
}

// prefix implements tableIndexTxn.
func (l lpmIndexTxn) prefix(key indexKey) (iter.Seq[object], <-chan struct{}) {
	panic("unimplemented")
}

// reindex implements tableIndexTxn.
func (l lpmIndexTxn) reindex(primaryKey indexKey, old *object, new *object) {
	var newKeys []LPMKey
	if new != nil {
		// TODO can we avoid the allocation?
		newKeys = slices.Collect(l.index.objectToKeys(*new))
	}

	if old != nil {
		// The old object existed, remove any obsolete keys
		for oldKey := range l.index.objectToKeys(*old) {
			if !slices.ContainsFunc(newKeys, func(k LPMKey) bool {
				return bytes.Equal(k.Key, oldKey.Key) && k.PrefixLen == oldKey.PrefixLen
			}) {
				panic("FIXME")
				/*
					_, hadOld := l.txn.Delete(oldKey)
					if !hadOld {
						panic("BUG: delete did not find old object")
					}*/
			}
		}
	}

	for _, key := range newKeys {
		l.txn.Insert(key, *new)
	}
}

// rootWatch implements tableIndexTxn.
func (l lpmIndexTxn) rootWatch() <-chan struct{} {
	panic("unimplemented")
}

// snapshot implements tableIndexTxn.
func (l lpmIndexTxn) snapshot() tableIndexReader {
	panic("unimplemented")
}

var _ tableIndexTxn = lpmIndexTxn{}

func newLPM[T any](maxPrefixLen int) lpm[T] {
	return lpm[T]{
		root:         nil,
		maxPrefixLen: maxPrefixLen,
	}
}

type lpm[T any] struct {
	root         *lpmNode[T]
	maxPrefixLen int
}

func (l *lpm[T]) Txn() *lpmTxn[T] {
	return &lpmTxn[T]{
		root:         l.root,
		maxPrefixLen: l.maxPrefixLen,
	}
}

func (l *lpm[T]) Lookup(key LPMKey) (value T, found bool) {
	return lpmLookup(l.root, l.maxPrefixLen, key)
}

type lpmNode[T any] struct {
	children  [2]*lpmNode[T]
	key       []byte
	value     T
	prefixLen int
	imaginary bool
}

func (txn *lpmTxn[T]) clone(n *lpmNode[T]) *lpmNode[T] {
	if n == nil {
		return nil
	}
	if lpmNodeMutatedExists(&txn.mutated, n) {
		return n
	}

	n2 := *n
	n = &n2
	lpmNodeMutatedSet(&txn.mutated, n)
	return n
}

func (txn *lpmTxn[T]) Insert(key LPMKey, value T) error {
	if key.PrefixLen > txn.maxPrefixLen {
		return fmt.Errorf("prefix length %d larger than max %d", key.PrefixLen, txn.maxPrefixLen)
	}
	newNode := &lpmNode[T]{
		children:  [2]*lpmNode[T]{},
		key:       key.Key,
		value:     value,
		prefixLen: key.PrefixLen,
		imaginary: false,
	}

	txn.root = txn.clone(txn.root)
	nodep := &txn.root
	node := *nodep

	var matchLen int
	for node != nil {
		matchLen = longestMatch(txn.maxPrefixLen, node, key)
		if node.prefixLen != matchLen ||
			node.prefixLen == key.PrefixLen ||
			node.prefixLen == txn.maxPrefixLen {
			break
		}
		nodep = &node.children[getBitAt(key.Key, node.prefixLen)]
		*nodep = txn.clone(*nodep)
		node = *nodep
	}

	switch {
	case node == nil:
		*nodep = newNode
	case node.prefixLen == matchLen:
		newNode.children = node.children
		*nodep = newNode
	case matchLen == key.PrefixLen:
		index := getBitAt(node.key, matchLen)
		newNode.children[index] = node
		*nodep = newNode
	default:
		imaginary := &lpmNode[T]{
			children:  [2]*lpmNode[T]{},
			key:       node.key,
			prefixLen: matchLen,
			imaginary: true,
		}
		switch getBitAt(key.Key, matchLen) {
		case 0:
			imaginary.children[0] = newNode
			imaginary.children[1] = node
		case 1:
			imaginary.children[0] = node
			imaginary.children[1] = newNode
		}
		*nodep = imaginary
	}
	return nil
}

func (txn *lpmTxn[T]) Lookup(key LPMKey) (value T, found bool) {
	return lpmLookup(txn.root, txn.maxPrefixLen, key)
}

func (txn *lpmTxn[T]) Prefix(key LPMKey) iter.Seq[T] {
	keySize := (txn.maxPrefixLen + 7) / 8
	node := txn.root
	for node != nil {
		matchLen := longestMatch(keySize, node, key)
		if matchLen == txn.maxPrefixLen {
			break
		}
		if matchLen < node.prefixLen {
			break
		}
		node = node.children[getBitAt(key.Key, node.prefixLen)]
	}
	return func(yield func(T) bool) {
		// FIXME what order is this doing?
		stack := []*lpmNode[T]{node}
		for len(stack) > 0 {
			node := stack[0]
			stack = stack[1:]
			if !node.imaginary && !yield(node.value) {
				break
			}
			if node.children[0] != nil {
				stack = append(stack, node.children[0])
			}
			if node.children[1] != nil {
				stack = append(stack, node.children[1])
			}
		}
	}
}

func (txn *lpmTxn[T]) Commit() lpm[T] {
	return lpm[T]{
		root:         txn.root,
		maxPrefixLen: txn.maxPrefixLen,
	}
}

func longestMatch[T any](maxPrefixLen int, n *lpmNode[T], key LPMKey) int {
	keySize := (maxPrefixLen + 7) / 8
	prefixLen := 0
	for i := range keySize {
		matchLenInByte := bits.LeadingZeros8(n.key[i] ^ key.Key[i])
		prefixLen += matchLenInByte
		if n := min(n.prefixLen, key.PrefixLen); prefixLen >= n {
			return n
		}
		if matchLenInByte < 8 {
			// Less than full byte matched, we can stop.
			break
		}
	}
	return prefixLen
}

func lpmLookup[T any](root *lpmNode[T], maxPrefixLen int, key LPMKey) (value T, ok bool) {
	keySize := (maxPrefixLen + 7) / 8
	var closest *lpmNode[T]
	node := root
	for node != nil {
		matchLen := longestMatch(keySize, node, key)
		if matchLen == maxPrefixLen {
			return node.value, true
		}
		if matchLen < node.prefixLen {
			break
		}
		if !node.imaginary {
			closest = node
		}
		node = node.children[getBitAt(key.Key, node.prefixLen)]
	}
	if closest != nil {
		return closest.value, true
	}
	return value, false
}

func getBitAt(data []byte, index int) int {
	return int(data[index/8]>>(7-(index%8))) & 1
}

type lpmTxn[T any] struct {
	root         *lpmNode[T]
	maxPrefixLen int
	mutated      lpmNodeMutated
}

// ---
// FIXME: Share the mutation cache implementation with 'part'
const lpmNodeMutatedSize = 256 // must be power-of-two

type lpmNodeMutated struct {
	ptrs [lpmNodeMutatedSize]uintptr
	used bool
}

func lpmNodeMutatedSet[T any](nm *lpmNodeMutated, ptr *lpmNode[T]) {
	if nm == nil {
		return
	}
	ptrInt := uintptr(unsafe.Pointer(ptr))
	nm.ptrs[slot(ptrInt)] = ptrInt
	nm.used = true
}

func lpmNodeMutatedExists[T any](nm *lpmNodeMutated, ptr *lpmNode[T]) bool {
	if nm == nil {
		return false
	}
	ptrInt := uintptr(unsafe.Pointer(ptr))
	return nm.ptrs[slot(ptrInt)] == ptrInt
}

func slot(p uintptr) int {
	p >>= 4 // ignore low order bits
	// use some relevant bits from the pointer
	slot := uint8(p) ^ uint8(p>>8) ^ uint8(p>>16)
	return int(slot & (lpmNodeMutatedSize - 1))
}

func (nm *lpmNodeMutated) clear() {
	if nm == nil {
		return
	}
	if nm.used {
		clear(nm.ptrs[:])
	}
	nm.used = false
}
