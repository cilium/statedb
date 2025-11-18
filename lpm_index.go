package statedb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"iter"
	"math/bits"
	"net/netip"
	"slices"
	"strings"
	"unsafe"

	"github.com/cilium/statedb/index"
)

// AddrLPMIndex indexes objects with a [netip.Prefix] in a LPM
// index.
type AddrLPMIndex[Obj any] struct {
	// Name of the index
	Name string

	// FromObject returns the prefixes to index. Use [Just] to
	// return a single prefix.
	FromObject func(obj Obj) iter.Seq[netip.Prefix]
}

func (a AddrLPMIndex[Obj]) Query(addr netip.Addr) Query[Obj] {
	addr16 := addr.As16()
	return Query[Obj]{
		index: a.Name,
		key:   encodeLPMKey(addr16[:], 128),
	}
}

func (a AddrLPMIndex[Obj]) QueryPrefix(prefix netip.Prefix) Query[Obj] {
	return Query[Obj]{
		index: a.Name,
		key:   prefixToIndexKey(prefix),
	}
}

// ObjectToKey implements Indexer.
func (a AddrLPMIndex[Obj]) ObjectToKey(obj Obj) index.Key {
	for prefix := range a.FromObject(obj) {
		return prefixToIndexKey(prefix)
	}
	return nil
}

// QueryFromObject implements Indexer.
func (a AddrLPMIndex[Obj]) QueryFromObject(obj Obj) Query[Obj] {
	for prefix := range a.FromObject(obj) {
		return Query[Obj]{
			index: a.Name,
			key:   prefixToIndexKey(prefix),
		}
	}
	return Query[Obj]{}
}

// fromString implements Indexer.
func (a AddrLPMIndex[Obj]) fromString(s string) (index.Key, error) {
	prefix, err := netip.ParsePrefix(s)
	if err != nil {
		return nil, err
	}
	return prefixToIndexKey(prefix), nil
}

// indexName implements Indexer.
func (a AddrLPMIndex[Obj]) indexName() string {
	return a.Name
}

// isIndexerOf implements Indexer.
func (a AddrLPMIndex[Obj]) isIndexerOf(Obj) {
	panic("isIndexerOf")
}

func (i AddrLPMIndex[Obj]) isUnique() bool {
	return true
}

// newTableIndex implements Indexer.
func (a AddrLPMIndex[Obj]) newTableIndex() tableIndex {
	return lpmIndex{
		lpm: newLPMTrie[*object](128),
		objectToKeys: func(obj *object) iter.Seq[index.Key] {
			return func(yield func(index.Key) bool) {
				for prefix := range a.FromObject(obj.data.(Obj)) {
					if !yield(prefixToIndexKey(prefix)) {
						return
					}
				}
			}
		},
		watch: make(chan struct{}),
	}
}

type PrefixLen = uint16

var _ Indexer[struct{}] = AddrLPMIndex[struct{}]{}

// LPMIndex implements Longest-Prefix Match indexing.
type LPMIndex[Obj any] struct {
	// Name of the index
	Name string

	// Maximum prefix length
	MaxPrefixLen PrefixLen

	// FromObject maps the object to a set of LPM keys
	FromObject func(obj Obj) iter.Seq2[[]byte, PrefixLen]

	FromString func(key string) (index.Key, error)
}

func (l LPMIndex[Obj]) isIndexerOf(Obj) {
	panic("isIndexerOf")
}

// fromString implements Indexer.
func (l LPMIndex[Obj]) fromString(s string) (index.Key, error) {
	return l.FromString(s)
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

func (l LPMIndex[Obj]) Query(data []byte, prefixLen PrefixLen) Query[Obj] {
	return Query[Obj]{
		index: l.Name,
		key:   encodeLPMKey(data, prefixLen),
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
		lpm: newLPMTrie[*object](l.MaxPrefixLen),
		objectToKeys: func(obj *object) iter.Seq[index.Key] {
			return func(yield func(index.Key) bool) {
				for data, prefixLen := range l.FromObject(obj.data.(Obj)) {
					if !yield(encodeLPMKey(data, prefixLen)) {
						break
					}
				}
			}
		},
		watch: make(chan struct{}),
	}
}

func (l LPMIndex[Obj]) isUnique() bool {
	return true
}

var _ Indexer[struct{}] = LPMIndex[struct{}]{}

type lpmIndex struct {
	lpm          lpmTrie[*object]
	objectToKeys func(*object) iter.Seq[index.Key]
	watch        chan struct{}
}

// emptyPrefix is a prefix with 0 length.
var emptyPrefix = index.Key{0, 0}

// all implements tableIndex.
func (l lpmIndex) all() (iter.Seq2[index.Key, *object], <-chan struct{}) {
	return lpmSeq(l.lpm.Txn().Prefix(emptyPrefix)), l.watch
}

func lpmSeq(s iter.Seq2[index.Key, *object]) iter.Seq2[index.Key, *object] {
	return func(yield func(index.Key, *object) bool) {
		for k, o := range s {
			if !yield(k, o) {
				break
			}
		}
	}
}

// get implements tableIndex.
func (l lpmIndex) get(ikey index.Key) (*object, <-chan struct{}, bool) {
	obj, found := l.lpm.Lookup(ikey)
	return obj, l.watch, found
}

// len implements tableIndex.
func (l lpmIndex) len() int {
	return l.lpm.size
}

// list implements tableIndex.
func (l lpmIndex) list(key index.Key) (iter.Seq[*object], <-chan struct{}) {
	// TODO what should this actually do? e.g. if we do
	// List("10.0.0.0/8") should we return only "10.0.0.0/8" or if it does't exist
	// say 10.0.0.0/7 etc?
	obj, _, found := l.get(key)
	return func(yield func(*object) bool) {
		if found {
			yield(obj)
		}
	}, l.watch
}

// lowerBound implements tableIndex.
func (l lpmIndex) lowerBound(key index.Key) (iter.Seq[*object], <-chan struct{}) {
	return Values(lpmSeq(l.lpm.Txn().LowerBound(key))), l.watch
}

// objectToKey implements tableIndex.
func (l lpmIndex) objectToKey(obj *object) index.Key {
	for k := range l.objectToKeys(obj) {
		return k
	}
	panic("bug")
}

// prefix implements tableIndex.
func (l lpmIndex) prefix(key index.Key) (iter.Seq[*object], <-chan struct{}) {
	return Values(lpmSeq(l.lpm.Txn().Prefix(key))), l.watch
}

// rootWatch implements tableIndex.
func (l lpmIndex) rootWatch() <-chan struct{} {
	return l.watch
}

// txn implements tableIndex.
func (l lpmIndex) txn() tableIndexTxn {
	return lpmIndexTxn{
		index: l,
		txn:   l.lpm.Txn(),
	}
}

var _ tableIndex = lpmIndex{}

type lpmIndexTxn struct {
	index lpmIndex
	txn   *lpmTxn[*object]
}

// all implements tableIndexTxn.
func (l lpmIndexTxn) all() (iter.Seq2[index.Key, *object], <-chan struct{}) {
	return lpmSeq(l.txn.Prefix(emptyPrefix)), l.index.watch
}

// commit implements tableIndexTxn.
func (l lpmIndexTxn) commit() tableIndex {
	return lpmIndex{
		lpm:          l.txn.Commit(),
		objectToKeys: l.index.objectToKeys,
		watch:        make(chan struct{}),
	}
}

// delete implements tableIndexTxn.
func (l lpmIndexTxn) delete(key index.Key) (old *object, hadOld bool) {
	panic("LPM index cannot be the primary index")
}

// insert implements tableIndexTxn.
func (l lpmIndexTxn) insert(key index.Key, obj *object) (old *object, hadOld bool, watch <-chan struct{}) {
	panic("LPM index cannot be the primary index")
}

// get implements tableIndexTxn.
func (l lpmIndexTxn) get(key index.Key) (*object, <-chan struct{}, bool) {
	obj, found := l.txn.Lookup(key)
	return obj, l.index.watch, found
}

// len implements tableIndexTxn.
func (l lpmIndexTxn) len() int {
	return l.txn.size
}

// list implements tableIndexTxn.
func (l lpmIndexTxn) list(key index.Key) (iter.Seq[*object], <-chan struct{}) {
	// TODO what should this do
	obj, _, found := l.get(key)
	return func(yield func(*object) bool) {
		if found {
			yield(obj)
		}
	}, l.index.watch
}

// lowerBound implements tableIndexTxn.
func (l lpmIndexTxn) lowerBound(key index.Key) (iter.Seq[*object], <-chan struct{}) {
	return Values(lpmSeq(l.txn.LowerBound(key))), l.index.watch
}

// modify implements tableIndexTxn.
func (l lpmIndexTxn) modify(key index.Key, obj *object, mod func(old *object) *object) (old *object, hadOld bool, watch <-chan struct{}) {
	panic("LPM index cannot be the primary index")
}

// notify implements tableIndexTxn.
func (l lpmIndexTxn) notify() {
	if l.index.watch != nil {
		close(l.index.watch)
		l.index.watch = nil
	}
}

// objectToKey implements tableIndexTxn.
func (l lpmIndexTxn) objectToKey(obj *object) index.Key {
	for k := range l.index.objectToKeys(obj) {
		return k
	}
	panic("bug")
}

// prefix implements tableIndexTxn.
func (l lpmIndexTxn) prefix(key index.Key) (iter.Seq[*object], <-chan struct{}) {
	return Values(lpmSeq(l.txn.Prefix(key))), l.index.watch
}

// reindex implements tableIndexTxn.
func (l lpmIndexTxn) reindex(primaryKey index.Key, old *object, new *object) {
	var newKeys []index.Key
	if new != nil {
		newKeys = slices.Collect(l.index.objectToKeys(new))
	}
	newKeysContains := func(k index.Key) bool {
		for _, k2 := range newKeys {
			dataOld, oldLen := decodeLPMKey(k)
			dataNew, newLen := decodeLPMKey(k2)
			if oldLen == newLen && bytes.Equal(dataOld, dataNew) {
				return true
			}
		}
		return false
	}
	if old != nil {
		// The old object existed, remove any obsolete keys
		for oldKey := range l.index.objectToKeys(old) {
			if !newKeysContains(oldKey) {
				l.txn.Delete(oldKey)
			}
		}
	}
	for _, key := range newKeys {
		l.txn.Insert(key, new)
	}
}

// rootWatch implements tableIndexTxn.
func (l lpmIndexTxn) rootWatch() <-chan struct{} {
	return l.index.watch
}

// snapshot implements tableIndexTxn.
func (l lpmIndexTxn) snapshot() tableIndexReader {
	// FIXME: right now this works since there's no state that is being
	// reused but might break at some point.
	l.txn.mutated.clear()
	return l.commit()
}

var _ tableIndexTxn = lpmIndexTxn{}

func newLPMTrie[T any](maxPrefixLen PrefixLen) lpmTrie[T] {
	return lpmTrie[T]{
		root:         nil,
		maxPrefixLen: maxPrefixLen,
	}
}

type lpmTrie[T any] struct {
	root         *lpmNode[T]
	size         int
	maxPrefixLen PrefixLen
}

func (l *lpmTrie[T]) Txn() *lpmTxn[T] {
	return &lpmTxn[T]{
		root:         l.root,
		maxPrefixLen: l.maxPrefixLen,
	}
}

func (l *lpmTrie[T]) print() {
	if l.root == nil {
		fmt.Printf("<empty>\n")
		return
	}
	var printNode func(*lpmNode[T], int)
	printNode = func(ln *lpmNode[T], indent int) {
		img := ""
		if ln.imaginary {
			img = "*"
		}
		fmt.Printf("%s%s/%d%s -> %v\n", strings.Repeat(" ", indent), showKey(ln.key), ln.prefixLen(), img, ln.value)
		if ln.children[0] != nil {
			printNode(ln.children[0], indent+2)
		}
		if ln.children[1] != nil {
			printNode(ln.children[1], indent+2)
		}
	}
	printNode(l.root, 0)
}

func (l *lpmTrie[T]) Lookup(key index.Key) (value T, found bool) {
	return lpmLookup(l.root, key)
}

type lpmNode[T any] struct {
	children  [2]*lpmNode[T]
	key       index.Key
	value     T
	imaginary bool
}

func (n *lpmNode[T]) prefixLen() PrefixLen {
	_, len := decodeLPMKey(n.key)
	return len
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

func (txn *lpmTxn[T]) Insert(key index.Key, value T) error {
	data, prefixLen := decodeLPMKey(key)
	if prefixLen > txn.maxPrefixLen {
		return fmt.Errorf("prefix length %d larger than max %d", prefixLen, txn.maxPrefixLen)
	}
	newNode := &lpmNode[T]{
		children:  [2]*lpmNode[T]{},
		key:       key,
		value:     value,
		imaginary: false,
	}

	txn.size++
	txn.root = txn.clone(txn.root)
	nodep := &txn.root
	node := *nodep

	var matchLen PrefixLen
	for node != nil {
		matchLen = longestMatch(matchLen, node, data, prefixLen)
		nodePrefixLen := node.prefixLen()
		if nodePrefixLen != matchLen ||
			nodePrefixLen == prefixLen ||
			nodePrefixLen == txn.maxPrefixLen {
			break
		}
		nodep = &node.children[getBitAt(data, nodePrefixLen)]
		*nodep = txn.clone(*nodep)
		node = *nodep
	}

	switch {
	case node == nil:
		*nodep = newNode
	case node.prefixLen() == matchLen:
		newNode.children = node.children
		*nodep = newNode
	case matchLen == prefixLen:
		index := getBitAt(node.key, matchLen)
		newNode.children[index] = node
		*nodep = newNode
	default:
		imaginary := &lpmNode[T]{
			children:  [2]*lpmNode[T]{},
			key:       encodeLPMKey(node.key, matchLen),
			imaginary: true,
		}
		switch getBitAt(data, matchLen) {
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

func (txn *lpmTxn[T]) Delete(key index.Key) (value T, found bool) {
	data, prefixLen := decodeLPMKey(key)
	if prefixLen > txn.maxPrefixLen {
		return
	}

	// Find the node containing the value we want to remove.
	// Collect the parent nodes on the path to the target.
	type parent struct {
		node  *lpmNode[T]
		index int
	}
	parents := make([]parent, 0, 32)
	var matchLen PrefixLen
	node := txn.root
	index := 0

	for node != nil {
		nodeData, nodePrefixLen := decodeLPMKey(node.key)
		// Exact match on non-imaginery node?
		if !node.imaginary &&
			prefixLen == nodePrefixLen &&
			bytes.Equal(data, nodeData) {
			break
		}

		// Partial match?
		matchLen = longestMatch(matchLen, node, data, prefixLen)
		if matchLen < nodePrefixLen || matchLen == prefixLen {
			return
		}
		index = getBitAt(data, nodePrefixLen)
		parents = append(parents, parent{node, index})
		node = node.children[index]
	}
	if node == nil {
		return
	}
	value = node.value

	// Turn the node imaginary to mark it for removal.
	node = txn.clone(node)
	var zero T
	node.value = zero
	node.imaginary = true

	// Reconstruct the parents, compressing the trie along the way.
	for i := len(parents) - 1; i >= 0; i-- {
		parents[i].node = txn.clone(parents[i].node)
		parent := parents[i].node
		index := parents[i].index
		if node.imaginary {
			switch {
			case node.children[0] == nil && node.children[1] == nil:
				// Node is empty and can be removed from the parent
				node = nil

			case node.children[0] != nil && node.children[1] == nil && index == 0:
				// One child and it matches the index at the parent
				node = node.children[0]

			case node.children[0] == nil && node.children[1] != nil && index == 1:
				// One child and it matches the index at the parent
				node = node.children[1]
			}
		}
		parent.children[index] = node
		node = parent
	}

	if len(parents) > 0 {
		txn.root = parents[0].node
	} else {
		txn.root = nil
	}

	txn.size--

	return value, true
}

func (txn *lpmTxn[T]) Lookup(key index.Key) (value T, found bool) {
	return lpmLookup(txn.root, key)
}

func (txn *lpmTxn[T]) Prefix(key index.Key) iter.Seq2[index.Key, T] {
	node := txn.root
	data, prefixLen := decodeLPMKey(key)

	var matchLen PrefixLen
	for node != nil {
		matchLen = longestMatch(matchLen, node, data, prefixLen)
		if matchLen == prefixLen || matchLen < node.prefixLen() {
			break
		}
		node = node.children[getBitAt(data, node.prefixLen())]
	}

	return func(yield func(index.Key, T) bool) {
		if node == nil {
			return
		}
		stack := make([]*lpmNode[T], 1, 32)
		stack[0] = node
		for len(stack) > 0 {
			node := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if !node.imaginary && !yield(node.key, node.value) {
				break
			}
			if node.children[1] != nil {
				stack = append(stack, node.children[1])
			}
			if node.children[0] != nil {
				stack = append(stack, node.children[0])
			}
		}
	}
}

func (txn *lpmTxn[T]) LowerBound(key index.Key) iter.Seq2[index.Key, T] {
	data, prefixLen := decodeLPMKey(key)
	node := txn.root
	stack := make([]*lpmNode[T], 0, 32)
	var matchLen PrefixLen
	for node != nil {
		matchLen = longestMatch(matchLen, node, data, prefixLen)
		if matchLen == prefixLen {
			stack = append(stack, node)
			break
		}
		if matchLen < node.prefixLen() {
			if bytes.Compare(node.key, data) >= 0 {
				stack = append(stack, node)
			}
			break
		}
		index := getBitAt(data, node.prefixLen())
		if index == 0 && node.children[1] != nil {
			// Add all the larger children to the stack.
			stack = append(stack, node.children[1])
		}
		node = node.children[index]
	}

	/*
		fmt.Println("stack:")
		for i, n := range stack {
			fmt.Printf("%d: %x/%d\n", i, n.key, n.prefixLen)
		}*/

	return func(yield func(index.Key, T) bool) {
		if len(stack) == 0 {
			return
		}
		stack := slices.Clone(stack)
		for len(stack) > 0 {
			node := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if !node.imaginary && !yield(node.key, node.value) {
				break
			}
			if node.children[1] != nil {
				stack = append(stack, node.children[1])
			}
			if node.children[0] != nil {
				stack = append(stack, node.children[0])
			}
		}
	}
}

func (txn *lpmTxn[T]) Commit() lpmTrie[T] {
	return lpmTrie[T]{
		root:         txn.root,
		size:         txn.size,
		maxPrefixLen: txn.maxPrefixLen,
	}
}

func longestMatch[T any](startLen PrefixLen, node *lpmNode[T], keyData []byte, keyPrefixLen PrefixLen) PrefixLen {
	keySize := uint16(min(len(node.key), len(keyData)))
	startLenBytes := startLen / 8
	prefixLen := 8 * startLenBytes
	nodePrefixLen := node.prefixLen()
	minPrefixLen := min(nodePrefixLen, keyPrefixLen)
	for i := startLenBytes; i < keySize; i++ {
		matchLenInByte := bits.LeadingZeros8(node.key[i] ^ keyData[i])
		prefixLen += PrefixLen(matchLenInByte)
		if prefixLen >= minPrefixLen {
			return minPrefixLen
		}
		if matchLenInByte < 8 {
			// Less than full byte matched, we can stop.
			break
		}
	}
	return prefixLen
}

func lpmLookup[T any](root *lpmNode[T], key index.Key) (value T, ok bool) {
	keyData, keyPrefixLen := decodeLPMKey(key)
	var closest *lpmNode[T]
	node := root
	currentLen := PrefixLen(0)
	for node != nil {
		nodePrefixLen := node.prefixLen()
		matchLen := longestMatch(currentLen, node, keyData, keyPrefixLen)
		if matchLen == keyPrefixLen {
			return node.value, !node.imaginary
		}
		if matchLen < nodePrefixLen {
			break
		}
		currentLen = nodePrefixLen
		if !node.imaginary {
			closest = node
		}
		node = node.children[getBitAt(keyData, nodePrefixLen)]
	}
	if closest != nil {
		return closest.value, true
	}
	return value, false
}

func getBitAt(data []byte, index uint16) int {
	return int(data[index/8]>>(7-(index%8))) & 1
}

func showKey(key index.Key) string {
	data, bits := decodeLPMKey(key)
	var w strings.Builder
	for i, b := range data {
		mask := byte(0xff)
		if bits < 8 {
			mask <<= (8 - bits)
		}
		if len(data) == 4 { // "ipv4"
			fmt.Fprintf(&w, "%d", b&mask)
		} else { // "ipv6"
			fmt.Fprintf(&w, "%x", b&mask)
		}
		if i != len(data)-1 {
			fmt.Fprint(&w, ".")
		}
		bits -= 8
	}
	return w.String()
}

type lpmTxn[T any] struct {
	root         *lpmNode[T]
	size         int
	maxPrefixLen PrefixLen
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
	nm.ptrs[nm.slot(ptrInt)] = ptrInt
	nm.used = true
}

func lpmNodeMutatedExists[T any](nm *lpmNodeMutated, ptr *lpmNode[T]) bool {
	if nm == nil {
		return false
	}
	ptrInt := uintptr(unsafe.Pointer(ptr))
	return nm.ptrs[nm.slot(ptrInt)] == ptrInt
}

func (nm *lpmNodeMutated) slot(p uintptr) int {
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

func prefixToIndexKey(prefix netip.Prefix) index.Key {
	addr := prefix.Addr().As16()
	bits := prefix.Bits()
	if prefix.Addr().Is4() {
		bits += 12 * 8
	}
	return encodeLPMKey(
		addr[:],
		PrefixLen(bits),
	)
}

func encodeLPMKey(data []byte, prefixLen PrefixLen) index.Key {
	dataLen := (prefixLen + 7) / 8
	key := make(index.Key, dataLen, dataLen+2)
	copy(key, data[:dataLen])
	return binary.BigEndian.AppendUint16(key, prefixLen)
}

func decodeLPMKey(key index.Key) (data []byte, prefixLen PrefixLen) {
	if len(key) < 2 {
		panic("invalid LPM key")
	}
	data = key[:len(key)-2]
	prefixLen = binary.BigEndian.Uint16(key[len(key)-2:])
	return
}
