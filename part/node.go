// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package part

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"unsafe"
)

type nodeKind uint8

const (
	nodeKindUnknown = iota
	nodeKindLeaf
	nodeKind4
	nodeKind16
	nodeKind48
	nodeKind256
)

type header[T any] struct {
	flags  uint16        // kind(4b) | unused(3b) | size(9b)
	prefix []byte        // the compressed prefix, [0] is the key
	leaf   *leaf[T]      // non-nil if this node contains a value
	watch  chan struct{} // watch channel that is closed when this node mutates
}

const kindMask = uint16(0b1111_000_00000000_0)

func (n *header[T]) kind() nodeKind {
	return nodeKind(n.flags >> 12)
}

func (n *header[T]) setKind(k nodeKind) {
	n.flags = (n.flags & ^kindMask) | (uint16(k&0b1111) << 12)
}

const sizeMask = uint16(0b0000_000_1111_1111_1)

func (n *header[T]) cap() int {
	switch n.kind() {
	case nodeKindLeaf:
		return 0
	case nodeKind4:
		return 4
	case nodeKind16:
		return 16
	case nodeKind48:
		return 48
	case nodeKind256:
		return 256
	default:
		panic("unknown node kind")
	}
}

func (n *header[T]) isLeaf() bool {
	return n.kind() == nodeKindLeaf
}

func (n *header[T]) getLeaf() *leaf[T] {
	if n.isLeaf() {
		return (*leaf[T])(unsafe.Pointer(n))
	}
	return n.leaf
}

func (n *header[T]) size() int {
	return int(n.flags & sizeMask)
}

func (n *header[T]) setSize(size int) {
	n.flags = (n.flags & ^sizeMask) | uint16(size)&sizeMask
}

func (n *header[T]) self() *header[T] {
	return n
}

func (n *header[T]) node4() *node4[T] {
	return (*node4[T])(unsafe.Pointer(n))
}

func (n *header[T]) node16() *node16[T] {
	return (*node16[T])(unsafe.Pointer(n))
}

func (n *header[T]) node48() *node48[T] {
	return (*node48[T])(unsafe.Pointer(n))
}

func (n *header[T]) node256() *node256[T] {
	return (*node256[T])(unsafe.Pointer(n))
}

// clone returns a shallow clone of the node.
// We are working on the assumption here that only
// value-types are mutated in the returned clone.
func (n *header[T]) clone(watch bool) *header[T] {
	var nCopy *header[T]
	switch n.kind() {
	case nodeKindLeaf:
		l := *n.getLeaf()
		nCopy = (&l).self()
	case nodeKind4:
		n4 := *n.node4()
		nCopy = (&n4).self()
	case nodeKind16:
		n16 := *n.node16()
		nCopy = (&n16).self()
	case nodeKind48:
		n48 := *n.node48()
		nCopy = (&n48).self()
	case nodeKind256:
		nCopy256 := *n.node256()
		nCopy = (&nCopy256).self()
	default:
		panic("unknown node kind")
	}
	if watch {
		nCopy.watch = make(chan struct{})
	} else {
		nCopy.watch = nil
	}
	return nCopy
}

func (n *header[T]) promote(watch bool) *header[T] {
	switch n.kind() {
	case nodeKindLeaf:
		node4 := &node4[T]{}
		node4.prefix = n.prefix
		node4.leaf = n.getLeaf()
		node4.setKind(nodeKind4)
		if watch {
			node4.watch = make(chan struct{})
		}
		return node4.self()
	case nodeKind4:
		node4 := n.node4()
		node16 := &node16[T]{header: *n}
		node16.setKind(nodeKind16)
		copy(node16.children[:], node4.children[:node4.size()])
		if watch {
			node16.watch = make(chan struct{})
		}
		return node16.self()
	case nodeKind16:
		node16 := n.node16()
		node48 := &node48[T]{header: *n}
		node48.setKind(nodeKind48)
		copy(node48.children[:], node16.children[:node16.size()])
		if watch {
			node48.watch = make(chan struct{})
		}
		return node48.self()
	case nodeKind48:
		node48 := n.node48()
		node256 := &node256[T]{header: *n}
		node256.setKind(nodeKind256)

		// Since Node256 has children indexed directly, iterate over the children
		// to assign them to the right index.
		for _, child := range node48.children[:node48.size()] {
			node256.children[child.prefix[0]] = child
		}
		if watch {
			node256.watch = make(chan struct{})
		}
		return node256.self()
	case nodeKind256:
		panic("BUG: should not need to promote node256")
	default:
		panic("unknown node kind")
	}
}

func (n *header[T]) printTree(level int) {
	fmt.Print(strings.Repeat(" ", level))

	var children []*header[T]
	switch n.kind() {
	case nodeKindLeaf:
		fmt.Printf("leaf[%v]:", n.prefix)
	case nodeKind4:
		fmt.Printf("node4[%v]:", n.prefix)
		children = n.node4().children[:n.size()]
	case nodeKind16:
		fmt.Printf("node16[%v]:", n.prefix)
		children = n.node16().children[:n.size()]
	case nodeKind48:
		fmt.Printf("node48[%v]:", n.prefix)
		children = n.node48().children[:n.size()]
	case nodeKind256:
		fmt.Printf("node256[%v]:", n.prefix)
		children = n.node256().children[:]
	default:
		panic("unknown node kind")
	}
	if leaf := n.getLeaf(); leaf != nil {
		fmt.Printf(" %v -> %v", leaf.key, leaf.value)
	}
	fmt.Printf("(%p)\n", n)

	for _, child := range children {
		if child != nil {
			child.printTree(level + 1)
		}
	}
}

func (n *header[T]) children() []*header[T] {
	switch n.kind() {
	case nodeKindLeaf:
		return nil
	case nodeKind4:
		return n.node4().children[0:n.size():4]
	case nodeKind16:
		return n.node16().children[0:n.size():16]
	case nodeKind48:
		return n.node48().children[0:n.size():48]
	case nodeKind256:
		return n.node256().children[:]
	default:
		panic("unexpected node kind")
	}
}

func (n *header[T]) findIndex(key byte) (*header[T], int) {
	if n.kind() == nodeKind256 {
		return n.node256().children[key], int(key)
	}

	children := n.children()
	idx := sort.Search(len(children), func(i int) bool {
		return children[i].prefix[0] >= key
	})
	if idx >= n.size() || children[idx].prefix[0] != key {
		// No node found, return nil and the index into
		// which it should go.
		return nil, idx
	}
	return children[idx], idx
}

func (n *header[T]) find(key byte) *header[T] {
	child, _ := n.findIndex(key)
	return child
}

func (n *header[T]) insert(idx int, child *header[T]) {
	n.setSize(n.size() + 1)
	switch n.kind() {
	case nodeKind256:
		n.node256().children[child.prefix[0]] = child
	default:
		children := n.children()
		// Shift to make room
		copy(children[idx+1:], children[idx:])
		children[idx] = child
	}
}

func (n *header[T]) remove(idx int) {
	if n.kind() == nodeKind256 {
		n.node256().children[idx] = nil
	} else {
		children := n.children()
		copy(children[idx:], children[idx+1:])
		children[n.size()-1] = nil
	}
	n.setSize(n.size() - 1)
}

type leaf[T any] struct {
	header[T]
	key   []byte
	value T
}

func newLeaf[T any](o *options, prefix, key []byte, value T) *leaf[T] {
	leaf := &leaf[T]{key: key, value: value}
	leaf.prefix = prefix
	leaf.setKind(nodeKindLeaf)

	if !o.rootOnlyWatch {
		leaf.watch = make(chan struct{})
	}

	return leaf
}

type node4[T any] struct {
	header[T]
	children [4]*header[T]
}

type node16[T any] struct {
	header[T]
	children [16]*header[T]
}

type node48[T any] struct {
	header[T]
	children [48]*header[T]
}

type node256[T any] struct {
	header[T]
	children [256]*header[T]
}

func newNode4[T any]() *header[T] {
	n := &node4[T]{header: header[T]{watch: make(chan struct{})}}
	n.setKind(nodeKind4)
	return n.self()
}

func search[T any](root *header[T], key []byte) (value T, watch <-chan struct{}, ok bool) {
	this := root
	for {
		// Consume the prefix
		if !bytes.HasPrefix(key, this.prefix) {
			return
		}
		key = key[len(this.prefix):]

		if len(key) == 0 {
			if leaf := this.getLeaf(); leaf != nil {
				value = leaf.value
				watch = this.watch
				ok = true
			}
			return
		}

		if this.kind() == nodeKind256 {
			n256 := this.node256()
			this = n256.children[key[0]]
			if this == nil {
				return
			}
		} else {
			children := this.children()
			idx := sort.Search(len(children), func(i int) bool {
				return children[i].prefix[0] >= key[0]
			})
			if idx < len(children) && children[idx].prefix[0] == key[0] {
				this = children[idx]
			} else {
				return
			}
		}
	}
}

func commonPrefix(a, b []byte) []byte {
	n := min(len(a), len(b))
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return a[:i]
		}
	}
	return a[:n]
}
