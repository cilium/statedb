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
	nodeKind4
	nodeKind16
	nodeKind48
	nodeKind256
)

type leaf[T any] struct {
	key   []byte
	value T
}

type node[T any] struct {
	flags  uint16 // kind(4b) | unused(3b) | size(9b)
	prefix []byte
	leaf   *leaf[T]
	watch  chan struct{}
}

const kindMask = uint16(0b1111_000_00000000_0)

func (n *node[T]) kind() nodeKind {
	return nodeKind(n.flags >> 12)
}

func (n *node[T]) setKind(k nodeKind) {
	n.flags = (n.flags & ^kindMask) | (uint16(k&0b1111) << 12)
}

const sizeMask = uint16(0b0000_000_1111_1111_1)

func (n *node[T]) cap() int {
	switch n.kind() {
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

func (n *node[T]) size() int {
	return int(n.flags & sizeMask)
}

func (n *node[T]) setSize(size int) {
	n.flags = (n.flags & ^sizeMask) | uint16(size)&sizeMask
}

func (n *node[T]) self() *node[T] {
	return n
}

func (n *node[T]) node4() *node4[T] {
	return (*node4[T])(unsafe.Pointer(n))
}

func (n *node[T]) node16() *node16[T] {
	return (*node16[T])(unsafe.Pointer(n))
}

func (n *node[T]) node48() *node48[T] {
	return (*node48[T])(unsafe.Pointer(n))
}

func (n *node[T]) node256() *node256[T] {
	return (*node256[T])(unsafe.Pointer(n))
}

// clone returns a shallow clone of the node.
// We are working on the assumption here that only
// value-types are mutated in the returned clone.
func (n *node[T]) clone(watch bool) *node[T] {
	var nCopy *node[T]
	switch n.kind() {
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
		nCopy56 := *n.node256()
		nCopy = (&nCopy56).self()
	default:
		panic("unknown node kind")
	}
	if watch {
		nCopy.watch = make(chan struct{})
	} else {
		nCopy.watch = nil
	}
	if n.leaf != nil {
		nCopy.leaf = &leaf[T]{
			key:   n.leaf.key,
			value: n.leaf.value,
		}
	}
	return nCopy
}

func (n *node[T]) promote(watch bool) *node[T] {
	switch n.kind() {
	case nodeKind4:
		node4 := n.node4()
		node16 := &node16[T]{node: *n}
		node16.setKind(nodeKind16)
		copy(node16.children[:], node4.children[:node4.size()])
		if watch {
			node16.watch = make(chan struct{})
		}
		return node16.self()
	case nodeKind16:
		node16 := n.node16()
		node48 := &node48[T]{node: *n}
		node48.setKind(nodeKind48)
		copy(node48.children[:], node16.children[:node16.size()])
		if watch {
			node48.watch = make(chan struct{})
		}
		return node48.self()
	case nodeKind48:
		node48 := n.node48()
		node256 := &node256[T]{node: *n}
		node256.setKind(nodeKind256)
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

func (n *node[T]) printTree(level int) {
	fmt.Print(strings.Repeat(" ", level))

	var children []*node[T]
	switch n.kind() {
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
	if n.leaf != nil {
		fmt.Printf(" %v -> %v", n.leaf.key, n.leaf.value)
	}
	fmt.Printf("(%p)\n", n)

	for _, child := range children {
		if child != nil {
			child.printTree(level + 1)
		}
	}
}

func (n *node[T]) children() []*node[T] {
	switch n.kind() {
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

func (n *node[T]) findIndex(key byte) (*node[T], int) {
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

func (n *node[T]) find(key byte) *node[T] {
	child, _ := n.findIndex(key)
	return child
}

func (n *node[T]) insert(idx int, child *node[T]) {
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

type node0[T any] struct {
	node[T]
}

type node4[T any] struct {
	node[T]
	children [4]*node[T]
}

type node16[T any] struct {
	node[T]
	children [16]*node[T]
}

type node48[T any] struct {
	node[T]
	children [48]*node[T]
}

type node256[T any] struct {
	node[T]
	children [256]*node[T]
}

func newRoot[T any]() *node[T] {
	n := &node4[T]{node: node[T]{watch: make(chan struct{})}}
	n.setKind(nodeKind4)
	return n.self()
}

func search[T any](root *node[T], key []byte) (value T, watch <-chan struct{}, ok bool) {
	this := root
	for {
		// Consume the prefix
		if !bytes.HasPrefix(key, this.prefix) {
			return
		}
		key = key[len(this.prefix):]

		if len(key) == 0 {
			if this.leaf != nil {
				value = this.leaf.value
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
