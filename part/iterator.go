// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package part

import (
	"bytes"
	"sort"
)

// Iterator for key and value pairs where value is of type T
type Iterator[T any] struct {
	// start is the starting point of the iteration when there's only
	// a single edge to start from.
	start *header[T]

	// edges are the edges to explore
	edges [][]*header[T]
}

// All calls yield for every value. Can be called multiple times.
//
// [All] does not modify the iterator state. If [Next] is called then
// [All] will return only the remaining values.
func (it Iterator[T]) All(yield func(key []byte, value T) bool) {
	// Use a suitably large stack allocated array to hold the edges to explore.
	// [append] will allocate from the heap if this is not large enough.
	var nextArray [32][]*header[T]
	next := nextArray[0:0:32]

	if it.start != nil {
		node := it.start
		if node.size() > 0 {
			next = append(next, node.children())
		}
		if leaf := node.getLeaf(); leaf != nil {
			if !yield(leaf.fullKey(), leaf.value) {
				return
			}
		}
	} else {
		next = append(next, it.edges...)
	}

	// NOTE: Seems like there's a 25% performance drop if we try to share
	// the code below with Next() by moving it into a function. Looks like the
	// inliner at least in v1.25.4 doesn't want to inline it. So let's just live
	// with the duplication.

	for len(next) > 0 {
		// Pop the next set of edges to explore
		edges := next[len(next)-1]
		next = next[:len(next)-1]

		// Node256 may have nil children, so jump over them.
		for len(edges) > 0 && edges[0] == nil {
			edges = edges[1:]
		}

		if len(edges) == 0 {
			continue
		} else if len(edges) > 1 {
			// More edges remain to be explored, add them back to queue.
			next = append(next, edges[1:])
		}

		node := edges[0]
		if node.size() > 0 {
			// Node has children, add them to queue.
			next = append(next, node.children())
		}
		if leaf := node.getLeaf(); leaf != nil {
			if !yield(leaf.fullKey(), leaf.value) {
				return
			}
		}
	}
}

// Next returns the next key, value and true if the value exists,
// otherwise it returns false.
//
// This modifies the iterator state and changes what [All]
// returns (e.g. the values consumed by [Next] are not returned by it).
func (it *Iterator[T]) Next() (key []byte, value T, ok bool) {
	if it == nil {
		return
	}

	if it.edges == nil {
		if it.start == nil {
			return
		}
		node := it.start
		it.start = nil
		if node.size() > 0 {
			it.edges = make([][]*header[T], 1, 32)
			it.edges[0] = node.children()
		}
		if leaf := node.getLeaf(); leaf != nil {
			return leaf.fullKey(), leaf.value, true
		}
	}

	for len(it.edges) > 0 {
		// Pop the next set of edges to explore
		edges := it.edges[len(it.edges)-1]
		it.edges = it.edges[:len(it.edges)-1]

		// Node256 may have nil children, so jump over them.
		for len(edges) > 0 && edges[0] == nil {
			edges = edges[1:]
		}

		if len(edges) == 0 {
			continue
		} else if len(edges) > 1 {
			// More edges remain to be explored, add them back to queue.
			it.edges = append(it.edges, edges[1:])
		}

		node := edges[0]
		if node.size() > 0 {
			// Node has children, add them to queue.
			it.edges = append(it.edges, node.children())
		}
		if leaf := node.getLeaf(); leaf != nil {
			key = leaf.fullKey()
			value = leaf.value
			ok = true
			return
		}
	}
	return
}

func newIterator[T any](start *header[T]) Iterator[T] {
	return Iterator[T]{start: start}
}

type reverseFrame[T any] struct {
	children []*header[T]
	leaf     *leaf[T]
}

func newReverseFrame[T any](node *header[T]) reverseFrame[T] {
	return reverseFrame[T]{children: node.children(), leaf: node.getLeaf()}
}

func (f *reverseFrame[T]) nextChild() *header[T] {
	// Find the next non-nil child (node256 may have holes)
	for i := len(f.children) - 1; i >= 0; i-- {
		if child := f.children[i]; child != nil {
			f.children = f.children[:i]
			return child
		}
	}
	f.children = nil
	return nil
}

// ReverseIterator iterates over key/value pairs in reverse order.
type ReverseIterator[T any] struct {
	start *header[T]
	stack []reverseFrame[T]
}

// All calls yield for every value in reverse order. Can be called multiple times.
func (it ReverseIterator[T]) All(yield func(key []byte, value T) bool) {
	// Try to use a stack-allocated stack of frames to avoid heap allocations.
	var stackArray [32]reverseFrame[T]
	stack := stackArray[0:0:32]

	switch {
	case it.start != nil:
		stack = append(stack, newReverseFrame(it.start))
	case len(it.stack) > 0:
		stack = append(stack, it.stack...)
	default:
		return
	}

	for len(stack) > 0 {
		frame := &stack[len(stack)-1]
		if child := frame.nextChild(); child != nil {
			stack = append(stack, newReverseFrame(child))
			continue
		}

		if leaf := frame.leaf; leaf != nil {
			frame.leaf = nil
			if !yield(leaf.fullKey(), leaf.value) {
				return
			}
			continue
		}

		stack = stack[:len(stack)-1]
	}
}

// Next returns the next key, value and true if the value exists,
// otherwise it returns false.
func (it *ReverseIterator[T]) Next() (key []byte, value T, ok bool) {
	if it == nil {
		return
	}

	if it.stack == nil {
		if it.start == nil {
			return
		}
		it.stack = make([]reverseFrame[T], 1, 32)
		it.stack[0] = newReverseFrame(it.start)
		it.start = nil
	}

	for len(it.stack) > 0 {
		frame := &it.stack[len(it.stack)-1]
		if child := frame.nextChild(); child != nil {
			it.stack = append(it.stack, newReverseFrame(child))
			continue
		}

		if leaf := frame.leaf; leaf != nil {
			frame.leaf = nil
			return leaf.fullKey(), leaf.value, true
		}

		it.stack = it.stack[:len(it.stack)-1]
	}
	return
}

func newReverseIterator[T any](start *header[T]) ReverseIterator[T] {
	return ReverseIterator[T]{start: start}
}

func prefixSearch[T any](root *header[T], rootWatch <-chan struct{}, prefix []byte) (Iterator[T], <-chan struct{}) {
	if root == nil {
		return newIterator[T](nil), rootWatch
	}

	this := root
	watch := rootWatch
	for {
		// Does the node have part of the prefix we're looking for?
		commonPrefix := this.prefix()[:min(len(prefix), int(this.prefixLen))]
		if !bytes.HasPrefix(prefix, commonPrefix) {
			// Mismatching prefix, return the watch channel from the previous matching node.
			return newIterator[T](nil), watch
		}

		if !this.isLeaf() && this.watch != nil {
			// Leaf watch channels only close when the leaf is manipulated,
			// thus we only return non-leaf watch channels.
			watch = this.watch
		}

		// Consume the prefix of this node
		prefix = prefix[len(commonPrefix):]
		if len(prefix) == 0 {
			// Exact match to our search prefix.
			return newIterator(this), watch
		}

		this = this.find(prefix[0])
		if this == nil {
			return newIterator[T](nil), watch
		}
	}
}

func prefixSearchReverse[T any](root *header[T], rootWatch <-chan struct{}, prefix []byte) (ReverseIterator[T], <-chan struct{}) {
	iter, watch := prefixSearch(root, rootWatch, prefix)
	return newReverseIterator(iter.start), watch
}

func traverseToMin[T any](n *header[T], edges [][]*header[T]) [][]*header[T] {
	if leaf := n.getLeaf(); leaf != nil {
		return append(edges, []*header[T]{n})
	}
	children := n.children()

	// Find the first non-nil child
	for len(children) > 0 && children[0] == nil {
		children = children[1:]
	}

	if len(children) > 0 {
		// Add the larger children.
		if len(children) > 1 {
			edges = append(edges, children[1:])
		}
		// Recurse into the smallest child
		return traverseToMin(children[0], edges)
	}
	return edges
}

func lowerbound[T any](start *header[T], key []byte) Iterator[T] {
	if start == nil {
		return Iterator[T]{}
	}

	// The starting edges to explore. This contains all larger nodes encountered
	// on the path to the node larger or equal to the key.
	var edges [][]*header[T]
	this := start
loop:
	for {
		switch bytes.Compare(this.prefix(), key[:min(len(key), int(this.prefixLen))]) {
		case -1:
			// Prefix is smaller, stop here and return an iterator for
			// the larger nodes in the parent's.
			break loop

		case 0:
			if int(this.prefixLen) == len(key) {
				// Exact match.
				edges = append(edges, []*header[T]{this})
				break loop
			}

			// Prefix matches the beginning of the key, but more
			// remains of the key. Drop the matching part and keep
			// going further.
			key = key[this.prefixLen:]

			if this.kind() == nodeKind256 {
				children := this.node256().children[:]
				idx := int(key[0])
				this = children[idx]

				// Add all larger children and recurse further.
				children = children[idx+1:]
				for len(children) > 0 && children[0] == nil {
					children = children[1:]
				}
				edges = append(edges, children)

				if this == nil {
					break loop
				}
			} else {
				children := this.children()

				// Find the smallest child that is equal or larger than the lower bound
				idx := sort.Search(len(children), func(i int) bool {
					return children[i].key() >= key[0]
				})
				if idx >= this.size() {
					break loop
				}
				// Add all larger children and recurse further.
				if len(children) > idx+1 {
					edges = append(edges, children[idx+1:])
				}
				this = children[idx]
			}

		case 1:
			// Prefix bigger than lowerbound, go to smallest node and stop.
			edges = traverseToMin(this, edges)
			break loop
		}
	}

	if len(edges) > 0 {
		return Iterator[T]{edges: edges}
	}
	return Iterator[T]{}
}
