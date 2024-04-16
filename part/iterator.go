// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package part

import (
	"bytes"
	"sort"
)

// Iterator for key and value pairs where value is of type T
type Iterator[T any] struct {
	next [][]*header[T] // sets of edges to explore
}

// Next returns the next key, value and true if the value exists,
// otherwise it returns false.
func (it *Iterator[T]) Next() (key []byte, value T, ok bool) {
	for len(it.next) > 0 {
		// Pop the next set of edges to explore
		edges := it.next[len(it.next)-1]
		for len(edges) > 0 && edges[0] == nil {
			// Node256 may have nil children, so jump over them.
			edges = edges[1:]
		}
		it.next = it.next[:len(it.next)-1]

		if len(edges) == 0 {
			continue
		} else if len(edges) > 1 {
			// More edges remain to be explored, add them back.
			it.next = append(it.next, edges[1:])
		}

		// Follow the smallest edge and add its children to the queue.
		node := edges[0]

		if node.size() > 0 {
			it.next = append(it.next, node.children())
		}
		if leaf := node.getLeaf(); leaf != nil {
			key = leaf.key
			value = leaf.value
			ok = true
			return
		}
	}
	return
}

func newIterator[T any](start *header[T]) *Iterator[T] {
	if start == nil {
		return &Iterator[T]{nil}
	}
	return &Iterator[T]{[][]*header[T]{{start}}}
}

func prefixSearch[T any](root *header[T], key []byte) (*Iterator[T], <-chan struct{}) {
	this := root

	for {
		switch {
		case bytes.Equal(key, this.prefix[:min(len(key), len(this.prefix))]):
			return newIterator(this), this.watch

		case bytes.HasPrefix(key, this.prefix):
			key = key[len(this.prefix):]
			if len(key) == 0 {
				return newIterator(this), this.watch
			}

		default:
			return newIterator[T](nil), root.watch
		}

		this = this.find(key[0])
		if this == nil {
			return newIterator[T](nil), root.watch
		}
	}
}

func lowerbound[T any](start *header[T], key []byte) *Iterator[T] {
	// The starting edges to explore. This contains all larger nodes encountered
	// on the path to the node larger or equal to the key.
	edges := [][]*header[T]{}

	var traverseToMin func(n *header[T])
	traverseToMin = func(n *header[T]) {
		if leaf := n.getLeaf(); leaf != nil {
			edges = append(edges, []*header[T]{n})
			return
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
			traverseToMin(children[0])
		}
	}

	this := start
loop:
	for {
		switch bytes.Compare(this.prefix, key[:min(len(key), len(this.prefix))]) {
		case -1:
			// Prefix is smaller, which means there is no node smaller than
			// the given lowerbound.
			return &Iterator[T]{nil}

		case 0:
			if len(this.prefix) == len(key) {
				// Exact match.
				edges = append(edges, []*header[T]{this})
				break loop
			} else if len(key) == 0 {
				// Search key exhausted, find the minimum node.
				traverseToMin(this)
				break loop
			}

			// Prefix matches, keep going.
			key = key[len(this.prefix):]

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
					return children[i].prefix[0] >= key[0]
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
			traverseToMin(this)
			break loop
		}
	}

	if len(edges) > 0 {
		return &Iterator[T]{edges}
	}
	return &Iterator[T]{nil}
}
