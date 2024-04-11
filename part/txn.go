// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package part

import (
	"bytes"
)

type Txn[T any] struct {
	opts *options
	root *node[T]
	size int

	// The set of nodes mutated in this transaction that we can keep
	// mutating without cloning them again.
	mutated map[*node[T]]struct{}

	watches map[chan struct{}]struct{}

	deleteParents []deleteParent[T]
}

func (txn *Txn[T]) Len() int {
	return txn.size
}

func (txn *Txn[T]) Clone() *Txn[T] {
	// Clear the mutated nodes so that the returned clone won't be changed by
	// further modifications in this transaction.
	clear(txn.mutated)
	return &Txn[T]{
		opts:          txn.opts,
		root:          txn.root,
		size:          txn.size,
		mutated:       map[*node[T]]struct{}{},
		watches:       map[chan struct{}]struct{}{},
		deleteParents: nil,
	}
}

func (txn *Txn[T]) Insert(key []byte, value T) (old T, hadOld bool) {
	old, hadOld, txn.root = txn.insert(txn.root, key, value)
	if !hadOld {
		txn.size++
	}
	return
}

func (txn *Txn[T]) Delete(key []byte) (old T, hadOld bool) {
	old, hadOld, txn.root = txn.delete(txn.root, key)
	if hadOld {
		txn.size--
	}
	return
}

func (txn *Txn[T]) RootWatch() <-chan struct{} {
	return txn.root.watch
}

func (txn *Txn[T]) Get(key []byte) (T, <-chan struct{}, bool) {
	value, watch, ok := search(txn.root, key)
	if txn.opts.rootOnlyWatch {
		watch = txn.root.watch
	}
	return value, watch, ok
}

func (txn *Txn[T]) Prefix(key []byte) (*Iterator[T], <-chan struct{}) {
	clear(txn.mutated)
	iter, watch := prefixSearch(txn.root, key)
	if txn.opts.rootOnlyWatch {
		watch = txn.root.watch
	}
	return iter, watch
}

func (txn *Txn[T]) LowerBound(key []byte) *Iterator[T] {
	clear(txn.mutated)
	return lowerbound(txn.root, key)
}

func (txn *Txn[T]) Iterator() *Iterator[T] {
	clear(txn.mutated)
	return newIterator[T](txn.root)
}

func (txn *Txn[T]) Commit() *Tree[T] {
	txn.mutated = nil
	for ch := range txn.watches {
		close(ch)
	}
	txn.watches = nil
	return &Tree[T]{txn.opts, txn.root, txn.size}
}

func (txn *Txn[T]) CommitOnly() *Tree[T] {
	txn.mutated = nil
	return &Tree[T]{txn.opts, txn.root, txn.size}
}

func (txn *Txn[T]) Notify() {
	for ch := range txn.watches {
		close(ch)
	}
	txn.watches = nil
}

func (txn *Txn[T]) PrintTree() {
	txn.root.printTree(0)
}

func (txn *Txn[T]) cloneNode(n *node[T]) *node[T] {
	if _, ok := txn.mutated[n]; ok {
		return n
	}
	if n.watch != nil {
		txn.watches[n.watch] = struct{}{}
	}
	n = n.clone(!txn.opts.rootOnlyWatch || n == txn.root)
	txn.mutated[n] = struct{}{}
	return n
}

func (txn *Txn[T]) insert(root *node[T], key []byte, value T) (oldValue T, hadOld bool, newRoot *node[T]) {
	fullKey := key
	mkLeafNode := func(prefix []byte) *node[T] {
		newLeaf := &node4[T]{}
		newLeaf.leaf = &leaf[T]{key: fullKey, value: value}
		newLeaf.setKind(nodeKind4)
		if !txn.opts.rootOnlyWatch {
			newLeaf.node.watch = make(chan struct{})
		}
		newLeaf.prefix = prefix
		return newLeaf.self()
	}

	if root.size() == 0 && root.leaf == nil {
		txn.watches[root.watch] = struct{}{}
		newRoot = mkLeafNode(key)
		if newRoot.watch == nil {
			newRoot.watch = make(chan struct{})
		}
		return
	}

	newRoot = txn.cloneNode(root)
	this := newRoot
	thisp := &newRoot
	for {
		if bytes.HasPrefix(key, this.prefix) {
			key = key[len(this.prefix):]
			if len(key) == 0 {
				if this.leaf != nil {
					oldValue = this.leaf.value
					hadOld = true

					this.leaf.key = fullKey
					this.leaf.value = value
				} else {
					this.leaf = &leaf[T]{key: fullKey, value: value}
				}
				return
			}

			child, idx := this.findIndex(key[0])

			if child == nil || child.prefix[0] != key[0] {
				// No node for this key, add it.
				if this.size()+1 > this.cap() {
					// too small, need to promote to a larger size.
					if !txn.opts.rootOnlyWatch || this == newRoot {
						txn.watches[this.watch] = struct{}{}
					}
					this = this.promote(!txn.opts.rootOnlyWatch || this == newRoot)
				} else {
					// it fits, just need a clone.
					this = txn.cloneNode(this)
				}
				this.insert(idx, mkLeafNode(key).self())
				*thisp = this
				return
			}

			// Node exists, replace it with a clone and recurse into it.
			child = txn.cloneNode(child)
			if this.kind() == nodeKind256 {
				thisp = &this.node256().children[idx]
			} else {
				thisp = &this.children()[idx]
			}
			*thisp = child
			this = child
		} else {
			// Reached a node with a different prefix, split it.
			newPrefix := commonPrefix(key, this.prefix)

			this.prefix = this.prefix[len(newPrefix):]
			key = key[len(newPrefix):]

			newLeaf := mkLeafNode(key)
			newNode := &node4[T]{
				node: node[T]{prefix: newPrefix},
			}
			if this.prefix[0] < key[0] {
				newNode.children[0] = this
				newNode.children[1] = newLeaf
			} else {
				newNode.children[0] = newLeaf
				newNode.children[1] = this
			}
			newNode.setKind(nodeKind4)
			newNode.setSize(2)
			if !txn.opts.rootOnlyWatch || this == newRoot {
				newNode.node.watch = make(chan struct{})
			}
			*thisp = newNode.self()
			return
		}
	}
}

type deleteParent[T any] struct {
	node  *node[T]
	index int
}

func (txn *Txn[T]) delete(root *node[T], key []byte) (oldValue T, hadOld bool, newRoot *node[T]) {
	newRoot = root
	this := root

	// Reuse the same slice in the transaction to hold the parents in order to avoid
	// allocations.
	if txn.deleteParents == nil {
		txn.deleteParents = make([]deleteParent[T], 0, 32)
	}
	parents := txn.deleteParents[:0]
	parents = append(parents, deleteParent[T]{nil, -1}) // Placeholder for new root.

	// Find the target node and record the path to it.
	for {
		if bytes.HasPrefix(key, this.prefix) {
			key = key[len(this.prefix):]
			if len(key) == 0 {
				if this.leaf == nil {
					// Not found.
					return
				}
				// Target node found!
				break
			}
			var idx int
			this, idx = this.findIndex(key[0])
			if this == nil {
				return
			}
			parents = append(parents, deleteParent[T]{this, idx})
		} else {
			// Reached a node with a different prefix, so node not found.
			return
		}
	}

	oldValue = this.leaf.value
	hadOld = true

	// The target was found, rebuild the tree from the root upwards.
	newRoot = txn.cloneNode(root)
	parents[0].node = newRoot

	if this == root {
		// Target is the root, clear it.
		newRoot.leaf = nil
		if newRoot.size() == 0 {
			// No children so we can clear the prefix.
			newRoot.prefix = nil
		}
		return
	}

	for i := len(parents) - 1; i > 0; i-- {
		parent := &parents[i-1]
		target := &parents[i]

		// Clone the parent to mutate it.
		parent.node = txn.cloneNode(parent.node)
		var children []*node[T]
		if parent.node.kind() == nodeKind256 {
			children = parent.node.node256().children[:]
		} else {
			children = parent.node.children()
		}

		if target.node == this && target.node.size() > 0 {
			// This is the node that we want to delete, but it has
			// children. Clone and clear the leaf.
			target.node = txn.cloneNode(target.node)
			target.node.leaf = nil
			children[target.index] = target.node
		}

		if target.node.size() == 0 && (target.node == this || target.node.leaf == nil) {
			// The node is empty, remove it from the parent.
			if parent.node.kind() == nodeKind256 {
				children[target.index] = nil
			} else {
				copy(children[target.index:], children[target.index+1:])
				children[parent.node.size()-1] = nil
			}
			parent.node.setSize(parent.node.size() - 1)
		} else {
			// Update target to point to the cloned node
			children[target.index] = target.node
		}

		if parent.node.size() > 0 {
			// Check if the node should be demoted.
			// TODO: Can we avoid the initial clone of parent.node?
			var newNode *node[T]
			switch {
			case parent.node.kind() == nodeKind256 && parent.node.size() <= 48:
				newNode = (&node48[T]{node: *parent.node}).self()
				newNode.setKind(nodeKind48)
				children := newNode.node48().children[:0]
				for _, n := range parent.node.node256().children[:] {
					if n != nil {
						children = append(children, n)
					}
				}
			case parent.node.kind() == nodeKind48 && parent.node.size() <= 16:
				newNode = (&node16[T]{node: *parent.node}).self()
				newNode.setKind(nodeKind16)
				copy(newNode.children()[:], parent.node.children())
			case parent.node.kind() == nodeKind16 && parent.node.size() <= 4:
				newNode = (&node4[T]{node: *parent.node}).self()
				newNode.setKind(nodeKind4)
				copy(newNode.children()[:], parent.node.children())
			}
			if newNode != nil {
				parent.node = newNode
			}
		}
	}

	return
}
