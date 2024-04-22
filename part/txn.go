// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package part

import (
	"bytes"
)

// Txn is a transaction against a tree. It allows doing efficient
// modifications to a tree by caching and reusing cloned nodes.
type Txn[T any] struct {
	// tree is the tree being modified
	Tree[T]

	// mutated is the set of nodes mutated in this transaction
	// that we can keep mutating without cloning them again.
	// It is cleared if the transaction is cloned or iterated
	// upon.
	mutated nodeMutated[T]

	// watches contains the channels of cloned nodes that should be closed
	// when transaction is committed.
	watches map[chan struct{}]struct{}

	// deleteParentsCache keeps the last allocated slice to avoid
	// reallocating it on every deletion.
	deleteParentsCache []deleteParent[T]
}

// Len returns the number of objects in the tree.
func (txn *Txn[T]) Len() int {
	return txn.size
}

// Clone returns a clone of the transaction. The clone is unaffected
// by any future changes done with the original transaction.
func (txn *Txn[T]) Clone() *Txn[T] {
	// Clear the mutated nodes so that the returned clone won't be changed by
	// further modifications in this transaction.
	txn.mutated.clear()
	return &Txn[T]{
		Tree:               txn.Tree,
		watches:            map[chan struct{}]struct{}{},
		deleteParentsCache: nil,
	}
}

// Insert or update the tree with the given key and value.
// Returns the old value if it exists.
func (txn *Txn[T]) Insert(key []byte, value T) (old T, hadOld bool) {
	old, hadOld, txn.root = txn.insert(txn.root, key, value)
	if !hadOld {
		txn.size++
	}
	return
}

// Delete the given key from the tree.
// Returns the old value if it exists.
func (txn *Txn[T]) Delete(key []byte) (old T, hadOld bool) {
	old, hadOld, txn.root = txn.delete(txn.root, key)
	if hadOld {
		txn.size--
	}
	return
}

// RootWatch returns a watch channel for the root of the tree.
// Since this is the channel associated with the root, this closes
// when there are any changes to the tree.
func (txn *Txn[T]) RootWatch() <-chan struct{} {
	return txn.root.watch
}

// Get fetches the value associated with the given key.
// Returns the value, a watch channel (which is closed on
// modification to the key) and boolean which is true if
// value was found.
func (txn *Txn[T]) Get(key []byte) (T, <-chan struct{}, bool) {
	value, watch, ok := search(txn.root, key)
	if txn.opts.rootOnlyWatch {
		watch = txn.root.watch
	}
	return value, watch, ok
}

// Prefix returns an iterator for all objects that starts with the
// given prefix, and a channel that closes when any objects matching
// the given prefix are upserted or deleted.
func (txn *Txn[T]) Prefix(key []byte) (*Iterator[T], <-chan struct{}) {
	txn.mutated.clear()
	iter, watch := prefixSearch(txn.root, key)
	if txn.opts.rootOnlyWatch {
		watch = txn.root.watch
	}
	return iter, watch
}

// LowerBound returns an iterator for all objects that have a
// key equal or higher than the given 'key'.
func (txn *Txn[T]) LowerBound(key []byte) *Iterator[T] {
	txn.mutated.clear()
	return lowerbound(txn.root, key)
}

// Iterator returns an iterator for all objects.
func (txn *Txn[T]) Iterator() *Iterator[T] {
	txn.mutated.clear()
	return newIterator[T](txn.root)
}

// Commit the transaction and produce the new tree.
func (txn *Txn[T]) Commit() *Tree[T] {
	txn.mutated.clear()
	for ch := range txn.watches {
		close(ch)
	}
	txn.watches = nil
	return &Tree[T]{txn.opts, txn.root, txn.size}
}

// CommitOnly the transaction, but do not close the
// watch channels. Returns the new tree.
// To close the watch channels call Notify().
func (txn *Txn[T]) CommitOnly() *Tree[T] {
	txn.mutated.clear()
	return &Tree[T]{txn.opts, txn.root, txn.size}
}

// Notify closes the watch channels of nodes that were
// mutated as part of this transaction.
func (txn *Txn[T]) Notify() {
	for ch := range txn.watches {
		close(ch)
	}
	txn.watches = nil
}

// PrintTree to the standard output. For debugging.
func (txn *Txn[T]) PrintTree() {
	txn.root.printTree(0)
}

func (txn *Txn[T]) cloneNode(n *header[T]) *header[T] {
	if txn.mutated.exists(n) {
		return n
	}
	if n.watch != nil {
		txn.watches[n.watch] = struct{}{}
	}
	n = n.clone(!txn.opts.rootOnlyWatch || n == txn.root)
	txn.mutated.put(n)
	return n
}

func (txn *Txn[T]) insert(root *header[T], key []byte, value T) (oldValue T, hadOld bool, newRoot *header[T]) {
	fullKey := key

	if root.size() == 0 && root.getLeaf() == nil {
		txn.watches[root.watch] = struct{}{}
		newRoot = newLeaf(txn.opts, key, key, value).self()
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
				if this.isLeaf() {
					// This is a leaf node and we just cloned it. Update the value.
					leaf := this.getLeaf()
					oldValue = leaf.value
					leaf.value = value
					hadOld = true
				} else {
					// This is a non-leaf node, create/replace the existing leaf.
					this.setLeaf(newLeaf(txn.opts, key, fullKey, value))
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
				this.insert(idx, newLeaf(txn.opts, key, fullKey, value).self())
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

			newLeaf := newLeaf(txn.opts, key, fullKey, value).self()
			newNode := &node4[T]{
				header: header[T]{prefix: newPrefix},
			}
			if this.prefix[0] < key[0] {
				newNode.children[0] = this
				newNode.keys[0] = this.prefix[0]
				newNode.children[1] = newLeaf
				newNode.keys[1] = newLeaf.prefix[0]
			} else {
				newNode.children[0] = newLeaf
				newNode.keys[0] = newLeaf.prefix[0]
				newNode.children[1] = this
				newNode.keys[1] = this.prefix[0]
			}
			newNode.setKind(nodeKind4)
			newNode.setSize(2)
			if !txn.opts.rootOnlyWatch || this == newRoot {
				newNode.header.watch = make(chan struct{})
			}
			*thisp = newNode.self()
			return
		}
	}
}

// deleteParent tracks a node on the path to the target node that is being
// deleted.
type deleteParent[T any] struct {
	node  *header[T]
	index int // the index of this node at its parent
}

func (txn *Txn[T]) delete(root *header[T], key []byte) (oldValue T, hadOld bool, newRoot *header[T]) {
	// Reuse the same slice in the transaction to hold the parents in order to avoid
	// allocations. Pre-allocate 32 levels to cover most of the use-cases without
	// reallocation.
	if txn.deleteParentsCache == nil {
		txn.deleteParentsCache = make([]deleteParent[T], 0, 32)
	}
	parents := txn.deleteParentsCache[:1] // Placeholder for root

	newRoot = root
	this := root

	// Find the target node and record the path to it.
	var leaf *leaf[T]
	for {
		if bytes.HasPrefix(key, this.prefix) {
			key = key[len(this.prefix):]
			if len(key) == 0 {
				leaf = this.getLeaf()
				if leaf == nil {
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

	oldValue = leaf.value
	hadOld = true

	// Mark the watch channel of the target node for closing.
	if this.watch != nil {
		txn.watches[this.watch] = struct{}{}
	}

	if this == root {
		// Target is the root, clear it.
		if root.isLeaf() || newRoot.size() == 0 {
			// Replace leaf or empty root with a node4
			newRoot = newNode4[T]()
		} else {
			newRoot = txn.cloneNode(root)
			newRoot.setLeaf(nil)
		}
		return
	}

	// The target was found, rebuild the tree from the root upwards.
	parents[0].node = root

	for i := len(parents) - 1; i > 0; i-- {
		parent := &parents[i-1]
		target := &parents[i]

		// Clone the parent to mutate it.
		parent.node = txn.cloneNode(parent.node)
		children := parent.node.children()

		if target.node == this && target.node.size() > 0 {
			// This is the node that we want to delete, but it has
			// children. Clone and clear the leaf.
			target.node = txn.cloneNode(target.node)
			target.node.setLeaf(nil)
			children[target.index] = target.node
		} else if target.node.size() == 0 && (target.node == this || target.node.getLeaf() == nil) {
			// The node is empty, remove it from the parent.
			parent.node.remove(target.index)
		} else {
			// Update the target (as it may have been cloned)
			children[target.index] = target.node
		}

		if parent.node.size() > 0 {
			// Check if the node should be demoted.
			// To avoid thrashing we don't demote at the boundary, but at a slightly
			// smaller size.
			// TODO: Can we avoid the initial clone of parent.node?
			var newNode *header[T]
			switch {
			case parent.node.kind() == nodeKind256 && parent.node.size() <= 37:
				newNode = (&node48[T]{header: *parent.node}).self()
				newNode.setKind(nodeKind48)
				n48 := newNode.node48()
				children := n48.children[:0]
				for k, n := range parent.node.node256().children[:] {
					if n != nil {
						n48.index[k] = int8(len(children))
						children = append(children, n)
					}
				}
			case parent.node.kind() == nodeKind48 && parent.node.size() <= 12:
				newNode = (&node16[T]{header: *parent.node}).self()
				newNode.setKind(nodeKind16)
				copy(newNode.children()[:], parent.node.children())
				n16 := newNode.node16()
				size := n16.size()
				for i := 0; i < size; i++ {
					n16.keys[i] = n16.children[i].prefix[0]
				}
			case parent.node.kind() == nodeKind16 && parent.node.size() <= 3:
				newNode = (&node4[T]{header: *parent.node}).self()
				newNode.setKind(nodeKind4)
				n16 := parent.node.node16()
				size := n16.size()
				n4 := newNode.node4()
				copy(n4.children[:], n16.children[:size])
				copy(n4.keys[:], n16.keys[:size])
			}
			if newNode != nil {
				parent.node = newNode
			}
		}
	}
	newRoot = parents[0].node
	return
}
