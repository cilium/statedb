// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package part

type Tree[T any] struct {
	opts *options
	root *node[T]
	size int
}

func (t *Tree[T]) Txn() *Txn[T] {
	txn := &Txn[T]{
		opts:    t.opts,
		root:    t.root,
		size:    t.size,
		mutated: make(map[*node[T]]struct{}),
		watches: make(map[chan struct{}]struct{}),
	}
	return txn
}

func (t *Tree[T]) Len() int {
	return t.size
}

func (t *Tree[T]) Get(key []byte) (T, <-chan struct{}, bool) {
	value, watch, ok := search(t.root, key)
	if t.opts.rootOnlyWatch {
		watch = t.root.watch
	}
	return value, watch, ok
}

func (t *Tree[T]) Prefix(key []byte) (*Iterator[T], <-chan struct{}) {
	iter, watch := prefixSearch(t.root, key)
	if t.opts.rootOnlyWatch {
		watch = t.root.watch
	}
	return iter, watch
}

func (t *Tree[T]) RootWatch() <-chan struct{} {
	return t.root.watch
}

func (t *Tree[T]) LowerBound(key []byte) *Iterator[T] {
	return lowerbound(t.root, key)
}

func (t *Tree[T]) Insert(key []byte, value T) (old T, hadOld bool, tree *Tree[T]) {
	txn := t.Txn()
	old, hadOld = txn.Insert(key, value)
	tree = txn.Commit()
	return
}

func (t *Tree[T]) Delete(key []byte) (old T, hadOld bool, tree *Tree[T]) {
	txn := t.Txn()
	old, hadOld = txn.Delete(key)
	tree = txn.Commit()
	return
}

func (t *Tree[T]) Iterator() *Iterator[T] {
	return newIterator[T](t.root)
}

func (t *Tree[T]) PrintTree() {
	t.root.printTree(0)
}

type options struct {
	rootOnlyWatch bool
}

type Option func(*options)

func RootOnlyWatch(o *options) { o.rootOnlyWatch = true }

func New[T any](opts ...Option) *Tree[T] {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	return &Tree[T]{
		root: newRoot[T](),
		size: 0,
		opts: &o,
	}
}
