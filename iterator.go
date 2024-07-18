// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package statedb

import (
	"bytes"
	"fmt"
	"iter"

	"github.com/cilium/statedb/index"
	"github.com/cilium/statedb/part"
)

// Collect creates a slice of objects out of the iterator.
// The iterator is consumed in the process.
func Collect[Obj any](seq iter.Seq2[Obj, Revision]) []Obj {
	objs := []Obj{}
	for obj := range seq {
		objs = append(objs, obj)
	}
	return objs
}

// Map a function over a sequence of objects returned by
// a query.
func Map[In, Out any](seq iter.Seq2[In, Revision], fn func(In) Out) iter.Seq2[Out, Revision] {
	return func(yield func(Out, Revision) bool) {
		for obj, rev := range seq {
			yield(fn(obj), rev)
		}
	}
}

func Filter[Obj any](seq iter.Seq2[Obj, Revision], keep func(Obj) bool) iter.Seq2[Obj, Revision] {
	return func(yield func(Obj, Revision) bool) {
		for obj, rev := range seq {
			if keep(obj) {
				yield(obj, rev)
			}
		}
	}

}

type iobjIter interface{ Next() ([]byte, object, bool) }

// allSeq returns a sequence of all objects returned by the underlying
// iterator.
func allSeq[Obj any](itxn indexReadTxn) iter.Seq2[Obj, Revision] {
	return func(yield func(Obj, Revision) bool) {
		iter := itxn.Iterator()
		for {
			_, iobj, ok := iter.Next()
			if !ok {
				break
			}
			if !yield(iobj.data.(Obj), iobj.revision) {
				break
			}
		}
	}
}

func lowerboundSeq[Obj any](itxn indexReadTxn, key []byte) iter.Seq2[Obj, Revision] {
	return func(yield func(Obj, Revision) bool) {
		iter := itxn.LowerBound(key)
		for {
			_, iobj, ok := iter.Next()
			if !ok {
				break
			}
			if !yield(iobj.data.(Obj), iobj.revision) {
				break
			}
		}
	}
}

// prefixSeq returns a sequence of all objects with given prefix.
func prefixSeq[Obj any](iter *part.Iterator[object], prefix []byte) iter.Seq2[Obj, Revision] {
	return func(yield func(Obj, Revision) bool) {
		// Iterate over a clone of the original iterator to allow the sequence to be iterated
		// from scratch multiple times.
		it := iter.Clone()
		for {
			_, iobj, ok := it.Next()
			if !ok {
				break
			}
			if !yield(iobj.data.(Obj), iobj.revision) {
				break
			}
		}
	}
}

// uniqueSeq returns a sequence of objects that match the search key.
// Yields zero or one objects.
// Used for iterating over objects in a "unique" index.
func uniqueSeq[Obj any](iter iobjIter, searchKey []byte) iter.Seq2[Obj, Revision] {
	return func(yield func(Obj, Revision) bool) {
		key, iobj, ok := iter.Next()
		if ok && bytes.Equal(key, searchKey) {
			yield(iobj.data.(Obj), iobj.revision)
		}
	}
}

func nonUniqueSeq[Obj any](iter *part.Iterator[object], searchKey []byte) iter.Seq2[Obj, Revision] {
	return func(yield func(Obj, Revision) bool) {
		// Clone the iterator to allow multiple iterations over the sequence.
		it := iter.Clone()
		for {
			key, iobj, ok := it.Next()
			if !ok {
				break
			}

			_, secondary := decodeNonUniqueKey(key)

			// The secondary key doesn't match the search key. Since the primary
			// key length can vary, we need to continue the prefix search.
			if len(secondary) != len(searchKey) {
				continue
			}

			if !yield(iobj.data.(Obj), iobj.revision) {
				break
			}
		}
	}
}

// iterator adapts the "any" object iterator to a typed object.
type iterator[Obj any] struct {
	iter interface{ Next() ([]byte, object, bool) }
}

func (it *iterator[Obj]) Next() (obj Obj, revision uint64, ok bool) {
	_, iobj, ok := it.iter.Next()
	if ok {
		obj = iobj.data.(Obj)
		revision = iobj.revision
	}
	return
}

func NewDualIterator[Obj any](left, right Iterator[Obj]) *DualIterator[Obj] {
	return &DualIterator[Obj]{
		left:  iterState[Obj]{iter: left},
		right: iterState[Obj]{iter: right},
	}
}

type iterState[Obj any] struct {
	iter Iterator[Obj]
	obj  Obj
	rev  Revision
	ok   bool
}

// DualIterator allows iterating over two iterators in revision order.
// Meant to be used for combined iteration of LowerBound(ByRevision)
// and Deleted().
type DualIterator[Obj any] struct {
	left  iterState[Obj]
	right iterState[Obj]
}

func (it *DualIterator[Obj]) Next() (obj Obj, revision uint64, fromLeft, ok bool) {
	// Advance the iterators
	if !it.left.ok && it.left.iter != nil {
		it.left.obj, it.left.rev, it.left.ok = it.left.iter.Next()
		if !it.left.ok {
			it.left.iter = nil
		}
	}
	if !it.right.ok && it.right.iter != nil {
		it.right.obj, it.right.rev, it.right.ok = it.right.iter.Next()
		if !it.right.ok {
			it.right.iter = nil
		}
	}

	// Find the lowest revision object
	switch {
	case !it.left.ok && !it.right.ok:
		ok = false
		return
	case it.left.ok && !it.right.ok:
		it.left.ok = false
		return it.left.obj, it.left.rev, true, true
	case it.right.ok && !it.left.ok:
		it.right.ok = false
		return it.right.obj, it.right.rev, false, true
	case it.left.rev <= it.right.rev:
		it.left.ok = false
		return it.left.obj, it.left.rev, true, true
	case it.right.rev <= it.left.rev:
		it.right.ok = false
		return it.right.obj, it.right.rev, false, true
	default:
		panic(fmt.Sprintf("BUG: Unhandled case: %+v", it))
	}
}

type changeIterator[Obj any] struct {
	table    Table[Obj]
	revision Revision
	dt       *deleteTracker[Obj]
	iter     *DualIterator[Obj]
	watch    <-chan struct{}
}

func (it *changeIterator[Obj]) refresh(txn ReadTxn, updateRevision, deleteRevision Revision) {
	indexTxn := txn.getTxn().mustIndexReadTxn(it.table, RevisionIndexPos)
	updateIter := &iterator[Obj]{indexTxn.LowerBound(index.Uint64(updateRevision))}
	deleteIter := it.dt.deleted(txn, deleteRevision)
	it.iter = NewDualIterator(deleteIter, updateIter)

	// It is enough to watch the revision index and not the graveyard since
	// any object that is inserted into the graveyard will be deleted from
	// the revision index.
	it.watch = indexTxn.RootWatch()
}

func (it *changeIterator[Obj]) Changes() iter.Seq2[Change[Obj], Revision] {
	return func(yield func(Change[Obj], Revision) bool) {
		if it.iter == nil {
			return
		}
		for obj, rev, deleted, ok := it.iter.Next(); ok; obj, rev, deleted, ok = it.iter.Next() {
			it.revision = rev
			if deleted {
				it.dt.mark(rev)
			}
			change := Change[Obj]{
				Object:   obj,
				Revision: rev,
				Deleted:  deleted,
			}
			if !yield(change, rev) {
				return
			}
		}
		it.iter = nil
	}
}

func (it *changeIterator[Obj]) Watch(txn ReadTxn) <-chan struct{} {
	if it.iter == nil {
		// Iterator has been exhausted, check if we need to requery
		// or whether we need to wait for changes first.
		select {
		case <-it.watch:
		default:
			// Watch channel not closed yet, so return the same watch
			// channel.
			return it.watch
		}

		newRev := it.revision + 1
		it.refresh(txn, newRev, newRev)

		// Return a closed watch channel to immediately trigger iteration.
		return closedWatchChannel
	}
	return it.watch
}

func (it *changeIterator[Obj]) Close() {
	if it.dt != nil {
		it.dt.close()
	}
	*it = changeIterator[Obj]{}
}
