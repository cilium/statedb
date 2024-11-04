// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package statedb

import (
	"bytes"
	"iter"
	"slices"

	"github.com/cilium/statedb/index"
	"github.com/cilium/statedb/part"
)

// Collect creates a slice of objects out of the iterator.
// The iterator is consumed in the process.
func Collect[Obj any](seq iter.Seq2[Obj, Revision]) []Obj {
	return slices.Collect(ToSeq(seq))
}

// Map a function over a sequence of objects returned by
// a query.
func Map[In, Out any](seq iter.Seq2[In, Revision], fn func(In) Out) iter.Seq2[Out, Revision] {
	return func(yield func(Out, Revision) bool) {
		for obj, rev := range seq {
			if !yield(fn(obj), rev) {
				break
			}
		}
	}
}

func Filter[Obj any](seq iter.Seq2[Obj, Revision], keep func(Obj) bool) iter.Seq2[Obj, Revision] {
	return func(yield func(Obj, Revision) bool) {
		for obj, rev := range seq {
			if keep(obj) {
				if !yield(obj, rev) {
					break
				}
			}
		}
	}
}

// ToSeq takes a Seq2 and produces a Seq with the first element of the pair.
func ToSeq[A, B any](seq iter.Seq2[A, B]) iter.Seq[A] {
	return func(yield func(A) bool) {
		for x, _ := range seq {
			if !yield(x) {
				break
			}
		}
	}
}

// partSeq returns a casted sequence of objects from a part Iterator.
func partSeq[Obj any](iter *part.Iterator[object]) iter.Seq2[Obj, Revision] {
	return func(yield func(Obj, Revision) bool) {
		// Iterate over a clone of the original iterator to allow the sequence to be iterated
		// from scratch multiple times.
		it := iter.Clone()
		for {
			_, iobj, ok := it.Next()
			if !ok {
				break
			}

			if iobj.deleted {
				continue
			}

			if !yield(iobj.data.(Obj), iobj.revision) {
				break
			}
		}
	}
}

// nonUniqueSeq returns a sequence of objects for a non-unique index.
// Non-unique indexes work by concatenating the secondary key with the
// primary key and then prefix searching for the items:
//
//	<secondary>\0<primary><secondary length>
//	^^^^^^^^^^^
//
// Since the primary key can be of any length and we're prefix searching,
// we need to iterate over all objects matching the prefix and only emitting
// those which have the correct secondary key length.
// For example if we search for the key "aaaa", then we might have the following
// matches (_ is just delimiting, not part of the key):
//
//	aaaa\0bbb4
//	aaa\0abab3
//	aaaa\0ccc4
//
// We yield "aaaa\0bbb4", skip "aaa\0abab3" and yield "aaaa\0ccc4".
func nonUniqueSeq[Obj any](iter *part.Iterator[object], prefixSearch bool, searchKey []byte) iter.Seq2[Obj, Revision] {
	return func(yield func(Obj, Revision) bool) {
		// Clone the iterator to allow multiple iterations over the sequence.
		it := iter.Clone()

		var visited map[string]struct{}
		if prefixSearch {
			// When prefix searching, keep track of objects we've already seen as
			// multiple keys in non-unique index may map to a single object.
			// When just doing a List() on a non-unique index we will see each object
			// only once and do not need to track this.
			//
			// This of course makes iterating over a non-unique index with a prefix
			// (or lowerbound search) about 20x slower than normal!
			visited = map[string]struct{}{}
		}

		for {
			key, iobj, ok := it.Next()
			if !ok {
				break
			}

			secondary, primary := decodeNonUniqueKey(key)

			switch {
			case !prefixSearch && len(secondary) != len(searchKey):
				// This a List(), thus secondary key must match length exactly.
				continue
			case prefixSearch && len(secondary) < len(searchKey):
				// This is Prefix(), thus key must be equal or longer to search key.
				continue
			}

			if prefixSearch {
				// When doing a prefix search on a non-unique index we may see the
				// same object multiple times since multiple keys may point it.
				// Skip if we've already seen this object.
				if _, found := visited[string(primary)]; found {
					continue
				}
				visited[string(primary)] = struct{}{}
			}

			if iobj.deleted {
				continue
			}

			if !yield(iobj.data.(Obj), iobj.revision) {
				break
			}
		}
	}
}

func nonUniqueLowerBoundSeq[Obj any](iter *part.Iterator[object], searchKey []byte) iter.Seq2[Obj, Revision] {
	return func(yield func(Obj, Revision) bool) {
		// Clone the iterator to allow multiple uses.
		iter = iter.Clone()

		// Keep track of objects we've already seen as multiple keys in non-unique
		// index may map to a single object.
		visited := map[string]struct{}{}
		for {
			key, iobj, ok := iter.Next()
			if !ok {
				break
			}
			if iobj.deleted {
				continue
			}

			// With a non-unique index we have a composite key <secondary><primary><secondary len>.
			// This means we need to check every key that it's larger or equal to the search key.
			// Just seeking to the first one isn't enough as the secondary key length may vary.
			secondary, primary := decodeNonUniqueKey(key)
			if bytes.Compare(secondary, searchKey) >= 0 {
				if _, found := visited[string(primary)]; found {
					continue
				}

				visited[string(primary)] = struct{}{}

				if !yield(iobj.data.(Obj), iobj.revision) {
					return
				}
			}
		}
	}
}

// Iterator for iterating a sequence objects.
type Iterator[Obj any] interface {
	// Next returns the next object and its revision if ok is true, otherwise
	// zero values to mean that the iteration has finished.
	Next() (obj Obj, rev Revision, ok bool)
}

type changeIterator[Obj any] struct {
	table Table[Obj]

	// revision is the latest observed object
	revision Revision

	// deleteStartRevision is the oldest deleted revision
	// this iterator should observe. E.g. this is the revision
	// of the table at the time the change iterator was created,
	// and it's used to make sure we don't observe deletions that
	// happened in the past.
	deleteStartRevision Revision

	dt    *deleteTracker[Obj]
	iter  *part.Iterator[object]
	watch <-chan struct{}
}

func (it *changeIterator[Obj]) refresh(txn ReadTxn) {
	// Instead of indexReadTxn() we look up directly here so we don't
	// refresh from mutated indexes in case [txn] is a WriteTxn. This
	// is important as the WriteTxn may be aborted and thus revisions will
	// reset back and watermarks bumped from here would be invalid.
	itxn := txn.getTxn()
	indexEntry := itxn.root[it.table.tablePos()].indexes[RevisionIndexPos]
	indexTxn := indexReadTxn{indexEntry.tree, indexEntry.unique}
	it.iter = indexTxn.LowerBound(index.Uint64(it.revision + 1))
	it.watch = indexTxn.RootWatch()
}

func (it *changeIterator[Obj]) Next(txn ReadTxn) (seq iter.Seq2[Change[Obj], Revision], watch <-chan struct{}) {
	if it.iter == nil {
		// Iterator has been exhausted, check if we need to requery
		// or whether we need to wait for changes first.
		select {
		case <-it.watch:
			// Watch channel closed, so new changes await
		default:
			// Watch channel for the query not closed yet, so return it to allow
			// caller to wait for the new changes.
			watch = it.watch
			seq = func(yield func(Change[Obj], Revision) bool) {}
			return
		}
	}

	// Refresh the iterator regardless if it was fully consumed or not to
	// pull in new changes. We keep returning a closed channel until the
	// iterator has been fully consumed. This does mean there's an extra
	// Next() call to get a proper watch channel, but it does make this
	// API much safer to use even when only partially consuming the
	// sequence.
	it.refresh(txn)
	watch = closedWatchChannel
	seq = func(yield func(Change[Obj], Revision) bool) {
		if it.iter == nil {
			return
		}
		for _, obj, ok := it.iter.Next(); ok; _, obj, ok = it.iter.Next() {
			rev := obj.revision
			it.revision = rev
			change := Change[Obj]{
				Revision: rev,
			}
			if obj.deleted {
				if rev <= it.deleteStartRevision {
					// Ignore objects that were marked deleted before this
					// change iterator was created.
					continue
				}

				change.Object = obj.data.(Obj)
				change.Deleted = true
				it.dt.mark(rev)
			} else {
				change.Object = obj.data.(Obj)
			}
			if !yield(change, rev) {
				return
			}
		}
		it.iter = nil
	}
	return
}

// changesAny is for implementing the /changes HTTP API where the concrete object
// type is not known.
func (it *changeIterator[Obj]) nextAny(txn ReadTxn) (iter.Seq2[Change[any], Revision], <-chan struct{}) {
	seq, watch := it.Next(txn)

	return func(yield func(Change[any], Revision) bool) {
		for change, rev := range seq {
			ok := yield(Change[any]{
				Object:   change.Object,
				Revision: change.Revision,
				Deleted:  change.Deleted,
			}, rev)
			if !ok {
				break
			}
		}
	}, watch
}

func (it *changeIterator[Obj]) close() {
	if it.dt != nil {
		it.dt.close()
	}
	it.dt = nil
}

type anyChangeIterator interface {
	nextAny(ReadTxn) (iter.Seq2[Change[any], Revision], <-chan struct{})
}
