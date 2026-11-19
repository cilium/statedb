// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package statedb

import "iter"

// Pipeline is a lazy, typed sequence of rows read from a StateDB snapshot.
// Construct pipelines with From and evaluate them with All or AllWatch.
//
// Pipelines expose values without table revisions. A joined row may come from
// multiple tables, so it has no single revision that represents all of its
// inputs.
type Pipeline[Row any] struct {
	run func(ReadTxn, *WatchSet) iter.Seq[Row]
}

// From creates a pipeline over the rows matching query. The table is read
// lazily when the returned pipeline is evaluated with All or AllWatch.
func From[Row any](table Table[Row], query Query[Row]) Pipeline[Row] {
	return Pipeline[Row]{run: func(txn ReadTxn, watchSet *WatchSet) iter.Seq[Row] {
		return func(yield func(Row) bool) {
			var rows iter.Seq2[Row, Revision]
			if watchSet == nil {
				rows = table.List(txn, query)
			} else {
				var watch <-chan struct{}
				rows, watch = table.ListWatch(txn, query)
				watchSet.Add(watch)
			}
			for row := range rows {
				if !yield(row) {
					return
				}
			}
		}
	}}
}

// FromAll creates a pipeline over every row in table. Prefer From when an
// index query can select a smaller set of rows.
func FromAll[Row any](table Table[Row]) Pipeline[Row] {
	return Pipeline[Row]{run: func(txn ReadTxn, watchSet *WatchSet) iter.Seq[Row] {
		return func(yield func(Row) bool) {
			var rows iter.Seq2[Row, Revision]
			if watchSet == nil {
				rows = table.All(txn)
			} else {
				var watch <-chan struct{}
				rows, watch = table.AllWatch(txn)
				watchSet.Add(watch)
			}
			for row := range rows {
				if !yield(row) {
					return
				}
			}
		}
	}}
}

// All evaluates the pipeline against txn and returns its rows as a lazy
// sequence. Every table read in the pipeline uses the same snapshot.
func (p Pipeline[Row]) All(txn ReadTxn) iter.Seq[Row] {
	return p.evaluate(txn, nil)
}

// AllWatch evaluates the pipeline against txn and adds watches for every table
// query performed during iteration to watchSet. The returned sequence is lazy;
// consume it fully before waiting on watchSet so that all join lookups are
// watched. Use [NewWatchSet] to create watchSet.
func (p Pipeline[Row]) AllWatch(txn ReadTxn, watchSet *WatchSet) iter.Seq[Row] {
	return p.evaluate(txn, watchSet)
}

func (p Pipeline[Row]) evaluate(txn ReadTxn, watchSet *WatchSet) iter.Seq[Row] {
	if p.run == nil {
		return func(func(Row) bool) {}
	}
	return p.run(txn, watchSet)
}

// Where keeps only the rows for which predicate returns true.
func (p Pipeline[Row]) Where(predicate func(Row) bool) Pipeline[Row] {
	return Pipeline[Row]{run: func(txn ReadTxn, watchSet *WatchSet) iter.Seq[Row] {
		return func(yield func(Row) bool) {
			for row := range p.evaluate(txn, watchSet) {
				if predicate(row) && !yield(row) {
					return
				}
			}
		}
	}}
}

// Select maps each row to a value of type Out.
func (p Pipeline[Row]) Select[Out any](selector func(Row) Out) Pipeline[Out] {
	return Pipeline[Out]{run: func(txn ReadTxn, watchSet *WatchSet) iter.Seq[Out] {
		return func(yield func(Out) bool) {
			for row := range p.evaluate(txn, watchSet) {
				if !yield(selector(row)) {
					return
				}
			}
		}
	}}
}

// Map is an alias for Select.
func (p Pipeline[Row]) Map[Out any](mapper func(Row) Out) Pipeline[Out] {
	return p.Select(mapper)
}

// JoinList performs an indexed inner join using exact matches from table.List.
// lookup constructs a query against table for each row in p. Every matching
// row from table produces one output row, in left-row and then index order.
func (p Pipeline[Row]) JoinList[Right, Out any](
	table Table[Right],
	lookup func(Row) Query[Right],
	selectRow func(Row, Right) Out,
) Pipeline[Out] {
	return joinRows(p, table, lookup, selectRow, func(txn ReadTxn, query Query[Right], watch bool) (iter.Seq2[Right, Revision], <-chan struct{}) {
		if watch {
			return table.ListWatch(txn, query)
		}
		return table.List(txn, query), nil
	})
}

// JoinLowerBound performs an indexed inner join using table.LowerBound. For
// each input row, it returns rows whose keys are greater than or equal to the
// key in the query returned by lookup.
func (p Pipeline[Row]) JoinLowerBound[Right, Out any](
	table Table[Right],
	lookup func(Row) Query[Right],
	selectRow func(Row, Right) Out,
) Pipeline[Out] {
	return joinRows(p, table, lookup, selectRow, func(txn ReadTxn, query Query[Right], watch bool) (iter.Seq2[Right, Revision], <-chan struct{}) {
		if watch {
			return table.LowerBoundWatch(txn, query)
		}
		return table.LowerBound(txn, query), nil
	})
}

// JoinPrefix performs an indexed inner join using table.Prefix. For each input
// row, it returns rows whose keys share the prefix in the query returned by
// lookup.
func (p Pipeline[Row]) JoinPrefix[Right, Out any](
	table Table[Right],
	lookup func(Row) Query[Right],
	selectRow func(Row, Right) Out,
) Pipeline[Out] {
	return joinRows(p, table, lookup, selectRow, func(txn ReadTxn, query Query[Right], watch bool) (iter.Seq2[Right, Revision], <-chan struct{}) {
		if watch {
			return table.PrefixWatch(txn, query)
		}
		return table.Prefix(txn, query), nil
	})
}

func joinRows[Left, Right, Out any](
	p Pipeline[Left],
	table Table[Right],
	lookup func(Left) Query[Right],
	selectRow func(Left, Right) Out,
	queryRows func(ReadTxn, Query[Right], bool) (iter.Seq2[Right, Revision], <-chan struct{}),
) Pipeline[Out] {
	return Pipeline[Out]{run: func(txn ReadTxn, watchSet *WatchSet) iter.Seq[Out] {
		return func(yield func(Out) bool) {
			for left := range p.evaluate(txn, watchSet) {
				rows, watch := queryRows(txn, lookup(left), watchSet != nil)
				if watchSet != nil {
					watchSet.Add(watch)
				}
				for right := range rows {
					if !yield(selectRow(left, right)) {
						return
					}
				}
			}
		}
	}}
}
