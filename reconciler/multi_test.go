// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package reconciler_test

import (
	"context"
	"errors"
	"iter"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cilium/hive"
	"github.com/cilium/hive/cell"
	"github.com/cilium/hive/hivetest"
	"github.com/cilium/hive/job"
	"github.com/cilium/statedb"
	"github.com/cilium/statedb/index"
	"github.com/cilium/statedb/reconciler"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type multiStatusObject struct {
	ID       uint64
	Statuses reconciler.StatusSet
}

func (m *multiStatusObject) Clone() *multiStatusObject {
	m2 := *m
	return &m2
}

var multiStatusIndex = statedb.Index[*multiStatusObject, uint64]{
	Name: "id",
	FromObject: func(t *multiStatusObject) index.KeySet {
		return index.NewKeySet(index.Uint64(t.ID))
	},
	FromKey: index.Uint64,
	Unique:  true,
}

type multiMockOps struct {
	numUpdates int
	faulty     atomic.Bool
}

// Delete implements reconciler.Operations.
func (m *multiMockOps) Delete(context.Context, statedb.ReadTxn, statedb.Revision, *multiStatusObject) error {
	return nil
}

// Prune implements reconciler.Operations.
func (m *multiMockOps) Prune(context.Context, statedb.ReadTxn, iter.Seq2[*multiStatusObject, statedb.Revision]) error {
	return nil
}

// Update implements reconciler.Operations.
func (m *multiMockOps) Update(ctx context.Context, txn statedb.ReadTxn, rev statedb.Revision, obj *multiStatusObject) error {
	m.numUpdates++
	if m.faulty.Load() {
		return errors.New("fail")
	}
	return nil
}

var _ reconciler.Operations[*multiStatusObject] = &multiMockOps{}

// TestMultipleReconcilers tests use of multiple reconcilers against
// a single object.
func TestMultipleReconcilers(t *testing.T) {
	var table statedb.RWTable[*multiStatusObject]

	var ops1, ops2 multiMockOps
	var db *statedb.DB

	hive := hive.New(
		statedb.Cell,
		job.Cell,
		cell.Provide(
			cell.NewSimpleHealth,
			reconciler.NewExpVarMetrics,
			func(r job.Registry, h cell.Health, lc cell.Lifecycle) job.Group {
				return r.NewGroup(h, lc)
			},
		),
		cell.Invoke(func(db_ *statedb.DB) (err error) {
			db = db_
			table, err = statedb.NewTable(db, "objects", multiStatusIndex)
			return err
		}),

		cell.Module("test1", "First reconciler",
			cell.Invoke(func(params reconciler.Params) error {
				_, err := reconciler.Register(
					params,
					table,
					(*multiStatusObject).Clone,
					func(obj *multiStatusObject, s reconciler.Status) *multiStatusObject {
						obj.Statuses = obj.Statuses.Set("test1", s)
						return obj
					},
					func(obj *multiStatusObject) reconciler.Status {
						return obj.Statuses.Get("test1")
					},
					&ops1,
					nil,
					reconciler.WithRetry(time.Hour, time.Hour),
				)
				return err
			}),
		),

		cell.Module("test2", "Second reconciler",
			cell.Invoke(func(params reconciler.Params) error {
				_, err := reconciler.Register(
					params,
					table,
					(*multiStatusObject).Clone,
					func(obj *multiStatusObject, s reconciler.Status) *multiStatusObject {
						obj.Statuses = obj.Statuses.Set("test2", s)
						return obj
					},
					func(obj *multiStatusObject) reconciler.Status {
						return obj.Statuses.Get("test2")
					},
					&ops2,
					nil,
					reconciler.WithRetry(time.Hour, time.Hour),
				)
				return err
			}),
		),
	)

	log := hivetest.Logger(t, hivetest.LogLevel(slog.LevelError))
	require.NoError(t, hive.Start(log, context.TODO()), "Start")

	wtxn := db.WriteTxn(table)
	table.Insert(wtxn, &multiStatusObject{
		ID:       1,
		Statuses: reconciler.NewStatusSet(),
	})
	wtxn.Commit()

	var obj1 *multiStatusObject
	for {
		obj, _, watch, found := table.GetWatch(db.ReadTxn(), multiStatusIndex.Query(1))
		if found &&
			obj.Statuses.Get("test1").Kind == reconciler.StatusKindDone &&
			obj.Statuses.Get("test2").Kind == reconciler.StatusKindDone {

			// Check that both reconcilers performed the update only once.
			assert.Equal(t, 1, ops1.numUpdates)
			assert.Equal(t, 1, ops2.numUpdates)
			assert.Regexp(t, "^Done: test[12] test[12] \\(.* ago\\)", obj.Statuses.String())

			obj1 = obj
			break
		}
		<-watch
	}

	// Make the second reconciler faulty.
	ops2.faulty.Store(true)

	// Mark the object pending again. Reuse the StatusSet.
	wtxn = db.WriteTxn(table)
	obj1 = obj1.Clone()
	obj1.Statuses = obj1.Statuses.Pending()
	assert.Regexp(t, "^Pending: test[12] test[12] \\(.* ago\\)", obj1.Statuses.String())
	table.Insert(wtxn, obj1)
	wtxn.Commit()

	// Wait for it to reconcile.
	for {
		obj, _, watch, found := table.GetWatch(db.ReadTxn(), multiStatusIndex.Query(1))
		if found &&
			obj.Statuses.Get("test1").Kind == reconciler.StatusKindDone &&
			obj.Statuses.Get("test2").Kind == reconciler.StatusKindError {

			assert.Equal(t, 2, ops1.numUpdates)
			assert.Equal(t, 2, ops2.numUpdates)
			assert.Regexp(t, "^Errored: test2 \\(fail\\), Done: test1 \\(.* ago\\)", obj.Statuses.String())

			break
		}
		<-watch
	}

	require.NoError(t, hive.Stop(log, context.TODO()), "Stop")
}
