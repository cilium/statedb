// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package reconciler_test

import (
	"context"
	"fmt"
	"iter"
	"log/slog"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cilium/hive"
	"github.com/cilium/hive/cell"
	"github.com/cilium/hive/hivetest"
	"github.com/cilium/hive/job"
	"github.com/cilium/statedb"
	"github.com/cilium/statedb/index"
	"github.com/cilium/statedb/reconciler"
	"github.com/stretchr/testify/require"
)

type waitObject struct {
	ID     uint64
	Status reconciler.Status
}

// TableHeader implements statedb.TableWritable.
func (w waitObject) TableHeader() []string {
	return []string{"ID", "Status"}
}

// TableRow implements statedb.TableWritable.
func (w waitObject) TableRow() []string {
	return []string{
		fmt.Sprintf("%d", w.ID),
		w.Status.String(),
	}
}

var _ statedb.TableWritable = waitObject{}

func (w *waitObject) Clone() *waitObject {
	w2 := *w
	return &w2
}

func (w *waitObject) GetStatus() reconciler.Status {
	return w.Status
}

func (w *waitObject) SetStatus(status reconciler.Status) *waitObject {
	w.Status = status
	return w
}

var waitObjectIDIndex = statedb.Index[*waitObject, uint64]{
	Name: "id",
	FromObject: func(obj *waitObject) index.KeySet {
		return index.NewKeySet(index.Uint64(obj.ID))
	},
	FromKey: index.Uint64,
	Unique:  true,
}

type waitOps struct {
	started     chan struct{}
	unblock     chan struct{}
	startedOnce sync.Once
}

func newWaitOps() *waitOps {
	return &waitOps{
		started: make(chan struct{}),
		unblock: make(chan struct{}),
	}
}

// Delete implements reconciler.Operations.
func (*waitOps) Delete(context.Context, statedb.ReadTxn, statedb.Revision, *waitObject) error {
	return nil
}

// Prune implements reconciler.Operations.
func (*waitOps) Prune(context.Context, statedb.ReadTxn, iter.Seq2[*waitObject, statedb.Revision]) error {
	return nil
}

// Update implements reconciler.Operations.
func (w *waitOps) Update(ctx context.Context, txn statedb.ReadTxn, rev statedb.Revision, obj *waitObject) error {
	w.startedOnce.Do(func() {
		close(w.started)
	})
	select {
	case <-w.unblock:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

var _ reconciler.Operations[*waitObject] = &waitOps{}

func TestWaitUntilReconciled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var (
			table statedb.RWTable[*waitObject]
			db    *statedb.DB
			r     reconciler.Reconciler[*waitObject]
		)
		ops := newWaitOps()

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
				table, err = statedb.NewTable(db, "wait-objects", waitObjectIDIndex)
				return err
			}),
			cell.Module("test", "test",
				cell.Invoke(func(params reconciler.Params) error {
					var err error
					r, err = reconciler.Register(
						params,
						table,
						(*waitObject).Clone,
						(*waitObject).SetStatus,
						(*waitObject).GetStatus,
						ops,
						nil,
						reconciler.WithoutPruning(),
					)
					return err
				}),
			),
		)

		log := hivetest.Logger(t, hivetest.LogLevel(slog.LevelError))
		require.NoError(t, hive.Start(log, context.TODO()), "Start")
		defer func() {
			require.NoError(t, hive.Stop(log, context.TODO()), "Stop")
		}()

		waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		// Won't block if we query with 0 revision.
		_, err := r.WaitUntilReconciled(waitCtx, 0)
		require.NoError(t, err)

		// Insert an object and wait for it to be reconciled.
		wtxn := db.WriteTxn(table)
		table.Insert(wtxn, &waitObject{
			ID:     1,
			Status: reconciler.StatusPending(),
		})
		revision := table.Revision(wtxn)
		wtxn.Commit()

		type waitResult struct {
			rev statedb.Revision
			err error
		}
		done := make(chan waitResult, 1)
		go func() {
			rev, err := r.WaitUntilReconciled(waitCtx, revision)
			done <- waitResult{rev: rev, err: err}
		}()

		synctest.Wait()
		select {
		case <-ops.started:
		default:
			t.Fatal("expected update to start")
		}

		select {
		case result := <-done:
			t.Fatalf("WaitUntilReconciled returned early: %v", result.err)
		default:
		}

		close(ops.unblock)
		synctest.Wait()

		select {
		case result := <-done:
			require.NoError(t, result.err)
			require.Equal(t, revision, result.rev)
		default:
			t.Fatal("expected WaitUntilReconciled to complete")
		}
	})
}
