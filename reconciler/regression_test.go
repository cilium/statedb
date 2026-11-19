// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package reconciler_test

import (
	"context"
	"errors"
	"iter"
	"log/slog"
	"maps"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cilium/hive"
	"github.com/cilium/hive/cell"
	"github.com/cilium/hive/hivetest"
	"github.com/cilium/hive/job"
	"github.com/cilium/statedb"
	"github.com/cilium/statedb/reconciler"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

type pruneRegressionOps struct {
	attempted    map[uint64][]int
	prunes       [][]uint64
	pruneObjects [][]uint64
	onUpdate     func(*testObject) error
}

func (ops *pruneRegressionOps) Update(_ context.Context, _ statedb.ReadTxn, _ statedb.Revision, obj *testObject) error {
	ops.attempted[obj.ID] = append(ops.attempted[obj.ID], obj.Updates)
	if ops.onUpdate != nil {
		if err := ops.onUpdate(obj); err != nil {
			return err
		}
	}
	if obj.Faulty {
		return errors.New("update failed")
	}
	return nil
}

func (*pruneRegressionOps) Delete(context.Context, statedb.ReadTxn, statedb.Revision, *testObject) error {
	return nil
}

func (ops *pruneRegressionOps) Prune(_ context.Context, _ statedb.ReadTxn, objects iter.Seq2[*testObject, statedb.Revision]) error {
	// Snapshot distinct IDs so retries cannot hide a missing initial attempt.
	ops.prunes = append(ops.prunes, slices.Sorted(maps.Keys(ops.attempted)))
	var ids []uint64
	for obj := range objects {
		ids = append(ids, obj.ID)
	}
	slices.Sort(ids)
	ops.pruneObjects = append(ops.pruneObjects, ids)
	return nil
}

func (ops *pruneRegressionOps) UpdateBatch(ctx context.Context, txn statedb.ReadTxn, batch []reconciler.BatchEntry[*testObject]) {
	for i := range batch {
		batch[i].Result = ops.Update(ctx, txn, batch[i].Revision, batch[i].Object)
	}
}

func (*pruneRegressionOps) DeleteBatch(context.Context, statedb.ReadTxn, []reconciler.BatchEntry[*testObject]) {
}

func TestPruneAfterInitialReconciliation(t *testing.T) {
	for _, mode := range []string{"single", "batch"} {
		for _, scenario := range []string{"initial-set", "concurrent-updates", "empty-with-nonzero-revision"} {
			t.Run(mode+"/"+scenario, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					var (
						db       *statedb.DB
						table    statedb.RWTable[*testObject]
						markInit func(statedb.WriteTxn)
					)
					ops := &pruneRegressionOps{attempted: make(map[uint64][]int)}
					var batchOps reconciler.BatchOperations[*testObject]
					if mode == "batch" {
						batchOps = ops
					}

					h := hive.New(
						statedb.Cell,
						job.Cell,
						cell.Provide(
							cell.NewSimpleHealth,
							func(registry job.Registry, health cell.Health) job.Group {
								return registry.NewGroup(health)
							},
						),
						cell.Invoke(func(db_ *statedb.DB) error {
							db = db_
							var err error
							table, err = statedb.NewTable(db, "prune-regression", idIndex)
							if err != nil {
								return err
							}
							txn := db.WriteTxn(table)
							defer txn.Abort()
							markInit = table.RegisterInitializer(txn, "test")
							if scenario == "empty-with-nonzero-revision" {
								// Past deletions are not yielded by a new change iterator.
								obj := &testObject{ID: 1, Status: reconciler.StatusPending()}
								_, _, err = table.Insert(txn, obj)
								if err != nil {
									return err
								}
								_, _, err = table.Delete(txn, obj)
								if err != nil {
									return err
								}
								markInit(txn)
							}
							txn.Commit()
							return nil
						}),
						cell.Module("test", "Test",
							cell.Invoke(func(params reconciler.Params) error {
								_, err := reconciler.Register(
									params, table,
									(*testObject).Clone,
									(*testObject).SetStatus,
									(*testObject).GetStatus,
									ops, batchOps,
									reconciler.WithMetrics(reconciler.NewUnpublishedExpVarMetrics()),
									reconciler.WithRoundLimits(1, rate.NewLimiter(1, 1)),
									reconciler.WithPruning(time.Hour),
									reconciler.WithRefreshing(0, nil),
									reconciler.WithRetry(time.Hour, time.Hour),
								)
								return err
							}),
						),
					)
					log := hivetest.Logger(t, hivetest.LogLevel(slog.LevelError))
					require.NoError(t, h.Start(log, context.Background()))
					defer func() {
						require.NoError(t, h.Stop(log, context.Background()))
					}()

					synctest.Wait()
					var want []uint64
					if scenario != "empty-with-nonzero-revision" {
						require.Empty(t, ops.prunes, "must not prune before initialization")
						// The first empty round has completed and the rate limiter
						// holds the next round. Make initialization and table changes
						// ready together, with a revision newer than that round's snapshot.
						if scenario == "concurrent-updates" {
							ops.onUpdate = func(obj *testObject) error {
								if obj.ID != 1 {
									return nil
								}
								// Move the remaining initial objects past the initialization
								// revision while the first round is running. Also insert an
								// object that the first prune must not include yet.
								txn := db.WriteTxn(table)
								defer txn.Abort()
								for id := uint64(2); id <= 4; id++ {
									_, _, err := table.Insert(txn, &testObject{
										ID: id, Updates: 1, Status: reconciler.StatusPending(),
									})
									if err != nil {
										return err
									}
								}
								txn.Commit()
								return nil
							}
						}
						txn := db.WriteTxn(table)
						for id := uint64(1); id <= 3; id++ {
							_, _, err := table.Insert(txn, &testObject{
								ID: id, Faulty: scenario == "initial-set" && id == 2, Status: reconciler.StatusPending(),
							})
							require.NoError(t, err)
						}
						markInit(txn)
						txn.Commit()
						want = []uint64{1, 2, 3}
					} else {
						require.Zero(t, table.NumObjects(db.ReadTxn()))
						require.NotZero(t, table.Revision(db.ReadTxn()))
					}

					// Advance virtual time through several rounds, but not far enough
					// for periodic pruning or retries to mask a missing initial prune.
					time.Sleep(10 * time.Second)
					synctest.Wait()
					require.Len(t, ops.prunes, 1, "initial pruning must eventually run")
					require.Equal(t, want, ops.prunes[0], "pruned before every initial object was attempted")
					require.Equal(t, want, ops.pruneObjects[0], "initial pruning must use the initial snapshot")
					if scenario == "initial-set" {
						obj, _, found := table.Get(db.ReadTxn(), idIndex.Query(2))
						require.True(t, found)
						require.Equal(t, reconciler.StatusKindError, obj.Status.Kind,
							"a failed attempt must not block pruning")
					}
					if scenario == "concurrent-updates" {
						// Once the pinned snapshot is drained, resume reading fresh
						// snapshots and reconcile all writes made during the initial pass.
						require.Equal(t, map[uint64][]int{
							1: {0}, 2: {0, 1}, 3: {0, 1}, 4: {1},
						}, ops.attempted)
						for obj := range table.All(db.ReadTxn()) {
							require.Equal(t, reconciler.StatusKindDone, obj.Status.Kind)
						}
					}
				})
			})
		}
	}
}
