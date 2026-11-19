// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package reconciler

import (
	"context"
	"errors"
	"fmt"
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
	"github.com/cilium/statedb/index"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

type pruneRegressionObject struct {
	ID      uint64
	Faulty  bool
	Updates int
	Status  Status
}

func (obj *pruneRegressionObject) Clone() *pruneRegressionObject {
	clone := *obj
	return &clone
}
func (obj *pruneRegressionObject) GetStatus() Status { return obj.Status }
func (obj *pruneRegressionObject) SetStatus(status Status) *pruneRegressionObject {
	obj.Status = status
	return obj
}
func (*pruneRegressionObject) TableHeader() []string  { return []string{"ID"} }
func (obj *pruneRegressionObject) TableRow() []string { return []string{fmt.Sprint(obj.ID)} }

var pruneRegressionIDIndex = statedb.Index[*pruneRegressionObject, uint64]{
	Name: "id",
	FromObject: func(obj *pruneRegressionObject) index.KeySet {
		return index.NewKeySet(index.Uint64(obj.ID))
	},
	FromKey: index.Uint64,
	Unique:  true,
}

type pruneRegressionOps struct {
	attempted    map[uint64][]int
	prunes       [][]uint64
	pruneObjects [][]uint64
	onUpdate     func(*pruneRegressionObject) error
}

func (ops *pruneRegressionOps) Update(_ context.Context, _ statedb.ReadTxn, _ statedb.Revision, obj *pruneRegressionObject) error {
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

func (*pruneRegressionOps) Delete(context.Context, statedb.ReadTxn, statedb.Revision, *pruneRegressionObject) error {
	return nil
}

func (ops *pruneRegressionOps) Prune(_ context.Context, _ statedb.ReadTxn, objects iter.Seq2[*pruneRegressionObject, statedb.Revision]) error {
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

func (ops *pruneRegressionOps) UpdateBatch(ctx context.Context, txn statedb.ReadTxn, batch []BatchEntry[*pruneRegressionObject]) {
	for i := range batch {
		batch[i].Result = ops.Update(ctx, txn, batch[i].Revision, batch[i].Object)
	}
}

func (*pruneRegressionOps) DeleteBatch(context.Context, statedb.ReadTxn, []BatchEntry[*pruneRegressionObject]) {
}

func TestPruneAfterInitialReconciliation(t *testing.T) {
	for _, mode := range []string{"single", "batch"} {
		for _, scenario := range []string{"initial-set", "concurrent-updates", "empty-with-nonzero-revision"} {
			t.Run(mode+"/"+scenario, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					var (
						db       *statedb.DB
						table    statedb.RWTable[*pruneRegressionObject]
						markInit func(statedb.WriteTxn)
					)
					ops := &pruneRegressionOps{attempted: make(map[uint64][]int)}
					var batchOps BatchOperations[*pruneRegressionObject]
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
							table, err = statedb.NewTable(db, "prune-regression", pruneRegressionIDIndex)
							if err != nil {
								return err
							}
							txn := db.WriteTxn(table)
							defer txn.Abort()
							markInit = table.RegisterInitializer(txn, "test")
							if scenario == "empty-with-nonzero-revision" {
								// Past deletions are not yielded by a new change iterator.
								obj := &pruneRegressionObject{ID: 1, Status: StatusPending()}
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
							cell.Invoke(func(params Params) error {
								_, err := Register(
									params, table,
									(*pruneRegressionObject).Clone,
									(*pruneRegressionObject).SetStatus,
									(*pruneRegressionObject).GetStatus,
									ops, batchOps,
									WithMetrics(NewUnpublishedExpVarMetrics()),
									WithRoundLimits(1, rate.NewLimiter(1, 1)),
									WithPruning(time.Hour),
									WithRefreshing(0, nil),
									WithRetry(time.Hour, time.Hour),
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
							ops.onUpdate = func(obj *pruneRegressionObject) error {
								if obj.ID != 1 {
									return nil
								}
								// Move the remaining initial objects past the initialization
								// revision while the first round is running. Also insert an
								// object that the first prune must not include yet.
								txn := db.WriteTxn(table)
								defer txn.Abort()
								for id := uint64(2); id <= 4; id++ {
									_, _, err := table.Insert(txn, &pruneRegressionObject{
										ID: id, Updates: 1, Status: StatusPending(),
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
							_, _, err := table.Insert(txn, &pruneRegressionObject{
								ID: id, Faulty: scenario == "initial-set" && id == 2, Status: StatusPending(),
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
						obj, _, found := table.Get(db.ReadTxn(), pruneRegressionIDIndex.Query(2))
						require.True(t, found)
						require.Equal(t, StatusKindError, obj.Status.Kind,
							"a failed attempt must not block pruning")
					}
					if scenario == "concurrent-updates" {
						// Once the pinned snapshot is drained, resume reading fresh
						// snapshots and reconcile all writes made during the initial pass.
						require.Equal(t, map[uint64][]int{
							1: {0}, 2: {0, 1}, 3: {0, 1}, 4: {1},
						}, ops.attempted)
						for obj := range table.All(db.ReadTxn()) {
							require.Equal(t, StatusKindDone, obj.Status.Kind)
						}
					}
				})
			})
		}
	}
}

type retryRegressionObject struct {
	ID          uint64
	Generation  int
	Status      Status
	OtherStatus Status
}

func (*retryRegressionObject) TableHeader() []string { return []string{"ID"} }
func (obj *retryRegressionObject) TableRow() []string {
	return []string{fmt.Sprint(obj.ID)}
}

var retryRegressionIDIndex = statedb.Index[*retryRegressionObject, uint64]{
	Name: "id",
	FromObject: func(obj *retryRegressionObject) index.KeySet {
		return index.NewKeySet(index.Uint64(obj.ID))
	},
	FromKey: index.Uint64,
	Unique:  true,
}

type retryRegressionOps struct {
	update func(*retryRegressionObject) error
	calls  []retryRegressionObject
}

func (ops *retryRegressionOps) Update(_ context.Context, _ statedb.ReadTxn, _ statedb.Revision, obj *retryRegressionObject) error {
	ops.calls = append(ops.calls, *obj)
	return ops.update(obj)
}

func (*retryRegressionOps) Delete(context.Context, statedb.ReadTxn, statedb.Revision, *retryRegressionObject) error {
	return nil
}

func (*retryRegressionOps) Prune(context.Context, statedb.ReadTxn, iter.Seq2[*retryRegressionObject, statedb.Revision]) error {
	return nil
}

func (ops *retryRegressionOps) UpdateBatch(ctx context.Context, txn statedb.ReadTxn, batch []BatchEntry[*retryRegressionObject]) {
	for i := range batch {
		batch[i].Result = ops.Update(ctx, txn, batch[i].Revision, batch[i].Object)
	}
}

func (*retryRegressionOps) DeleteBatch(context.Context, statedb.ReadTxn, []BatchEntry[*retryRegressionObject]) {
}

type retryRegressionFixture struct {
	db      *statedb.DB
	table   statedb.RWTable[*retryRegressionObject]
	changes statedb.ChangeIterator[*retryRegressionObject]
	incr    incremental[*retryRegressionObject]
	ops     *retryRegressionOps
}

func newRetryRegressionFixture(t *testing.T, batch bool) *retryRegressionFixture {
	t.Helper()
	f := &retryRegressionFixture{db: statedb.New(), ops: &retryRegressionOps{}}
	var err error
	f.table, err = statedb.NewTable(f.db, "retry-regression", retryRegressionIDIndex)
	require.NoError(t, err)
	txn := f.db.WriteTxn(f.table)
	f.changes, err = f.table.Changes(txn)
	txn.Commit()
	require.NoError(t, err)
	t.Cleanup(f.changes.Close)

	cfg := &config[*retryRegressionObject]{
		Table: f.table,
		CloneObject: func(obj *retryRegressionObject) *retryRegressionObject {
			clone := *obj
			return &clone
		},
		GetObjectStatus: func(obj *retryRegressionObject) Status { return obj.Status },
		SetObjectStatus: func(obj *retryRegressionObject, status Status) *retryRegressionObject {
			obj.Status = status
			return obj
		},
		Operations: f.ops,
		options:    defaultOptions(),
	}
	if batch {
		cfg.BatchOperations = f.ops
	}
	cfg.Metrics = NewUnpublishedExpVarMetrics()
	f.incr = incremental[*retryRegressionObject]{
		db: f.db, table: f.table, config: cfg, metrics: cfg.Metrics,
		primaryIndexer: f.table.PrimaryIndexer(),
		results:        make(map[*retryRegressionObject]opResult),
		retries: newRetries(time.Second, time.Second, func(obj any) index.Key {
			return index.Uint64(obj.(*retryRegressionObject).ID)
		}),
	}
	return f
}

func (f *retryRegressionFixture) insert(t *testing.T, obj *retryRegressionObject) statedb.Revision {
	t.Helper()
	txn := f.db.WriteTxn(f.table)
	defer txn.Abort()
	_, _, err := f.table.Insert(txn, obj)
	require.NoError(t, err)
	rev := f.table.Revision(txn)
	txn.Commit()
	return rev
}

func (f *retryRegressionFixture) object(t *testing.T) *retryRegressionObject {
	t.Helper()
	obj, _, found := f.table.Get(f.db.ReadTxn(), retryRegressionIDIndex.Query(1))
	require.True(t, found)
	return obj
}

func (f *retryRegressionFixture) round() ([]error, statedb.Revision) {
	txn := f.db.ReadTxn()
	changes, _ := f.changes.Next(txn)
	errs, low := f.incr.run(context.Background(), txn, changes)
	return errs, low
}

func TestRetryStatusConflict(t *testing.T) {
	for _, batch := range []bool{false, true} {
		for _, refreshing := range []bool{false, true} {
			for _, succeeds := range []bool{false, true} {
				for _, superseded := range []bool{false, true} {
					t.Run(fmt.Sprintf("batch=%v/refreshing=%v/succeeds=%v/superseded=%v", batch, refreshing, succeeds, superseded), func(t *testing.T) {
						synctest.Test(t, func(t *testing.T) {
							f := newRetryRegressionFixture(t, batch)
							failure := errors.New("update failed")
							f.ops.update = func(*retryRegressionObject) error { return failure }
							initialStatus := StatusPending()
							if refreshing {
								initialStatus = StatusRefreshing()
							}
							f.insert(t, &retryRegressionObject{ID: 1, Status: initialStatus, OtherStatus: StatusPending()})
							f.round()
							require.Equal(t, StatusKindError, f.object(t).Status.Kind)

							otherStatus := StatusDone()
							f.ops.update = func(*retryRegressionObject) error {
								// Simulate another reconciler committing a status while the
								// retry is in flight, forcing the result's CAS to fail.
								obj := *f.object(t)
								obj.OtherStatus = otherStatus
								if superseded {
									obj.Generation++
									obj.Status = StatusPending()
								}
								f.insert(t, &obj)
								if succeeds {
									return nil
								}
								return failure
							}
							time.Sleep(time.Second)
							f.round()
							require.Len(t, f.ops.calls, 2)
							require.Equal(t, initialStatus, f.ops.calls[1].Status, "retries must retain the original refresh hint")
							require.Equal(t, otherStatus, f.object(t).OtherStatus, "must preserve the other reconciler's status")

							switch {
							case superseded:
								require.Equal(t, StatusKindPending, f.object(t).Status.Kind, "must not overwrite newer work")
								require.Equal(t, 1, f.object(t).Generation)
							case succeeds:
								require.Equal(t, StatusKindDone, f.object(t).Status.Kind, "successful retry result was lost")
								require.Empty(t, f.incr.retries.errors())
								_, queued := f.incr.retries.Top()
								require.False(t, queued)
							default:
								require.Equal(t, StatusKindError, f.object(t).Status.Kind)
								_, queued := f.incr.retries.Top()
								require.True(t, queued, "failed retry must remain scheduled")
							}

							// A failed retry must recover on its next attempt, and newer
							// desired state must still be reconciled after a stale result.
							if !succeeds || superseded {
								f.ops.update = func(*retryRegressionObject) error { return nil }
								time.Sleep(time.Second)
								f.round()
								require.Len(t, f.ops.calls, 3)
								require.Equal(t, StatusKindDone, f.object(t).Status.Kind)
								require.Equal(t, otherStatus, f.object(t).OtherStatus)
								require.Empty(t, f.incr.retries.errors())
							}
						})
					})
				}
			}
		}
	}
}
