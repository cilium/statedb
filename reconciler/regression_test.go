// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package reconciler

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/cilium/hive/cell"
	"github.com/cilium/hive/hivetest"
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

func (*pruneRegressionObject) TableHeader() []string { return []string{"ID"} }
func (obj *pruneRegressionObject) TableRow() []string {
	return []string{fmt.Sprint(obj.ID)}
}

var pruneRegressionIDIndex = statedb.Index[*pruneRegressionObject, uint64]{
	Name: "id",
	FromObject: func(obj *pruneRegressionObject) index.KeySet {
		return index.NewKeySet(index.Uint64(obj.ID))
	},
	FromKey: index.Uint64,
	Unique:  true,
}

type pruneRegressionSnapshot struct {
	attempted []uint64
	objects   []uint64
}

type pruneRegressionOps struct {
	attempted map[uint64][]int
	prunes    []pruneRegressionSnapshot
	pruned    chan pruneRegressionSnapshot
	onUpdate  func(*pruneRegressionObject) error
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
	snapshot := pruneRegressionSnapshot{attempted: slices.Sorted(maps.Keys(ops.attempted))}
	for obj := range objects {
		snapshot.objects = append(snapshot.objects, obj.ID)
	}
	slices.Sort(snapshot.objects)
	ops.prunes = append(ops.prunes, snapshot)
	select {
	case ops.pruned <- snapshot:
	default:
	}
	return nil
}

func (ops *pruneRegressionOps) UpdateBatch(ctx context.Context, txn statedb.ReadTxn, batch []BatchEntry[*pruneRegressionObject]) {
	for i := range batch {
		batch[i].Result = ops.Update(ctx, txn, batch[i].Revision, batch[i].Object)
	}
}

func (*pruneRegressionOps) DeleteBatch(context.Context, statedb.ReadTxn, []BatchEntry[*pruneRegressionObject]) {
}

type pruneRegressionHealth struct {
	cell.Health
	rounds chan struct{}
}

func (h *pruneRegressionHealth) OK(status string) {
	h.Health.OK(status)
	select {
	case h.rounds <- struct{}{}:
	default:
	}
}

func TestPruneAfterInitialReconciliation(t *testing.T) {
	for _, mode := range []string{"single", "batch"} {
		for _, scenario := range []string{"initial-set", "concurrent-updates", "empty-with-nonzero-revision"} {
			t.Run(mode+"/"+scenario, func(t *testing.T) {
				db := statedb.New()
				table, err := statedb.NewTable("prune-regression", pruneRegressionIDIndex)
				require.NoError(t, err)
				require.NoError(t, db.RegisterTable(table))
				txn := db.WriteTxn(table)
				markInit := table.RegisterInitializer(txn, "test")
				if scenario == "empty-with-nonzero-revision" {
					// A new change iterator will not observe this past deletion.
					obj := &pruneRegressionObject{ID: 1, Status: StatusPending()}
					_, _, err = table.Insert(txn, obj)
					require.NoError(t, err)
					_, _, err = table.Delete(txn, obj)
					require.NoError(t, err)
					markInit(txn)
				}
				txn.Commit()

				ops := &pruneRegressionOps{
					attempted: make(map[uint64][]int),
					pruned:    make(chan pruneRegressionSnapshot, 1),
				}
				if scenario == "concurrent-updates" {
					ops.onUpdate = func(obj *pruneRegressionObject) error {
						if obj.ID != 1 {
							return nil
						}
						// Move the remaining initial objects past the initialization
						// revision and add an object outside the initial snapshot.
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
				cfg := config[*pruneRegressionObject]{
					Table: table, CloneObject: (*pruneRegressionObject).Clone,
					GetObjectStatus: func(obj *pruneRegressionObject) Status { return obj.Status },
					SetObjectStatus: func(obj *pruneRegressionObject, status Status) *pruneRegressionObject {
						obj.Status = status
						return obj
					},
					Operations: ops, options: defaultOptions(),
				}
				cfg.Metrics = NewUnpublishedExpVarMetrics()
				cfg.IncrementalRoundSize = 1
				cfg.RateLimiter = rate.NewLimiter(rate.Inf, 1)
				cfg.PruneInterval = time.Hour
				if mode == "batch" {
					cfg.BatchOperations = ops
				}
				r := &reconciler[*pruneRegressionObject]{
					Params: Params{DB: db, Log: hivetest.Logger(t)}, config: cfg,
					primaryIndexer: table.PrimaryIndexer(),
					retries: newRetries(time.Hour, time.Hour, func(obj any) index.Key {
						return index.Uint64(obj.(*pruneRegressionObject).ID)
					}),
					externalPruneTrigger: make(chan struct{}, 1),
				}
				health, _ := cell.NewSimpleHealth()
				h := &pruneRegressionHealth{Health: health, rounds: make(chan struct{}, 1)}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				done := make(chan struct{})
				go func() {
					defer close(done)
					r.reconcileLoop(ctx, h)
				}()
				stop := func() {
					cancel()
					select {
					case <-done:
					case <-time.After(5 * time.Second):
						t.Fatal("reconciler did not stop")
					}
					if timer := r.retries.waitTimer; timer != nil {
						timer.Stop()
					}
				}
				defer stop()

				// Wait for an empty round before publishing the initial set. Channel
				// synchronization keeps this test compatible with Go 1.24.
				select {
				case <-h.rounds:
				case <-ctx.Done():
					t.Fatal("reconciler did not complete its first round")
				}
				var want []uint64
				if scenario != "empty-with-nonzero-revision" {
					select {
					case <-ops.pruned:
						t.Fatal("pruned before initialization")
					default:
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
				var snapshot pruneRegressionSnapshot
				select {
				case snapshot = <-ops.pruned:
				case <-ctx.Done():
					t.Fatal("initial pruning did not run")
				}
				if scenario == "concurrent-updates" {
					// Observe reconciliation of the new object after the initial prune.
					for {
						obj, _, watch, found := table.GetWatch(db.ReadTxn(), pruneRegressionIDIndex.Query(4))
						if found && obj.Status.Kind == StatusKindDone {
							break
						}
						select {
						case <-watch:
						case <-ctx.Done():
							t.Fatal("writes made during the initial pass were not reconciled")
						}
					}
				}
				stop()
				require.Len(t, ops.prunes, 1)
				require.Equal(t, want, snapshot.attempted, "pruned before every initial object was attempted")
				require.Equal(t, want, snapshot.objects, "initial pruning must use the initial snapshot")
				if scenario == "initial-set" {
					obj, _, found := table.Get(db.ReadTxn(), pruneRegressionIDIndex.Query(2))
					require.True(t, found)
					require.Equal(t, StatusKindError, obj.Status.Kind, "a failed attempt must not block pruning")
				}
				if scenario == "concurrent-updates" {
					require.Equal(t, map[uint64][]int{
						1: {0}, 2: {0, 1}, 3: {0, 1}, 4: {1},
					}, ops.attempted)
					for obj := range table.All(db.ReadTxn()) {
						require.Equal(t, StatusKindDone, obj.Status.Kind)
					}
				}
			})
		}
	}
}
