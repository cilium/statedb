// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package reconciler_test

import (
	"context"
	"errors"
	"expvar"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"golang.org/x/exp/slices"
	"golang.org/x/time/rate"

	"github.com/cilium/hive"
	"github.com/cilium/hive/cell"
	"github.com/cilium/hive/job"
	"github.com/cilium/statedb"
	"github.com/cilium/statedb/index"
	"github.com/cilium/statedb/reconciler"
)

// Some constants so we don't use mysterious numbers in the test steps.
const (
	ID_1 = uint64(1)
	ID_2 = uint64(2)
	ID_3 = uint64(3)
)

func TestReconciler(t *testing.T) {
	testReconciler(t, false)
}

func TestReconciler_Batch(t *testing.T) {
	testReconciler(t, true)
}

func testReconciler(t *testing.T, batchOps bool) {
	defer goleak.VerifyNone(t,
		goleak.IgnoreCurrent(),
	)

	getInt := func(v expvar.Var) int64 {
		if v, ok := v.(*expvar.Int); ok && v != nil {
			return v.Value()
		}
		return -1
	}

	getFloat := func(v expvar.Var) float64 {
		if v, ok := v.(*expvar.Float); ok && v != nil {
			return v.Value()
		}
		return -1
	}

	runTest := func(name string, opts []reconciler.Option, run func(testHelper)) {
		var (
			ops        = &mockOps{}
			db         *statedb.DB
			r          reconciler.Reconciler[*testObject]
			fakeHealth *cell.SimpleHealth
			markInit   func()
		)

		expVarMetrics := reconciler.NewUnpublishedExpVarMetrics()

		testObjects, err := statedb.NewTable[*testObject]("test-objects", idIndex, statusIndex)
		require.NoError(t, err, "NewTable")

		hive := hive.New(
			statedb.Cell,
			job.Cell,

			cell.Provide(cell.NewSimpleHealth),

			cell.Module(
				"test",
				"Test",

				cell.Provide(func() reconciler.Metrics {
					return expVarMetrics
				}),

				cell.Invoke(func(db_ *statedb.DB) error {
					db = db_
					return db.RegisterTable(testObjects)
				}),
				cell.Provide(func(p reconciler.Params) (reconciler.Reconciler[*testObject], error) {
					var bops reconciler.BatchOperations[*testObject]
					if batchOps {
						bops = ops
					}
					return reconciler.Register(
						p,
						testObjects,
						(*testObject).Clone,
						(*testObject).SetStatus,
						(*testObject).GetStatus,
						ops,
						bops,
						append(
							[]reconciler.Option{
								// Speed things up a bit.
								reconciler.WithRetry(5*time.Millisecond, 5*time.Millisecond),
								reconciler.WithRoundLimits(1000, rate.NewLimiter(1000.0, 10)),
							},
							// Add the override options last.
							opts...,
						)...,
					)
				}),

				cell.Invoke(func(r_ reconciler.Reconciler[*testObject], h *cell.SimpleHealth) {
					r = r_
					fakeHealth = h
					wtxn := db.WriteTxn(testObjects)
					done := testObjects.RegisterInitializer(wtxn, "test")
					wtxn.Commit()
					markInit = func() {
						wtxn := db.WriteTxn(testObjects)
						done(wtxn)
						wtxn.Commit()
					}
				}),
			),
		)

		t.Run(name, func(t *testing.T) {
			require.NoError(t, hive.Start(context.TODO()), "Start")
			t.Cleanup(func() {
				assert.NoError(t, hive.Stop(context.TODO()), "Stop")
			})
			run(testHelper{
				t:        t,
				db:       db,
				tbl:      testObjects,
				ops:      ops,
				r:        r,
				health:   fakeHealth,
				m:        expVarMetrics,
				markInit: markInit,
			})

		})
	}

	numIterations := 3

	runTest("incremental",
		[]reconciler.Option{
			reconciler.WithPruning(0), // Disable
		},
		func(h testHelper) {
			h.markInitialized()
			for i := 0; i < numIterations; i++ {
				t.Logf("Iteration %d", i)

				// Insert some test objects and check that they're reconciled
				t.Logf("Inserting test objects 1, 2 & 3")
				h.insert(ID_1, NonFaulty, reconciler.StatusPending())
				h.expectOp(opUpdate(ID_1))
				h.expectStatus(ID_1, reconciler.StatusKindDone, "")

				h.insert(ID_2, NonFaulty, reconciler.StatusPending())
				h.expectOp(opUpdate(ID_2))
				h.expectStatus(ID_2, reconciler.StatusKindDone, "")

				h.insert(ID_3, NonFaulty, reconciler.StatusPending())
				h.expectOp(opUpdate(ID_3))
				h.expectStatus(ID_3, reconciler.StatusKindDone, "")

				h.expectHealth(cell.StatusOK, "OK, 3 object(s)", "")
				h.waitForReconciliation()

				// Set one to be faulty => object will error
				t.Log("Setting '1' faulty")
				h.insert(ID_1, Faulty, reconciler.StatusPending())
				h.expectOp(opFail(opUpdate(ID_1)))
				h.expectStatus(ID_1, reconciler.StatusKindError, "update fail")
				h.expectRetried(ID_1)
				h.expectHealth(cell.StatusDegraded, "1 error(s)", "update fail")

				// Fix the object => object will reconcile again.
				t.Log("Setting '1' non-faulty")
				h.insert(ID_1, NonFaulty, reconciler.StatusPending())
				h.expectOp(opUpdate(ID_1))
				h.expectStatus(ID_1, reconciler.StatusKindDone, "")
				h.expectHealth(cell.StatusOK, "OK, 3 object(s)", "")

				t.Log("Delete 1 & 2")
				h.markForDelete(ID_1)
				h.expectOp(opDelete(1))
				h.expectNotFound(ID_1)

				h.markForDelete(ID_2)
				h.expectOp(opDelete(2))
				h.expectNotFound(ID_2)

				t.Log("Try to delete '3' with faulty ops")
				h.setTargetFaulty(true)
				h.markForDelete(ID_3)
				h.expectOp(opFail(opDelete(3)))
				h.expectHealth(cell.StatusDegraded, "1 error(s)", "delete fail")

				t.Log("Set the target non-faulty to delete '3'")
				h.setTargetFaulty(false)
				h.expectOp(opDelete(3))
				h.expectHealth(cell.StatusOK, "OK, 0 object(s)", "")

				h.waitForReconciliation()

				assert.Greater(t, getInt(h.m.ReconciliationCountVar.Get("test")), int64(0), "ReconciliationCount")
				assert.Greater(t, getFloat(h.m.ReconciliationDurationVar.Get("test/update")), float64(0), "ReconciliationDuration/update")
				assert.Greater(t, getFloat(h.m.ReconciliationDurationVar.Get("test/delete")), float64(0), "ReconciliationDuration/delete")
				assert.Greater(t, getInt(h.m.ReconciliationTotalErrorsVar.Get("test")), int64(0), "ReconciliationTotalErrors")
				assert.Equal(t, getInt(h.m.ReconciliationCurrentErrorsVar.Get("test")), int64(0), "ReconciliationCurrentErrors")
			}
		})

	runTest("pruning", nil, func(h testHelper) {
		// Without any objects, we should not be able to see a prune,
		// even when triggered.
		t.Log("Try to prune without objects and uninitialized table")
		h.triggerPrune()
		h.expectHealth(cell.StatusOK, "OK, 0 object(s)", "")

		// With table not initialized, we should not see the prune even
		// when triggered.
		t.Log("Try to prune with object and uninitialized table")
		h.insert(ID_1, NonFaulty, reconciler.StatusPending())
		h.triggerPrune()
		h.expectOp(opUpdate(ID_1))
		h.expectHealth(cell.StatusOK, "OK, 1 object(s)", "")
		h.markInitialized()

		// After initialization we expect to see pruning immediately
		// as soon as something changes. Marking the table initialized
		// does not yet trigger the reconciler since it's waiting for
		// objects to change.
		h.insert(ID_2, NonFaulty, reconciler.StatusPending())
		h.expectOps(opUpdate(ID_2), opPrune(2))
		h.expectHealth(cell.StatusOK, "OK, 2 object(s)", "")

		// Pruning can be now triggered at will.
		h.triggerPrune()
		h.expectOps(opUpdate(ID_2), opPrune(2), opPrune(2))

		// Add few objects and wait until incremental reconciliation is done.
		t.Log("Insert more objects")
		h.insert(ID_1, NonFaulty, reconciler.StatusPending())
		h.insert(ID_2, NonFaulty, reconciler.StatusPending())
		h.insert(ID_3, NonFaulty, reconciler.StatusPending())
		h.expectStatus(ID_1, reconciler.StatusKindDone, "")
		h.expectStatus(ID_2, reconciler.StatusKindDone, "")
		h.expectStatus(ID_3, reconciler.StatusKindDone, "")
		h.expectNumUpdates(ID_1, 1)
		h.expectNumUpdates(ID_2, 1)
		h.expectNumUpdates(ID_3, 1)
		h.expectHealth(cell.StatusOK, "OK, 3 object(s)", "")

		// Pruning with functioning ops.
		t.Log("Prune with non-faulty ops")
		h.triggerPrune()
		h.expectOps(opPrune(3))
		h.expectHealth(cell.StatusOK, "OK, 3 object(s)", "")

		// Make the ops faulty and trigger the pruning.
		t.Log("Prune with faulty ops")
		h.setTargetFaulty(true)
		h.triggerPrune()
		h.expectOps(opPrune(3))
		h.expectHealth(cell.StatusDegraded, "1 error(s)", "prune: prune fail")

		// Make the ops healthy again and try pruning again.
		t.Log("Prune again with non-faulty ops")
		h.setTargetFaulty(false)
		h.triggerPrune()
		h.expectHealth(cell.StatusOK, "OK, 3 object(s)", "")

		// Cleanup.
		h.markForDelete(ID_1)
		h.markForDelete(ID_2)
		h.markForDelete(ID_3)
		h.expectNotFound(ID_1)
		h.expectNotFound(ID_2)
		h.expectNotFound(ID_3)
		h.triggerPrune()
		h.expectOps(opDelete(1), opDelete(2), opDelete(3), opPrune(0))
		h.waitForReconciliation()

		// Validate metrics.
		assert.Greater(t, getInt(h.m.PruneCountVar.Get("test")), int64(0), "PruneCount")
		assert.Greater(t, getFloat(h.m.PruneDurationVar.Get("test")), float64(0), "PruneDuration")
		assert.Equal(t, getInt(h.m.PruneCurrentErrorsVar.Get("test")), int64(0), "PruneCurrentErrors")
	})

	runTest("refreshing",
		[]reconciler.Option{
			reconciler.WithPruning(0), // Disable
			reconciler.WithRefreshing(500*time.Millisecond, rate.NewLimiter(100.0, 1)),
		},
		func(h testHelper) {
			t.Logf("Inserting test object 1")
			h.insert(ID_1, NonFaulty, reconciler.StatusPending())
			h.expectOp(opUpdate(ID_1))
			h.expectStatus(ID_1, reconciler.StatusKindDone, "")

			t.Logf("Setting UpdatedAt to be in past to force refresh")
			status := reconciler.StatusDone()
			status.UpdatedAt = status.UpdatedAt.Add(-2 * time.Minute)
			h.insert(ID_1, NonFaulty, status)

			h.expectOps(
				opUpdate(ID_1),        // Initial insert
				opUpdateRefresh(ID_1), // The refresh
			)

			t.Logf("Setting target faulty and forcing refresh")
			h.setTargetFaulty(true)
			status.UpdatedAt = status.UpdatedAt.Add(-time.Minute)
			h.insert(ID_1, NonFaulty, status)
			h.expectOp(opFail(opUpdateRefresh(ID_1)))
			h.expectStatus(ID_1, reconciler.StatusKindError, "update fail")
			h.expectRetried(ID_1)
			h.expectHealth(cell.StatusDegraded, "1 error(s)", "update fail")

			t.Logf("Setting target healthy")
			h.setTargetFaulty(false)
			h.insert(ID_1, NonFaulty, status)
			h.expectOp(opUpdateRefresh(ID_1))
			h.expectStatus(ID_1, reconciler.StatusKindDone, "")
			h.expectHealth(cell.StatusOK, "OK, 1 object(s)", "")

		})
}

type testObject struct {
	id      uint64
	faulty  bool
	updates int
	status  reconciler.Status
}

var idIndex = statedb.Index[*testObject, uint64]{
	Name: "id",
	FromObject: func(t *testObject) index.KeySet {
		return index.NewKeySet(index.Uint64(t.id))
	},
	FromKey: index.Uint64,
	Unique:  true,
}

var statusIndex = reconciler.NewStatusIndex((*testObject).GetStatus)

func (t *testObject) GetStatus() reconciler.Status {
	return t.status
}

func (t *testObject) SetStatus(status reconciler.Status) *testObject {
	t.status = status
	return t
}

func (t *testObject) Clone() *testObject {
	t2 := *t
	return &t2
}

type opHistory struct {
	mu      sync.Mutex
	history []opHistoryItem
}

type opHistoryItem = string

func opUpdate(id uint64) opHistoryItem {
	return opHistoryItem(fmt.Sprintf("update(%d)", id))
}
func opUpdateRefresh(id uint64) opHistoryItem {
	return opHistoryItem(fmt.Sprintf("update-refresh(%d)", id))
}
func opDelete(id uint64) opHistoryItem {
	return opHistoryItem(fmt.Sprintf("delete(%d)", id))
}
func opPrune(numDesiredObjects int) opHistoryItem {
	return opHistoryItem(fmt.Sprintf("prune(n=%d)", numDesiredObjects))
}
func opFail(item opHistoryItem) opHistoryItem {
	return item + " fail"
}

func (o *opHistory) add(item opHistoryItem) {
	o.mu.Lock()
	o.history = append(o.history, item)
	o.mu.Unlock()
}

func (o *opHistory) latest() opHistoryItem {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.history) > 0 {
		return o.history[len(o.history)-1]
	}
	return "<empty history>"
}

func (o *opHistory) take(n int) []opHistoryItem {
	o.mu.Lock()
	defer o.mu.Unlock()

	out := []opHistoryItem{}
	for n > 0 {
		idx := len(o.history) - n
		if idx >= 0 {
			out = append(out, o.history[idx])
		}
		n--
	}
	return out
}

type intMap struct {
	sync.Map
}

func (m *intMap) incr(key uint64) {
	if n, ok := m.Load(key); ok {
		m.Store(key, n.(int)+1)
	} else {
		m.Store(key, 1)
	}
}

func (m *intMap) get(key uint64) int {
	if n, ok := m.Load(key); ok {
		return n.(int)
	}
	return 0
}

type mockOps struct {
	history opHistory
	faulty  atomic.Bool
	updates intMap
}

// DeleteBatch implements recogciler.BatchOperations.
func (mt *mockOps) DeleteBatch(ctx context.Context, txn statedb.ReadTxn, batch []reconciler.BatchEntry[*testObject]) {
	for i := range batch {
		batch[i].Result = mt.Delete(ctx, txn, batch[i].Object)
	}
}

// UpdateBatch implements reconciler.BatchOperations.
func (mt *mockOps) UpdateBatch(ctx context.Context, txn statedb.ReadTxn, batch []reconciler.BatchEntry[*testObject]) {
	for i := range batch {
		batch[i].Result = mt.Update(ctx, txn, batch[i].Object)
	}
}

// Delete implements reconciler.Operations.
func (mt *mockOps) Delete(ctx context.Context, txn statedb.ReadTxn, obj *testObject) error {
	if mt.faulty.Load() || obj.faulty {
		mt.history.add(opFail(opDelete(obj.id)))
		return errors.New("delete fail")
	}
	mt.history.add(opDelete(obj.id))

	return nil
}

// Prune implements reconciler.Operations.
func (mt *mockOps) Prune(ctx context.Context, txn statedb.ReadTxn, iter statedb.Iterator[*testObject]) error {
	if mt.faulty.Load() {
		return errors.New("prune fail")
	}
	objs := statedb.Collect(iter)
	mt.history.add(opPrune(len(objs)))
	return nil
}

// Update implements reconciler.Operations.
func (mt *mockOps) Update(ctx context.Context, txn statedb.ReadTxn, obj *testObject) error {
	mt.updates.incr(obj.id)

	op := opUpdate(obj.id)
	if obj.status.Kind == reconciler.StatusKindRefreshing {
		op = opUpdateRefresh(obj.id)
	}
	if mt.faulty.Load() || obj.faulty {
		mt.history.add(opFail(op))
		return errors.New("update fail")
	}
	mt.history.add(op)
	obj.updates += 1

	return nil
}

var _ reconciler.Operations[*testObject] = &mockOps{}
var _ reconciler.BatchOperations[*testObject] = &mockOps{}

// testHelper defines a sort of mini-language for writing the test steps.
type testHelper struct {
	t        testing.TB
	db       *statedb.DB
	tbl      statedb.RWTable[*testObject]
	ops      *mockOps
	r        reconciler.Reconciler[*testObject]
	health   *cell.SimpleHealth
	m        *reconciler.ExpVarMetrics
	markInit func()
}

const (
	Faulty    = true
	NonFaulty = false
)

func (h testHelper) markInitialized() {
	h.markInit()
	h.markInit = nil
}

func (h testHelper) insert(id uint64, faulty bool, status reconciler.Status) {
	wtxn := h.db.WriteTxn(h.tbl)
	_, _, err := h.tbl.Insert(wtxn, &testObject{
		id:     id,
		faulty: faulty,
		status: status,
	})
	require.NoError(h.t, err, "Insert failed")
	wtxn.Commit()
}

func (h testHelper) markForDelete(id uint64) {
	wtxn := h.db.WriteTxn(h.tbl)
	_, _, err := h.tbl.Delete(wtxn, &testObject{id: id})
	require.NoError(h.t, err, "Delete failed")
	wtxn.Commit()
}

func (h testHelper) expectStatus(id uint64, kind reconciler.StatusKind, err string) {
	cond := func() bool {
		obj, _, ok := h.tbl.Get(h.db.ReadTxn(), idIndex.Query(id))
		return ok && obj.status.Kind == kind && obj.status.Error == err
	}
	if !assert.Eventually(h.t, cond, time.Second, time.Millisecond) {
		actual := "<not found>"
		obj, _, ok := h.tbl.Get(h.db.ReadTxn(), idIndex.Query(id))
		if ok {
			actual = string(obj.status.Kind)
		}
		require.Failf(h.t, "status mismatch", "expected object %d to be marked with status %q, but it was %q",
			id, kind, actual)
	}
}

func (h testHelper) expectNumUpdates(id uint64, n int) {
	cond := func() bool {
		obj, _, ok := h.tbl.Get(h.db.ReadTxn(), idIndex.Query(id))
		return ok && obj.updates == n
	}
	if !assert.Eventually(h.t, cond, time.Second, time.Millisecond) {
		actual := "<not found>"
		obj, _, ok := h.tbl.Get(h.db.ReadTxn(), idIndex.Query(id))
		if ok {
			actual = fmt.Sprintf("%d", obj.updates)
		}
		require.Failf(h.t, "updates mismatch", "expected object %d to be have %d updates but it had %q",
			id, n, actual)
	}
}

func (h testHelper) expectNotFound(id uint64) {
	h.t.Helper()
	cond := func() bool {
		_, _, ok := h.tbl.Get(h.db.ReadTxn(), idIndex.Query(id))
		return !ok
	}
	require.Eventually(h.t, cond, time.Second, time.Millisecond, "expected object %d to not be found", id)
}

func (h testHelper) expectOp(op opHistoryItem) {
	h.t.Helper()
	cond := func() bool {
		return h.ops.history.latest() == op
	}
	if !assert.Eventually(h.t, cond, time.Second, time.Millisecond) {
		require.Failf(h.t, "operation mismatch", "expected last operation to be %q, it was %q", op, h.ops.history.latest())
	}
}

func (h testHelper) expectOps(ops ...opHistoryItem) {
	h.t.Helper()
	sort.Strings(ops)
	cond := func() bool {
		actual := h.ops.history.take(len(ops))
		sort.Strings(actual)
		return slices.Equal(ops, actual)
	}
	if !assert.Eventually(h.t, cond, time.Second, time.Millisecond) {
		actual := h.ops.history.take(len(ops))
		sort.Strings(actual)
		require.Failf(h.t, "operations mismatch", "expected operations to be %v, but they were %v", ops, actual)
	}
}

func (h testHelper) expectRetried(id uint64) {
	h.t.Helper()
	old := h.ops.updates.get(id)
	cond := func() bool {
		new := h.ops.updates.get(id)
		return new > old
	}
	require.Eventually(h.t, cond, time.Second, time.Millisecond, "expected %d to be retried", id)
}

func (h testHelper) expectHealth(level cell.Level, statusSubString string, errSubString string) {
	h.t.Helper()
	cond := func() bool {
		health := h.health.GetChild("test", "job-reconcile")
		require.NotNil(h.t, health, "GetChild")
		health.Lock()
		defer health.Unlock()
		errStr := ""
		if health.Error != nil {
			errStr = health.Error.Error()
		}
		return level == health.Level && strings.Contains(health.Status, statusSubString) && strings.Contains(errStr, errSubString)
	}
	if !assert.Eventually(h.t, cond, time.Second, time.Millisecond) {
		hc := h.health.GetChild("test", "job-reconcile")
		require.NotNil(h.t, hc, "GetChild")
		hc.Lock()
		defer hc.Unlock()
		require.Failf(h.t, "health mismatch", "expected health level %q, status %q, error %q, got: %q, %q, %q", level, statusSubString, errSubString, hc.Level, hc.Status, hc.Error)
	}
}

func (h testHelper) setTargetFaulty(faulty bool) {
	h.ops.faulty.Store(faulty)
}

func (h testHelper) triggerPrune() {
	h.r.Prune()
}

func (h testHelper) waitForReconciliation() {
	err := reconciler.WaitForReconciliation(context.TODO(), h.db, h.tbl, statusIndex)
	require.NoError(h.t, err, "expected WaitForReconciliation to succeed")
}
