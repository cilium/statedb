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

	var (
		ops = &mockOps{}
		db  *statedb.DB
		r   reconciler.Reconciler[*testObject]

		fakeHealth *cell.SimpleHealth
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

			cell.Provide(func(db_ *statedb.DB) (statedb.RWTable[*testObject], error) {
				db = db_
				return testObjects, db.RegisterTable(testObjects)
			}),
			cell.Provide(func() reconciler.Config[*testObject] {
				cfg := reconciler.Config[*testObject]{
					// Don't run the full reconciliation via timer, but rather explicitly so that the full
					// reconciliation operations don't mix with incremental when not expected.
					FullReconcilationInterval: time.Hour,

					RetryBackoffMinDuration: time.Millisecond,
					RetryBackoffMaxDuration: 10 * time.Millisecond,
					IncrementalRoundSize:    1000,
					GetObjectStatus:         (*testObject).GetStatus,
					SetObjectStatus:         (*testObject).SetStatus,
					CloneObject:             (*testObject).Clone,
					Operations:              ops,
				}
				if batchOps {
					cfg.BatchOperations = ops
				}
				return cfg
			}),
			cell.Provide(reconciler.New[*testObject]),

			cell.Invoke(func(r_ reconciler.Reconciler[*testObject], h *cell.SimpleHealth) {
				r = r_
				fakeHealth = h
			}),
		),
	)

	require.NoError(t, hive.Start(context.TODO()), "Start")

	h := testHelper{
		t:      t,
		db:     db,
		tbl:    testObjects,
		ops:    ops,
		r:      r,
		health: fakeHealth,
	}

	numIterations := 3

	t.Run("incremental", func(t *testing.T) {
		h.t = t

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

			h.expectHealthLevel(cell.StatusOK)
			h.waitForReconciliation()

			// Set one to be faulty => object will error
			t.Log("Setting '1' faulty")
			h.insert(ID_1, Faulty, reconciler.StatusPending())
			h.expectOp(opFail(opUpdate(ID_1)))
			h.expectStatus(ID_1, reconciler.StatusKindError, "update fail")
			h.expectRetried(ID_1)
			h.expectHealthLevel(cell.StatusDegraded)

			// Fix the object => object will reconcile again.
			t.Log("Setting '1' non-faulty")
			h.insert(ID_1, NonFaulty, reconciler.StatusPending())
			h.expectOp(opUpdate(ID_1))
			h.expectStatus(ID_1, reconciler.StatusKindDone, "")
			h.expectHealthLevel(cell.StatusOK)

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
			h.expectStatus(ID_3, reconciler.StatusKindError, "delete fail")
			h.expectHealthLevel(cell.StatusDegraded)

			t.Log("Delete 3")
			h.setTargetFaulty(false)
			h.expectOp(opDelete(3))
			h.expectNotFound(ID_3)
			h.expectHealthLevel(cell.StatusOK)

			h.waitForReconciliation()

		}
	})

	t.Run("full", func(t *testing.T) {
		h.t = t

		for i := 0; i < numIterations; i++ {
			t.Logf("Iteration %d", i)

			// Without any objects, we should only see prune.
			t.Log("Full reconciliation without objects")
			h.triggerFullReconciliation()
			h.expectOp(opPrune(0))
			h.expectHealthLevel(cell.StatusOK)

			// Register a table initializer to prohibit pruning.
			markInitialized := h.registerInitializer()

			// With table not initialized, we should not see the prune.
			h.insert(ID_1, NonFaulty, reconciler.StatusPending())
			h.triggerFullReconciliation()
			h.expectOp(opUpdate(ID_1))
			markInitialized()

			// Add few objects and wait until incremental reconciliation is done.
			t.Log("Insert test objects")
			h.insert(ID_1, NonFaulty, reconciler.StatusPending())
			h.insert(ID_2, NonFaulty, reconciler.StatusPending())
			h.insert(ID_3, NonFaulty, reconciler.StatusPending())
			h.expectStatus(ID_1, reconciler.StatusKindDone, "")
			h.expectStatus(ID_2, reconciler.StatusKindDone, "")
			h.expectStatus(ID_3, reconciler.StatusKindDone, "")
			h.expectNumUpdates(ID_1, 1)
			h.expectNumUpdates(ID_2, 1)
			h.expectNumUpdates(ID_3, 1)
			h.expectHealthLevel(cell.StatusOK)

			// Full reconciliation with functioning ops.
			t.Log("Full reconciliation with non-faulty ops")
			h.triggerFullReconciliation()
			h.expectOps(opPrune(3), opUpdate(ID_1), opUpdate(ID_2), opUpdate(ID_3))
			h.expectStatus(ID_1, reconciler.StatusKindDone, "")
			h.expectStatus(ID_2, reconciler.StatusKindDone, "")
			h.expectStatus(ID_3, reconciler.StatusKindDone, "")
			h.expectNumUpdates(ID_1, 2)
			h.expectNumUpdates(ID_2, 2)
			h.expectNumUpdates(ID_3, 2)
			h.expectHealthLevel(cell.StatusOK)

			// Make the ops faulty and trigger the full reconciliation.
			t.Log("Full reconciliation with faulty ops")
			h.setTargetFaulty(true)
			h.triggerFullReconciliation()
			h.expectOps(
				opFail(opUpdate(ID_1)),
				opFail(opUpdate(ID_2)),
				opFail(opUpdate(ID_3)),
			)
			h.expectHealthLevel(cell.StatusDegraded)

			// Expect the objects to be retried also after the full reconciliation.
			h.expectRetried(ID_1)
			h.expectRetried(ID_2)
			h.expectRetried(ID_3)

			// All should be marked as errored.
			h.expectStatus(ID_1, reconciler.StatusKindError, "update fail")
			h.expectStatus(ID_2, reconciler.StatusKindError, "update fail")
			h.expectStatus(ID_3, reconciler.StatusKindError, "update fail")

			// Make the ops healthy again and check that the objects recover.
			t.Log("Retries succeed after ops is non-faulty")
			h.setTargetFaulty(false)
			h.expectOps(opUpdate(ID_1), opUpdate(ID_2), opUpdate(ID_3))
			h.expectStatus(ID_1, reconciler.StatusKindDone, "")
			h.expectStatus(ID_2, reconciler.StatusKindDone, "")
			h.expectStatus(ID_3, reconciler.StatusKindDone, "")
			h.expectHealthLevel(cell.StatusOK)

			// Cleanup.
			h.markForDelete(ID_1)
			h.markForDelete(ID_2)
			h.markForDelete(ID_3)
			h.expectNotFound(ID_1)
			h.expectNotFound(ID_2)
			h.expectNotFound(ID_3)
			h.triggerFullReconciliation()
			h.expectOps(opDelete(1), opDelete(2), opDelete(3), opPrune(0))
			h.waitForReconciliation()

		}
	})

	// Validate that the metrics are populated and make some sense.
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

	assert.Greater(t, getInt(expVarMetrics.FullReconciliationCountVar.Get("test")), int64(0), "FullReconciliationCount")
	assert.Greater(t, getInt(expVarMetrics.FullReconciliationOutOfSyncCountVar.Get("test")), int64(0), "FullReconciliationOutOfSyncCount")
	assert.Greater(t, getFloat(expVarMetrics.FullReconciliationDurationVar.Get("test/prune")), float64(0), "FullReconciliationDuration/prune")
	assert.Greater(t, getFloat(expVarMetrics.FullReconciliationDurationVar.Get("test/update")), float64(0), "FullReconciliationDuration/update")
	assert.Equal(t, getInt(expVarMetrics.FullReconciliationCurrentErrorsVar.Get("test")), int64(0), "FullReconciliationCurrentErrors")

	assert.Greater(t, getInt(expVarMetrics.IncrementalReconciliationCountVar.Get("test")), int64(0), "IncrementalReconciliationCount")
	assert.Greater(t, getFloat(expVarMetrics.IncrementalReconciliationDurationVar.Get("test/update")), float64(0), "IncrementalReconciliationDuration/update")
	assert.Greater(t, getFloat(expVarMetrics.IncrementalReconciliationDurationVar.Get("test/delete")), float64(0), "IncrementalReconciliationDuration/delete")
	assert.Greater(t, getInt(expVarMetrics.IncrementalReconciliationTotalErrorsVar.Get("test")), int64(0), "IncrementalReconciliationTotalErrors")
	assert.Equal(t, getInt(expVarMetrics.IncrementalReconciliationCurrentErrorsVar.Get("test")), int64(0), "IncrementalReconciliationCurrentErrors")

	assert.NoError(t, hive.Stop(context.TODO()), "Stop")
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

var statusIndex = reconciler.NewStatusIndex[*testObject]((*testObject).GetStatus)

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
		batch[i].Result = mt.Update(ctx, txn, batch[i].Object, nil)
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
	objs := statedb.Collect(iter)
	mt.history.add(opPrune(len(objs)))
	return nil
}

// Update implements reconciler.Operations.
func (mt *mockOps) Update(ctx context.Context, txn statedb.ReadTxn, obj *testObject, changed *bool) error {
	if changed != nil {
		*changed = true
	}
	mt.updates.incr(obj.id)
	if mt.faulty.Load() || obj.faulty {
		mt.history.add(opFail(opUpdate(obj.id)))
		return errors.New("update fail")
	}
	mt.history.add(opUpdate(obj.id))

	obj.updates += 1

	return nil
}

var _ reconciler.Operations[*testObject] = &mockOps{}
var _ reconciler.BatchOperations[*testObject] = &mockOps{}

// testHelper defines a sort of mini-language for writing the test steps.
type testHelper struct {
	t      testing.TB
	db     *statedb.DB
	tbl    statedb.RWTable[*testObject]
	ops    *mockOps
	r      reconciler.Reconciler[*testObject]
	health *cell.SimpleHealth
}

const (
	Faulty    = true
	NonFaulty = false
)

func (h testHelper) registerInitializer() func() {
	wtxn := h.db.WriteTxn(h.tbl)
	done := h.tbl.RegisterInitializer(wtxn)
	wtxn.Commit()
	return func() {
		wtxn := h.db.WriteTxn(h.tbl)
		done(wtxn)
		wtxn.Commit()
	}
}

func (h testHelper) insert(id uint64, faulty bool, status reconciler.Status) {
	wtxn := h.db.WriteTxn(h.tbl)
	_, _, err := h.tbl.Insert(wtxn, &testObject{
		id:     id,
		faulty: faulty,
		status: status,
	})
	require.NoError(h.t, err, "insert failed")
	wtxn.Commit()
}

func (h testHelper) markForDelete(id uint64) {
	wtxn := h.db.WriteTxn(h.tbl)
	_, _, err := h.tbl.Insert(wtxn, &testObject{
		id:     id,
		faulty: false,
		status: reconciler.StatusPendingDelete(),
	})
	require.NoError(h.t, err, "delete failed")
	wtxn.Commit()
}

func (h testHelper) expectStatus(id uint64, kind reconciler.StatusKind, err string) {
	cond := func() bool {
		obj, _, ok := h.tbl.First(h.db.ReadTxn(), idIndex.Query(id))
		return ok && obj.status.Kind == kind && obj.status.Error == err
	}
	if !assert.Eventually(h.t, cond, time.Second, time.Millisecond) {
		actual := "<not found>"
		obj, _, ok := h.tbl.First(h.db.ReadTxn(), idIndex.Query(id))
		if ok {
			actual = string(obj.status.Kind)
		}
		require.Failf(h.t, "status mismatch", "expected object %d to be marked with status %q, but it was %q",
			id, kind, actual)

	}
}

func (h testHelper) expectNumUpdates(id uint64, n int) {
	cond := func() bool {
		obj, _, ok := h.tbl.First(h.db.ReadTxn(), idIndex.Query(id))
		return ok && obj.updates == n
	}
	if !assert.Eventually(h.t, cond, time.Second, time.Millisecond) {
		actual := "<not found>"
		obj, _, ok := h.tbl.First(h.db.ReadTxn(), idIndex.Query(id))
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
		_, _, ok := h.tbl.First(h.db.ReadTxn(), idIndex.Query(id))
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

func (h testHelper) expectHealthLevel(level cell.Level) {
	h.t.Helper()
	cond := func() bool {
		h := h.health.GetChild("test", "job-reconciler-loop")
		if h == nil {
			return false
		}
		h.Lock()
		defer h.Unlock()
		return level == h.Level
	}
	if !assert.Eventually(h.t, cond, time.Second, time.Millisecond) {
		require.Failf(h.t, "health mismatch", "expected health level %q, got: %q (%s)", level, h.health.Level, h.health.Status)
	}
}

func (h testHelper) setTargetFaulty(faulty bool) {
	h.ops.faulty.Store(faulty)
}

func (h testHelper) triggerFullReconciliation() {
	h.r.TriggerFullReconciliation()
}

func (h testHelper) waitForReconciliation() {
	err := reconciler.WaitForReconciliation[*testObject](context.TODO(), h.db, h.tbl, statusIndex)
	require.NoError(h.t, err, "expected WaitForReconciliation to succeed")
}

func concatLabels(m map[string]string) string {
	labels := []string{}
	for k, v := range m {
		labels = append(labels, k+"="+v)
	}
	return strings.Join(labels, ",")
}
