package reconciler_test

import (
	"context"
	"errors"
	"expvar"
	"fmt"
	"iter"
	"maps"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cilium/hive"
	"github.com/cilium/hive/cell"
	"github.com/cilium/hive/hivetest"
	"github.com/cilium/hive/job"
	"github.com/cilium/hive/script"
	"github.com/cilium/hive/script/scripttest"
	"github.com/cilium/statedb"
	"github.com/cilium/statedb/index"
	"github.com/cilium/statedb/reconciler"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

func newEngine(t testing.TB, args []string) *script.Engine {
	log := hivetest.Logger(t)

	var (
		ops                 = &mockOps{}
		db                  *statedb.DB
		r                   reconciler.Reconciler[*testObject]
		reconcilerParams    reconciler.Params
		reconcilerLifecycle = &cell.DefaultLifecycle{}
		markInit            func()
	)

	expVarMetrics := reconciler.NewUnpublishedExpVarMetrics()

	testObjects, err := statedb.NewTable("test-objects", idIndex)
	require.NoError(t, err, "NewTable")

	hive := hive.New(
		statedb.Cell,
		job.Cell,

		cell.Provide(
			cell.NewSimpleHealth,
			func(h *cell.SimpleHealth) hive.ScriptCmdOut {
				return hive.NewScriptCmd(
					"health",
					cell.SimpleHealthCmd(h))
			},
		),

		cell.Module(
			"test",
			"Test",

			cell.Provide(
				func() reconciler.Metrics {
					return expVarMetrics
				}),

			cell.Invoke(
				func(db_ *statedb.DB, p_ reconciler.Params) error {
					db = db_
					reconcilerParams = p_
					return db.RegisterTable(testObjects)
				},

				func(lc cell.Lifecycle) {
					lc.Append(cell.Hook{
						OnStop: func(ctx cell.HookContext) error { return reconcilerLifecycle.Stop(log, ctx) },
					})
				},

				func(h *cell.SimpleHealth) {
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

	cmds, err := hive.ScriptCommands(log)
	require.NoError(t, err)

	cmds["mark-init"] = script.Command(
		script.CmdUsage{Summary: "Mark table as initialized"},
		func(s *script.State, args ...string) (script.WaitFunc, error) {
			markInit()
			return nil, nil
		},
	)

	cmds["start-reconciler"] = script.Command(
		script.CmdUsage{Summary: "Start the reconciler"},
		func(s *script.State, args ...string) (script.WaitFunc, error) {
			opts := []reconciler.Option{
				// Speed things up a bit. Quick retry interval does mean we can't
				// assert the metrics exactly (e.g. error count depends on how
				// many retries happened).
				reconciler.WithRetry(50*time.Millisecond, 50*time.Millisecond),
				reconciler.WithRoundLimits(1000, rate.NewLimiter(1000.0, 10)),
			}
			var bops reconciler.BatchOperations[*testObject]
			for _, arg := range args {
				switch arg {
				case "with-prune":
					opts = append(opts, reconciler.WithPruning(time.Hour))
				case "with-refresh":
					opts = append(opts, reconciler.WithRefreshing(50*time.Millisecond, rate.NewLimiter(100.0, 1)))
				case "with-batchops":
					bops = ops
				default:
					return nil, fmt.Errorf("unexpected arg, expected 'with-prune', 'with-batchops' or 'with-refresh'")
				}
			}
			reconcilerParams.Lifecycle = reconcilerLifecycle
			r, err = reconciler.Register(
				reconcilerParams,
				testObjects,
				(*testObject).Clone,
				(*testObject).SetStatus,
				(*testObject).GetStatus,
				ops,
				bops,
				opts...)
			if err != nil {
				return nil, err
			}
			return nil, reconcilerLifecycle.Start(log, context.TODO())
		},
	)

	cmds["prune"] = script.Command(
		script.CmdUsage{Summary: "Trigger pruning"},
		func(s *script.State, args ...string) (script.WaitFunc, error) {
			r.Prune()
			return nil, nil
		},
	)

	cmds["set-faulty"] = script.Command(
		script.CmdUsage{Summary: "Mark target faulty or not"},
		func(s *script.State, args ...string) (script.WaitFunc, error) {
			if args[0] == "true" {
				t.Logf("Marked target faulty")
				ops.faulty.Store(true)
			} else {
				t.Logf("Marked target healthy")
				ops.faulty.Store(false)
			}
			return nil, nil
		},
	)

	cmds["expect-ops"] = script.Command(
		script.CmdUsage{Summary: "Assert ops"},
		func(s *script.State, args ...string) (script.WaitFunc, error) {
			sort.Strings(args)
			var actual []string
			cond := func() bool {
				actual = ops.history.take(len(args))
				sort.Strings(actual)
				return slices.Equal(args, actual)
			}
			for s.Context().Err() == nil {
				if cond() {
					return nil, nil
				}
			}
			return nil, fmt.Errorf("operations mismatch, expected %v, got %v", args, actual)
		},
	)

	cmds["expvar"] = script.Command(
		script.CmdUsage{Summary: "Print expvars to stdout"},
		func(s *script.State, args ...string) (script.WaitFunc, error) {
			return func(*script.State) (stdout, stderr string, err error) {
				var buf strings.Builder
				expVarMetrics.Map().Do(func(kv expvar.KeyValue) {
					switch v := kv.Value.(type) {
					case expvar.Func:
						// skip
					case *expvar.Map:
						v.Do(func(kv2 expvar.KeyValue) {
							fmt.Fprintf(&buf, "%s.%s: %s\n", kv.Key, kv2.Key, kv2.Value)
						})
					default:
						fmt.Fprintf(&buf, "%s: %s\n", kv.Key, kv.Value)
					}
				})
				return buf.String(), "", nil
			}, nil
		},
	)

	require.NoError(t, err, "ScriptCommands")
	maps.Insert(cmds, maps.All(script.DefaultCmds()))

	t.Cleanup(func() {
		assert.NoError(t, hive.Stop(log, context.TODO()))
	})

	return &script.Engine{
		Cmds: cmds,
	}
}

func TestScript(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	scripttest.Test(t,
		ctx, newEngine,
		[]string{},
		"testdata/*.txtar")
}

type testObject struct {
	ID      uint64
	Faulty  bool
	Updates int
	Status  reconciler.Status
}

var idIndex = statedb.Index[*testObject, uint64]{
	Name: "id",
	FromObject: func(t *testObject) index.KeySet {
		return index.NewKeySet(index.Uint64(t.ID))
	},
	FromKey: index.Uint64,
	Unique:  true,
}

func (t *testObject) GetStatus() reconciler.Status {
	return t.Status
}

func (t *testObject) SetStatus(status reconciler.Status) *testObject {
	t.Status = status
	return t
}

func (t *testObject) Clone() *testObject {
	t2 := *t
	return &t2
}

func (t *testObject) TableHeader() []string {
	return []string{
		"ID",
		"Faulty",
		"StatusKind",
		"StatusError",
	}
}

func (t *testObject) TableRow() []string {
	return []string{
		strconv.FormatUint(t.ID, 10),
		strconv.FormatBool(t.Faulty),
		string(t.Status.Kind),
		t.Status.Error,
	}
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

type mockOps struct {
	history opHistory
	faulty  atomic.Bool
	updates intMap
}

// DeleteBatch implements reconciler.BatchOperations.
func (mt *mockOps) DeleteBatch(ctx context.Context, txn statedb.ReadTxn, batch []reconciler.BatchEntry[*testObject]) {
	for i := range batch {
		batch[i].Result = mt.Delete(ctx, txn, batch[i].Revision, batch[i].Object)
	}
}

// UpdateBatch implements reconciler.BatchOperations.
func (mt *mockOps) UpdateBatch(ctx context.Context, txn statedb.ReadTxn, batch []reconciler.BatchEntry[*testObject]) {
	for i := range batch {
		batch[i].Result = mt.Update(ctx, txn, batch[i].Revision, batch[i].Object)
	}
}

// Delete implements reconciler.Operations.
func (mt *mockOps) Delete(ctx context.Context, txn statedb.ReadTxn, rev statedb.Revision, obj *testObject) error {
	if rev == 0 {
		panic("BUG: revision must not be 0")
	}

	if mt.faulty.Load() || obj.Faulty {
		mt.history.add(opFail(opDelete(obj.ID)))
		return errors.New("delete fail")
	}
	mt.history.add(opDelete(obj.ID))

	return nil
}

// Prune implements reconciler.Operations.
func (mt *mockOps) Prune(ctx context.Context, txn statedb.ReadTxn, objects iter.Seq2[*testObject, statedb.Revision]) error {
	objs := statedb.Collect(objects)
	if mt.faulty.Load() {
		mt.history.add(opFail(opPrune(len(objs))))
		return errors.New("prune fail")
	}
	mt.history.add(opPrune(len(objs)))
	return nil
}

// Update implements reconciler.Operations.
func (mt *mockOps) Update(ctx context.Context, txn statedb.ReadTxn, rev statedb.Revision, obj *testObject) error {
	if rev == 0 {
		panic("BUG: revision must not be 0")
	}
	mt.updates.incr(obj.ID)

	op := opUpdate(obj.ID)
	if obj.Status.Kind == reconciler.StatusKindRefreshing {
		op = opUpdateRefresh(obj.ID)
	}
	if mt.faulty.Load() || obj.Faulty {
		mt.history.add(opFail(op))
		return errors.New("update fail")
	}
	mt.history.add(op)
	obj.Updates += 1

	return nil
}

var _ reconciler.Operations[*testObject] = &mockOps{}
var _ reconciler.BatchOperations[*testObject] = &mockOps{}
