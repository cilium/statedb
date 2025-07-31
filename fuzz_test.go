// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package statedb_test

import (
	"flag"
	"fmt"
	"log"
	"maps"
	"math/rand"
	"os"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/cilium/statedb"
	"github.com/cilium/statedb/index"
)

// Run test with "--debug" for log output.
var debug = flag.Bool("debug", false, "Enable debug logging")

type debugLogger struct {
	l *log.Logger
}

func (l *debugLogger) log(fmt string, args ...any) {
	if l == nil {
		return
	}
	l.l.Printf(fmt, args...)
}

func newDebugLogger(worker int) *debugLogger {
	if !*debug {
		return nil
	}
	logger := log.New(os.Stdout, fmt.Sprintf("worker[%03d] | ", worker), 0)
	return &debugLogger{logger}
}

const (
	numUniqueIDs    = 3000
	numUniqueValues = 2000
	numWorkers      = 20
	numTrackers     = 5
	numIterations   = 1000
)

type fuzzObj struct {
	id    string
	value uint64
}

// TableHeader implements statedb.TableWritable.
func (f fuzzObj) TableHeader() []string {
	return []string{
		"id",
		"value",
	}
}

// TableRow implements statedb.TableWritable.
func (f fuzzObj) TableRow() []string {
	return []string{
		f.id,
		fmt.Sprintf("%d", f.value),
	}
}

var _ statedb.TableWritable = fuzzObj{}

func mkID() string {
	// We use a string hex presentation instead of the raw uint64 so we get
	// a wide range of different length keys and different prefixes.
	return fmt.Sprintf("%x", 1+uint64(rand.Int63n(numUniqueIDs)))
}

func mkValue() uint64 {
	return 1 + uint64(rand.Int63n(numUniqueValues))
}

var idIndex = statedb.Index[fuzzObj, string]{
	Name: "id",
	FromObject: func(obj fuzzObj) index.KeySet {
		return index.NewKeySet(index.String(obj.id))
	},
	FromKey: index.String,
	Unique:  true,
}

var valueIndex = statedb.Index[fuzzObj, uint64]{
	Name: "value",
	FromObject: func(obj fuzzObj) index.KeySet {
		return index.NewKeySet(index.Uint64(obj.value))
	},
	FromKey: index.Uint64,
	Unique:  false,
}

var (
	fuzzMetrics = statedb.NewExpVarMetrics(false)
	fuzzDB      = statedb.New(statedb.WithMetrics(fuzzMetrics))
	tableFuzz1  = statedb.MustNewTable(fuzzDB, "fuzz1", idIndex, valueIndex)
	tableFuzz2  = statedb.MustNewTable(fuzzDB, "fuzz2", idIndex, valueIndex)
	tableFuzz3  = statedb.MustNewTable(fuzzDB, "fuzz3", idIndex, valueIndex)
	tableFuzz4  = statedb.MustNewTable(fuzzDB, "fuzz4", idIndex, valueIndex)
	fuzzTables  = []statedb.TableMeta{tableFuzz1, tableFuzz2, tableFuzz3, tableFuzz4}
)

func randomSubset[T any](xs []T) []T {
	xs = slices.Clone(xs)
	rand.Shuffle(len(xs), func(i, j int) {
		xs[i], xs[j] = xs[j], xs[i]
	})
	// Pick random subset
	n := 1 + rand.Intn(len(xs))
	return xs[:n]
}

type actionLog interface {
	append(actionLogEntry)
	validateTable(txn statedb.ReadTxn, table statedb.Table[fuzzObj]) error
}

type realActionLog struct {
	sync.Mutex
	log map[string][]actionLogEntry
}

func (a *realActionLog) append(e actionLogEntry) {
	a.Lock()
	a.log[e.table.Name()] = append(a.log[e.table.Name()], e)
	a.Unlock()
}

func (a *realActionLog) validateTable(txn statedb.ReadTxn, table statedb.Table[fuzzObj]) error {
	a.Lock()
	defer a.Unlock()

	// Collapse the log down to objects that are alive at the end.
	alive := map[string]struct{}{}
	for _, e := range a.log[table.Name()] {
		switch e.act {
		case actInsert:
			alive[e.id] = struct{}{}
		case actDelete:
			delete(alive, e.id)
		case actDeleteAll:
			clear(alive)
		}
	}

	// Since everything was deleted we can clear the log entries for this table now
	a.log[table.Name()] = nil

	actual := map[string]struct{}{}
	for obj := range table.All(txn) {
		actual[obj.id] = struct{}{}
	}
	diff := setSymmetricDifference(actual, alive)
	if len(diff) != 0 {
		return fmt.Errorf("validate failed, mismatching ids: %v", maps.Keys(diff))
	}
	return nil
}

func setSymmetricDifference[T comparable, M map[T]struct{}](s1, s2 M) M {
	counts := make(map[T]int, len(s1)+len(s2))
	for k1 := range s1 {
		counts[k1] = 1
	}
	for k2 := range s2 {
		counts[k2]++
	}
	result := M{}
	for k, count := range counts {
		if count == 1 {
			result[k] = struct{}{}
		}
	}
	return result
}

type nopActionLog struct {
}

func (nopActionLog) append(e actionLogEntry) {}

func (nopActionLog) validateTable(txn statedb.ReadTxn, table statedb.Table[fuzzObj]) error {
	return nil
}

const (
	actInsert = iota
	actDelete
	actDeleteAll
)

type actionLogEntry struct {
	table statedb.Table[fuzzObj]
	act   int
	id    string
	value uint64
}

type tableAndID struct {
	table string
	id    string
}

type txnActionLog struct {
	latest map[tableAndID]actionLogEntry
}

type actionContext struct {
	t      *testing.T
	log    *debugLogger
	actLog actionLog
	txnLog *txnActionLog
	txn    statedb.WriteTxn
	table  statedb.RWTable[fuzzObj]
}

type action func(ctx actionContext)

func insertAction(ctx actionContext) {
	id := mkID()
	value := mkValue()
	ctx.log.log("%s: Insert %s", ctx.table.Name(), id)
	ctx.table.Insert(ctx.txn, fuzzObj{id, value})
	e := actionLogEntry{ctx.table, actInsert, id, value}
	ctx.actLog.append(e)
	ctx.txnLog.latest[tableAndID{ctx.table.Name(), id}] = e
}

func deleteAction(ctx actionContext) {
	id := mkID()
	ctx.log.log("%s: Delete %s", ctx.table.Name(), id)
	ctx.table.Delete(ctx.txn, fuzzObj{id, 0})
	e := actionLogEntry{ctx.table, actDelete, id, 0}
	ctx.actLog.append(e)
	ctx.txnLog.latest[tableAndID{ctx.table.Name(), id}] = e
}

func deleteAllAction(ctx actionContext) {
	ctx.log.log("%s: DeleteAll", ctx.table.Name())

	// Validate the log before objects are wiped.
	if err := ctx.actLog.validateTable(ctx.txn, ctx.table); err != nil {
		panic(err)
	}
	ctx.table.DeleteAll(ctx.txn)
	ctx.actLog.append(actionLogEntry{ctx.table, actDeleteAll, "", 0})
	clear(ctx.txnLog.latest)
}

func deleteManyAction(ctx actionContext) {
	// Delete third of the objects using iteration to test that
	// nothing bad happens when the iterator is used while deleting.
	toDelete := ctx.table.NumObjects(ctx.txn) / 3

	n := 0
	for obj := range ctx.table.All(ctx.txn) {
		ctx.log.log("%s: DeleteMany %s (%d/%d)", ctx.table.Name(), obj.id, n+1, toDelete)
		_, hadOld, _ := ctx.table.Delete(ctx.txn, obj)
		if !hadOld {
			panic("expected Delete of a known object to return the old object")
		}
		e := actionLogEntry{ctx.table, actDelete, obj.id, 0}
		ctx.actLog.append(e)
		ctx.txnLog.latest[tableAndID{ctx.table.Name(), obj.id}] = e

		n++
		if n >= toDelete {
			break
		}
	}
}

func allAction(ctx actionContext) {
	iter := ctx.table.All(ctx.txn)
	ctx.log.log("%s: All => %d found", ctx.table.Name(), len(statedb.Collect(iter)))
}

func listAction(ctx actionContext) {
	value := mkValue()
	values := ctx.table.List(ctx.txn, valueIndex.Query(value))
	ctx.log.log("%s: List(%d)", ctx.table.Name(), value)
	for obj := range values {
		if e, ok2 := ctx.txnLog.latest[tableAndID{ctx.table.Name(), obj.id}]; ok2 {
			if e.act == actInsert {
				if e.value != obj.value {
					panic("List() did not return the last write")
				}
				if obj.value != value {
					panic(fmt.Sprintf("Get() returned object with wrong value, expected %d, got %d", value, obj.value))
				}
			} else if e.act == actDelete {
				panic("List() returned value even though it was deleted")
			}
		}
	}
}

func getAction(ctx actionContext) {
	id := mkID()
	obj, rev, ok := ctx.table.Get(ctx.txn, idIndex.Query(id))

	if e, ok2 := ctx.txnLog.latest[tableAndID{ctx.table.Name(), id}]; ok2 {
		if e.act == actInsert {
			if !ok {
				panic("Get() returned not found, expected last inserted value")
			}
			if e.value != obj.value {
				panic("Get() did not return the last write")
			}
		} else if e.act == actDelete {
			if ok {
				panic("Get() returned value even though it was deleted")
			}
		}
	}
	ctx.log.log("%s: Get(%s) => rev=%d, ok=%v", ctx.table.Name(), id, rev, ok)
}

func lowerboundAction(ctx actionContext) {
	id := mkID()
	iter, _ := ctx.table.LowerBoundWatch(ctx.txn, idIndex.Query(id))
	ctx.log.log("%s: LowerBound(%s) => %d found", ctx.table.Name(), id, len(statedb.Collect(iter)))
}

func prefixAction(ctx actionContext) {
	id := mkID()
	iter, watch := ctx.table.PrefixWatch(ctx.txn, idIndex.Query(id))
	if watch == nil {
		panic("PrefixWatch return nil watch channel")
	}
	ctx.log.log("%s: Prefix(%s) => %d found", ctx.table.Name(), id, len(statedb.Collect(iter)))
}

var actions = []action{
	// Make inserts much more likely than deletions to build up larger tables.
	insertAction, insertAction, insertAction, insertAction, insertAction,
	insertAction, insertAction, insertAction, insertAction, insertAction,
	insertAction, insertAction, insertAction, insertAction, insertAction,
	insertAction, insertAction, insertAction, insertAction, insertAction,
	insertAction, insertAction, insertAction, insertAction, insertAction,
	insertAction, insertAction, insertAction, insertAction, insertAction,
	insertAction, insertAction, insertAction, insertAction, insertAction,
	insertAction, insertAction, insertAction, insertAction, insertAction,
	insertAction, insertAction, insertAction, insertAction, insertAction,
	insertAction, insertAction, insertAction, insertAction, insertAction,
	insertAction, insertAction, insertAction, insertAction, insertAction,
	insertAction, insertAction, insertAction, insertAction, insertAction,
	insertAction, insertAction, insertAction, insertAction, insertAction,
	insertAction, insertAction, insertAction, insertAction, insertAction,
	insertAction, insertAction, insertAction, insertAction, insertAction,
	insertAction, insertAction, insertAction, insertAction, insertAction,
	insertAction, insertAction, insertAction, insertAction, insertAction,
	insertAction, insertAction, insertAction, insertAction, insertAction,
	insertAction, insertAction, insertAction, insertAction, insertAction,
	insertAction, insertAction, insertAction, insertAction, insertAction,
	insertAction, insertAction, insertAction, insertAction, insertAction,
	insertAction, insertAction, insertAction, insertAction, insertAction,
	insertAction, insertAction, insertAction, insertAction, insertAction,

	deleteAction, deleteAction, deleteAction,
	deleteManyAction, deleteAllAction,

	getAction, getAction, getAction, getAction, getAction,
	getAction, getAction, getAction, getAction, getAction,
	getAction, getAction, getAction, getAction, getAction,
	listAction, listAction, listAction, listAction, listAction,
	allAction, allAction,
	lowerboundAction, lowerboundAction, lowerboundAction,
	prefixAction, prefixAction, prefixAction,
}

func randomAction() action {
	return actions[rand.Intn(len(actions))]
}

func trackerWorker(i int, stop <-chan struct{}) {
	log := newDebugLogger(900 + i)
	wtxn := fuzzDB.WriteTxn(tableFuzz1)
	iter, err := tableFuzz1.Changes(wtxn)
	wtxn.Commit()
	if err != nil {
		panic(err)
	}

	// Keep track of what state the changes lead us to in order to validate it.
	state := map[string]*statedb.Change[fuzzObj]{}

	var txn statedb.ReadTxn
	var prevRev statedb.Revision
	for {
		newChanges := false
		txn = fuzzDB.ReadTxn()
		changes, watch := iter.Next(txn)
		for change, rev := range changes {
			newChanges = true
			log.log("%d: %v", rev, change)

			if rev != change.Revision {
				panic(fmt.Sprintf("trackerWorker: event.Revision mismatch with actual revision: %d vs %d", change.Revision, rev))
			}

			if rev <= prevRev {
				panic(fmt.Sprintf("trackerWorker: revisions went backwards %d <= %d: %v", rev, prevRev, change))
			}
			prevRev = rev

			if change.Object.id == "" || change.Object.value == 0 {
				panic("trackerWorker: object with zero id/value")
			}

			if change.Deleted {
				delete(state, change.Object.id)
			} else {
				change := change
				state[change.Object.id] = &change
			}
		}

		if txn != nil && newChanges {
			// Validate that the observed changes match with the database state at this
			// snapshot.
			state2 := maps.Clone(state)
			allObjects := tableFuzz1.LowerBound(txn, statedb.ByRevision[fuzzObj](0))
			for obj, rev := range allObjects {
				change, found := state[obj.id]
				if !found {
					panic(fmt.Sprintf("trackerWorker: object %s not found from state", obj.id))
				}

				if change.Revision != rev {
					panic(fmt.Sprintf("trackerWorker: last observed revision %d does not match real revision %d", change.Revision, rev))
				}

				if change.Object.value != obj.value {
					panic(fmt.Sprintf("trackerWorker: observed value %d does not match real value %d", change.Object.value, obj.value))
				}
				delete(state2, obj.id)
			}

			if len(state2) > 0 {
				for id := range state2 {
					log.log("%s should not exist\n", id)
				}
				panic(fmt.Sprintf("trackerWorker: %d orphan object(s)", len(state2)))
			}
		}

		select {
		case <-watch:
		case <-stop:
			log.log("final object count %d", len(state))
			return
		}
	}
}

func fuzzWorker(realActionLog *realActionLog, worker int, iterations int) {
	log := newDebugLogger(worker)
	for iterations > 0 {
		targets := randomSubset(fuzzTables)
		txn := fuzzDB.WriteTxn(targets[0], targets[1:]...)
		txnActionLog := &txnActionLog{
			latest: map[tableAndID]actionLogEntry{},
		}

		// Try to run other goroutines with write lock held.
		runtime.Gosched()

		var actLog actionLog = realActionLog
		abort := false
		if rand.Intn(10) == 0 {
			abort = true
			actLog = nopActionLog{}
		}

		for _, target := range targets {
			ctx := actionContext{
				log:    log,
				actLog: actLog,
				txnLog: txnActionLog,
				txn:    txn,
				table:  target.(statedb.RWTable[fuzzObj]),
			}
			numActs := rand.Intn(20)
			for range numActs {
				randomAction()(ctx)
				runtime.Gosched()
			}
		}
		runtime.Gosched()

		if abort {
			log.log("Abort")
			txn.Abort()
		} else {
			log.log("Commit")
			txn.Commit()
		}
		iterations--
	}
}

func TestDB_Fuzz(t *testing.T) {
	t.Parallel()

	fuzzDB.Start()
	defer fuzzDB.Stop()

	actionLog := &realActionLog{
		log: map[string][]actionLogEntry{},
	}

	// Start workers to mutate the tables.
	var wg sync.WaitGroup
	wg.Add(numWorkers)
	for i := range numWorkers {
		go func() {
			fuzzWorker(actionLog, i, numIterations)
			wg.Done()
		}()
	}

	// Start change trackers to observe changes.
	stop := make(chan struct{})
	var wg2 sync.WaitGroup
	wg2.Add(numTrackers)
	for i := range numTrackers {
		go func() {
			trackerWorker(i, stop)
			wg2.Done()
		}()
		// Delay a bit to start the trackers at different points in time
		// so they will observe a different starting state.
		time.Sleep(500 * time.Millisecond)
	}

	// Wait until the mutation workers stop and then stop
	// the change observers.
	wg.Wait()
	close(stop)
	wg2.Wait()

	for _, table := range []statedb.Table[fuzzObj]{tableFuzz1, tableFuzz2, tableFuzz3, tableFuzz4} {
		if err := actionLog.validateTable(fuzzDB.ReadTxn(), table); err != nil {
			t.Fatal(err)
		}
	}

	t.Logf("metrics:\n%s\n", fuzzMetrics.String())
}
