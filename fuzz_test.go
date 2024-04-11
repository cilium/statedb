// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package statedb_test

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime"
	"slices"
	"sync"
	"testing"

	"github.com/cilium/statedb"
	"github.com/cilium/statedb/index"
	"golang.org/x/exp/maps"
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
	numUniqueIDs    = 2000
	numUniqueValues = 3000
	numWorkers      = 20
	numIterations   = 1000
)

type fuzzObj struct {
	id    uint64
	value uint64
}

func mkID() uint64 {
	return uint64(rand.Int63n(numUniqueIDs))
}

func mkValue() uint64 {
	return uint64(rand.Int63n(numUniqueValues))
}

var idIndex = statedb.Index[fuzzObj, uint64]{
	Name: "id",
	FromObject: func(obj fuzzObj) index.KeySet {
		return index.NewKeySet(index.Uint64(obj.id))
	},
	FromKey: index.Uint64,
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
	tableFuzz1  = statedb.MustNewTable[fuzzObj]("fuzz1", idIndex, valueIndex)
	tableFuzz2  = statedb.MustNewTable[fuzzObj]("fuzz2", idIndex, valueIndex)
	tableFuzz3  = statedb.MustNewTable[fuzzObj]("fuzz3", idIndex, valueIndex)
	tableFuzz4  = statedb.MustNewTable[fuzzObj]("fuzz4", idIndex, valueIndex)
	fuzzTables  = []statedb.TableMeta{tableFuzz1, tableFuzz2, tableFuzz3, tableFuzz4}
	fuzzMetrics = statedb.NewExpVarMetrics(false)
	fuzzDB      *statedb.DB
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
	alive := map[uint64]struct{}{}
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

	iter, _ := table.All(txn)
	actual := map[uint64]struct{}{}
	for obj, _, ok := iter.Next(); ok; obj, _, ok = iter.Next() {
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
	id    uint64
	value uint64
}

type tableAndID struct {
	table string
	id    uint64
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
	ctx.log.log("%s: Insert %d", ctx.table.Name(), id)
	ctx.table.Insert(ctx.txn, fuzzObj{id, value})
	e := actionLogEntry{ctx.table, actInsert, id, value}
	ctx.actLog.append(e)
	ctx.txnLog.latest[tableAndID{ctx.table.Name(), id}] = e
}

func deleteAction(ctx actionContext) {
	id := mkID()
	ctx.log.log("%s: Delete %d", ctx.table.Name(), id)
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
	ctx.actLog.append(actionLogEntry{ctx.table, actDeleteAll, 0, 0})
	clear(ctx.txnLog.latest)
}

func deleteManyAction(ctx actionContext) {
	// Delete third of the objects using iteration to test that
	// nothing bad happens when the iterator is used while deleting.
	toDelete := ctx.table.NumObjects(ctx.txn) / 3

	iter, _ := ctx.table.All(ctx.txn)
	n := 0
	for obj, _, ok := iter.Next(); ok; obj, _, ok = iter.Next() {
		ctx.log.log("%s: DeleteMany %d (%d/%d)", ctx.table.Name(), obj.id, n+1, toDelete)
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
	iter, _ := ctx.table.All(ctx.txn)
	ctx.log.log("%s: All => %d found", ctx.table.Name(), len(statedb.Collect(iter)))
}

func getAction(ctx actionContext) {
	value := mkValue()
	iter, _ := ctx.table.Get(ctx.txn, valueIndex.Query(value))
	ctx.log.log("%s: Get(%d)", ctx.table.Name(), value)
	for obj, _, ok := iter.Next(); ok; obj, _, ok = iter.Next() {
		if e, ok2 := ctx.txnLog.latest[tableAndID{ctx.table.Name(), obj.id}]; ok2 {
			if e.act == actInsert {
				if !ok {
					panic("Get() returned not found, expected last inserted value")
				}
				if e.value != obj.value {
					panic("Get() did not return the last write")
				}
				if obj.value != value {
					panic(fmt.Sprintf("Get() returned object with wrong value, expected %d, got %d", value, obj.value))
				}
			} else if e.act == actDelete {
				if ok {
					panic("Get() returned value even though it was deleted")
				}
			}
		}
	}
}

func firstAction(ctx actionContext) {
	id := mkID()
	obj, rev, ok := ctx.table.First(ctx.txn, idIndex.Query(id))

	if e, ok2 := ctx.txnLog.latest[tableAndID{ctx.table.Name(), id}]; ok2 {
		if e.act == actInsert {
			if !ok {
				panic("First() returned not found, expected last inserted value")
			}
			if e.value != obj.value {
				panic("First() did not return the last write")
			}
		} else if e.act == actDelete {
			if ok {
				panic("First() returned value even though it was deleted")
			}
		}
	}
	ctx.log.log("%s: First(%d) => rev=%d, ok=%v", ctx.table.Name(), id, rev, ok)
}

func lowerboundAction(ctx actionContext) {
	id := mkID()
	iter, _ := ctx.table.LowerBound(ctx.txn, idIndex.Query(id))
	ctx.log.log("%s: LowerBound(%d) => %d found", ctx.table.Name(), id, len(statedb.Collect(iter)))
}

func prefixAction(ctx actionContext) {
	id := mkID()
	iter, _ := ctx.table.Prefix(ctx.txn, idIndex.Query(id))
	ctx.log.log("%s: Prefix(%d) => %d found", ctx.table.Name(), id, len(statedb.Collect(iter)))
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

	firstAction, firstAction, firstAction, firstAction, firstAction,
	firstAction, firstAction, firstAction, firstAction, firstAction,
	firstAction, firstAction, firstAction, firstAction, firstAction,
	getAction, getAction, getAction, getAction, getAction,
	allAction, allAction,
	lowerboundAction, lowerboundAction, lowerboundAction,
	prefixAction, prefixAction, prefixAction,
}

func randomAction() action {
	return actions[rand.Intn(len(actions))]
}

func trackerWorker(stop <-chan struct{}) {
	txn := fuzzDB.WriteTxn(tableFuzz1)
	dt, err := tableFuzz1.DeleteTracker(txn, "tracker")
	if err != nil {
		panic(err)
	}
	txn.Commit()
	defer dt.Close()

	for {
		watch := dt.Iterate(
			fuzzDB.ReadTxn(),
			func(obj fuzzObj, deleted bool, rev uint64) {
			},
		)
		select {
		case <-watch:
		case <-stop:
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
			for i := 0; i < numActs; i++ {
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

	fuzzDB, _ = statedb.NewDB(fuzzTables, fuzzMetrics)

	fuzzDB.Start(context.TODO())
	defer fuzzDB.Stop(context.TODO())

	actionLog := &realActionLog{
		log: map[string][]actionLogEntry{},
	}

	var wg sync.WaitGroup
	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		i := i
		go func() {
			fuzzWorker(actionLog, i, numIterations)
			wg.Done()
		}()
	}
	stop := make(chan struct{})
	var wg2 sync.WaitGroup
	wg2.Add(1)
	go func() {
		trackerWorker(stop)
		wg2.Done()
	}()
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
