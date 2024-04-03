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

	"github.com/stretchr/testify/require"

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
	numUniqueIDs  = 20
	numWorkers    = 50
	numIterations = 1000
)

type fuzzObj struct {
	id    uint64
	value uint64
}

func mkID() uint64 {
	return uint64(rand.Int63n(numUniqueIDs))
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
	tableFuzz1  = statedb.MustNewTable[fuzzObj]("fuzz1", idIndex)
	tableFuzz2  = statedb.MustNewTable[fuzzObj]("fuzz2", idIndex)
	tableFuzz3  = statedb.MustNewTable[fuzzObj]("fuzz3", idIndex, valueIndex)
	tableFuzz4  = statedb.MustNewTable[fuzzObj]("fuzz4", idIndex, valueIndex)
	fuzzTables  = []statedb.TableMeta{tableFuzz1, tableFuzz2, tableFuzz3, tableFuzz4}
	fuzzMetrics = statedb.NewExpVarMetrics(false)
	fuzzDB, _   = statedb.NewDB(fuzzTables, fuzzMetrics)
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
}

type realActionLog struct {
	sync.Mutex
	log []actionLogEntry
}

func (a *realActionLog) append(e actionLogEntry) {
	a.Lock()
	a.log = append(a.log, e)
	a.Unlock()
}

func (a *realActionLog) validate(db *statedb.DB, t *testing.T) {
	a.Lock()
	defer a.Unlock()

	// Collapse the log down to objects that are alive at the end.
	alive := map[statedb.Table[fuzzObj]]map[uint64]struct{}{}
	for _, e := range a.log {
		aliveThis, ok := alive[e.table]
		if !ok {
			aliveThis = map[uint64]struct{}{}
			alive[e.table] = aliveThis
		}
		switch e.act {
		case actInsert:
			aliveThis[e.id] = struct{}{}
		case actDelete:
			delete(aliveThis, e.id)
		case actDeleteAll:
			clear(aliveThis)
		}
	}

	for table, expected := range alive {
		txn := db.ReadTxn()
		iter, _ := table.All(txn)
		actual := map[uint64]struct{}{}
		for obj, _, ok := iter.Next(); ok; obj, _, ok = iter.Next() {
			actual[obj.id] = struct{}{}
		}
		require.Equal(t, expected, actual, "validate failed, mismatching ids: %v",
			setSymmetricDifference(actual, expected))
	}
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
	log    *debugLogger
	actLog actionLog
	txnLog *txnActionLog
	txn    statedb.WriteTxn
	table  statedb.RWTable[fuzzObj]
}

type action func(ctx actionContext)

func insertAction(ctx actionContext) {
	id := mkID()
	value := rand.Uint64()
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
	ctx.table.DeleteAll(ctx.txn)
	ctx.actLog.append(actionLogEntry{ctx.table, actDeleteAll, 0, 0})
	clear(ctx.txnLog.latest)
}

func allAction(ctx actionContext) {
	iter, _ := ctx.table.All(ctx.txn)
	ctx.log.log("%s: All => %d found", ctx.table.Name(), len(statedb.Collect(iter)))
}

func getAction(ctx actionContext) {
	id := mkID()
	iter, _ := ctx.table.Get(ctx.txn, idIndex.Query(mkID()))
	ctx.log.log("%s: Get(%d) => %d found", ctx.table.Name(), id, len(statedb.Collect(iter)))
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

func lastAction(ctx actionContext) {
	id := mkID()
	_, rev, ok := ctx.table.Last(ctx.txn, idIndex.Query(id))
	ctx.log.log("%s: Last(%d) => rev=%d, ok=%v", ctx.table.Name(), id, rev, ok)
}

func lowerboundAction(ctx actionContext) {
	id := mkID()
	iter, _ := ctx.table.LowerBound(ctx.txn, idIndex.Query(id))
	ctx.log.log("%s: LowerBound(%d) => %d found", ctx.table.Name(), id, len(statedb.Collect(iter)))
}

var actions = []action{
	insertAction, insertAction, insertAction, insertAction, insertAction,
	insertAction, insertAction, insertAction, insertAction, insertAction,
	insertAction, insertAction, insertAction, insertAction, insertAction,

	deleteAction, deleteAction, deleteAction,

	deleteAllAction,

	firstAction, firstAction, firstAction, firstAction, firstAction,
	allAction, lowerboundAction,
	getAction, getAction, getAction,
	lastAction, lastAction,
}

func randomAction() action {
	return actions[rand.Intn(len(actions))]
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

	fuzzDB.Start(context.TODO())
	defer fuzzDB.Stop(context.TODO())

	var actionLog realActionLog
	var wg sync.WaitGroup
	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		i := i
		go func() {
			fuzzWorker(&actionLog, i, numIterations)
			wg.Done()
		}()
	}
	wg.Wait()
	actionLog.validate(fuzzDB, t)

	t.Logf("metrics:\n%s\n", fuzzMetrics.String())
}
