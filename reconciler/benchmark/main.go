// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"runtime"
	"runtime/pprof"
	"sync/atomic"
	"time"

	"github.com/cilium/hive"
	"github.com/cilium/hive/cell"
	"github.com/cilium/hive/job"
	"github.com/cilium/statedb"
	"github.com/cilium/statedb/index"
	"github.com/cilium/statedb/reconciler"
)

var logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
	Level: slog.LevelError,
}))

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")
var memprofile = flag.String("memprofile", "", "write memory profile to `file`")
var numObjects = flag.Int("objects", 1000000, "number of objects to create")
var batchSize = flag.Int("batchsize", 1000, "batch size for writes")
var incrBatchSize = flag.Int("incrbatchsize", 1000, "maximum batch size for incremental reconciliation")
var quiet = flag.Bool("quiet", false, "quiet output for CI")

type testObject struct {
	id     uint64
	status reconciler.Status
}

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

type mockOps struct {
	numUpdates atomic.Int32
}

// Delete implements reconciler.Operations.
func (mt *mockOps) Delete(ctx context.Context, txn statedb.ReadTxn, obj *testObject) error {
	return nil
}

// Prune implements reconciler.Operations.
func (mt *mockOps) Prune(ctx context.Context, txn statedb.ReadTxn, iter statedb.Iterator[*testObject]) error {
	return nil
}

// Update implements reconciler.Operations.
func (mt *mockOps) Update(ctx context.Context, txn statedb.ReadTxn, obj *testObject, changed *bool) error {
	mt.numUpdates.Add(1)
	if changed != nil {
		*changed = true
	}
	return nil
}

var _ reconciler.Operations[*testObject] = &mockOps{}

var idIndex = statedb.Index[*testObject, uint64]{
	Name: "id",
	FromObject: func(t *testObject) index.KeySet {
		return index.NewKeySet(index.Uint64(t.id))
	},
	FromKey: index.Uint64,
	Unique:  true,
}

func main() {
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	var (
		mt = &mockOps{}
		db *statedb.DB
	)

	testObjects, err := statedb.NewTable[*testObject]("test-objects", idIndex)
	if err != nil {
		panic(err)
	}

	hive := hive.NewWithOptions(
		hive.Options{Logger: logger},

		cell.SimpleHealthCell,
		statedb.Cell,
		job.Cell,

		cell.Module(
			"test",
			"Test",

			cell.Invoke(func(db_ *statedb.DB) error {
				db = db_
				return db.RegisterTable(testObjects)
			}),
			cell.Provide(
				func() (*mockOps, reconciler.Operations[*testObject]) {
					return mt, mt
				},
			),
			cell.Provide(func() reconciler.Config[*testObject] {
				return reconciler.Config[*testObject]{
					Table: testObjects,

					// Don't run the full reconciliation via timer, but rather explicitly so that the full
					// reconciliation operations don't mix with incremental when not expected.
					FullReconcilationInterval: time.Hour,

					RetryBackoffMinDuration: time.Millisecond,
					RetryBackoffMaxDuration: 10 * time.Millisecond,
					IncrementalRoundSize:    *incrBatchSize,
					GetObjectStatus:         (*testObject).GetStatus,
					SetObjectStatus:         (*testObject).SetStatus,
					CloneObject:             (*testObject).Clone,
					Operations:              mt,
				}
			}),
			cell.Invoke(reconciler.Register[*testObject]),
		),
	)

	err = hive.Start(context.TODO())
	if err != nil {
		panic(err)
	}

	start := time.Now()

	// Create objects in batches to allow the reconciler to start working
	// on them while they're added.
	id := uint64(0)
	batches := int(*numObjects / *batchSize)
	for b := 0; b < batches; b++ {
		if !*quiet {
			fmt.Printf("\rInserting batch %d/%d ...", b+1, batches)
		}
		wtxn := db.WriteTxn(testObjects)
		for j := 0; j < *batchSize; j++ {
			testObjects.Insert(wtxn, &testObject{
				id:     id,
				status: reconciler.StatusPending(),
			})
			id++
		}
		wtxn.Commit()
	}

	if !*quiet {
		fmt.Printf("\nWaiting for reconciliation to finish ...\n\n")
	}

	// Wait for all to be reconciled by waiting for the last added objects to be marked
	// reconciled. This only works here since none of the operations fail.
	for {
		obj, _, watch, ok := testObjects.FirstWatch(db.ReadTxn(), idIndex.Query(id-1))
		if ok && obj.status.Kind == reconciler.StatusKindDone {
			break
		}
		<-watch
	}

	end := time.Now()
	duration := end.Sub(start)

	timePerObject := float64(duration) / float64(*numObjects)
	objsPerSecond := float64(time.Second) / timePerObject

	// Check that all objects were updated.
	if mt.numUpdates.Load() != int32(*numObjects) {
		log.Fatalf("expected %d updates, but only saw %d", *numObjects, mt.numUpdates.Load())
	}

	// Check that all statuses are correctly set.
	iter, _ := testObjects.All(db.ReadTxn())
	for obj, _, ok := iter.Next(); ok; obj, _, ok = iter.Next() {
		if obj.status.Kind != reconciler.StatusKindDone {
			log.Fatalf("Object with unexpected status: %#v", obj)
		}
	}

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		runtime.GC()    // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}

	runtime.GC()
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	err = hive.Stop(context.TODO())
	if err != nil {
		panic(err)
	}

	fmt.Printf("%d objects reconciled in %.2f seconds (batch size %d)\n",
		*numObjects, float64(duration)/float64(time.Second), *batchSize)
	fmt.Printf("Throughput %.2f objects per second\n", objsPerSecond)
	fmt.Printf("Allocated %d objects, %dkB bytes, %dkB bytes still in use\n",
		memAfter.HeapObjects-memBefore.HeapObjects,
		(memAfter.HeapAlloc-memBefore.HeapAlloc)/1024,
		(memAfter.HeapInuse-memBefore.HeapInuse)/1024)

}
