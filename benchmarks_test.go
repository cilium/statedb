// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package statedb

import (
	"context"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cilium/hive"
	"github.com/cilium/hive/cell"
	"github.com/cilium/statedb/index"
	"github.com/cilium/statedb/part"
)

// Number of objects to insert in tests that do repeated inserts.
const numObjectsToInsert = 1000

func BenchmarkDB_WriteTxn_1(b *testing.B) {
	db, table := newTestDBWithMetrics(b, &NopMetrics{})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txn := db.WriteTxn(table)
		_, _, err := table.Insert(txn, testObject{ID: 123, Tags: part.StringSet})
		if err != nil {
			b.Fatalf("Insert error: %s", err)
		}
		txn.Commit()
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "objects/sec")
}

func BenchmarkDB_WriteTxn_10(b *testing.B) {
	benchmarkDB_WriteTxn_batch(b, 10)
}

func BenchmarkDB_WriteTxn_100(b *testing.B) {
	benchmarkDB_WriteTxn_batch(b, 100)
}

func BenchmarkDB_WriteTxn_1000(b *testing.B) {
	benchmarkDB_WriteTxn_batch(b, 1000)
}

func BenchmarkDB_WriteTxn_10000(b *testing.B) {
	benchmarkDB_WriteTxn_batch(b, 10000)
}

func benchmarkDB_WriteTxn_batch(b *testing.B, batchSize int) {
	db, table := newTestDBWithMetrics(b, &NopMetrics{})
	n := b.N
	b.ResetTimer()
	for n > 0 {
		txn := db.WriteTxn(table)
		for j := 0; j < batchSize; j++ {
			_, _, err := table.Insert(txn, testObject{ID: uint64(j), Tags: part.StringSet})
			if err != nil {
				b.Fatalf("Insert error: %s", err)
			}
		}
		txn.Commit()
		n -= batchSize
	}
	txn := db.WriteTxn(table)
	for j := 0; j < n; j++ {
		_, _, err := table.Insert(txn, testObject{ID: uint64(j), Tags: part.StringSet})
		if err != nil {
			b.Fatalf("Insert error: %s", err)
		}
	}
	txn.Commit()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "objects/sec")
}

func BenchmarkDB_WriteTxn_100_SecondaryIndex(b *testing.B) {
	db, table := newTestDBWithMetrics(b, &NopMetrics{}, tagsIndex)
	n := b.N
	tags := []string{"test"}
	for n > 0 {
		txn := db.WriteTxn(table)
		for j := 0; j < 100; j++ {
			_, _, err := table.Insert(txn, testObject{ID: uint64(j), Tags: part.NewStringSet(tags...)})
			if err != nil {
				b.Fatalf("Insert error: %s", err)
			}
		}
		txn.Commit()
		n -= 100
	}
	txn := db.WriteTxn(table)
	for j := 0; j < n; j++ {
		_, _, err := table.Insert(txn, testObject{ID: uint64(j), Tags: part.NewStringSet(tags...)})
		if err != nil {
			b.Fatalf("Insert error: %s", err)
		}
	}
	txn.Commit()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "objects/sec")
}

func BenchmarkDB_RandomInsert(b *testing.B) {
	db, table := newTestDBWithMetrics(b, &NopMetrics{})
	ids := []uint64{}
	for i := 0; i < numObjectsToInsert; i++ {
		ids = append(ids, uint64(i))
	}
	rand.Shuffle(numObjectsToInsert, func(i, j int) {
		ids[i], ids[j] = ids[j], ids[i]
	})
	b.ResetTimer()

	for j := 0; j < b.N; j++ {
		txn := db.WriteTxn(table)
		for _, id := range ids {
			_, _, err := table.Insert(txn, testObject{ID: id, Tags: part.StringSet})
			if err != nil {
				b.Fatalf("Insert error: %s", err)
			}
		}
		txn.Abort()
	}
	b.StopTimer()

	b.ReportMetric(float64(numObjectsToInsert*b.N)/b.Elapsed().Seconds(), "objects/sec")
}

// BenchmarkDB_RandomReplace is like BenchmarkDB_RandomInsert, but instead of
// always inserting a new value this test replaces an existing value.
// This mainly shows the cost of the revision index delete and insert.
//
// This also uses a secondary index to make this a more realistic.
func BenchmarkDB_RandomReplace(b *testing.B) {
	db, table := newTestDBWithMetrics(b, &NopMetrics{}, tagsIndex)
	ids := []uint64{}
	txn := db.WriteTxn(table)
	for i := 0; i < numObjectsToInsert; i++ {
		tag := "odd"
		if i%2 == 0 {
			tag = "even"
		}
		table.Insert(txn, testObject{ID: uint64(i), Tags: part.NewStringSet(tag)})
		ids = append(ids, uint64(i))
	}
	txn.Commit()
	rand.Shuffle(numObjectsToInsert, func(i, j int) {
		ids[i], ids[j] = ids[j], ids[i]
	})
	b.ResetTimer()

	for j := 0; j < b.N; j++ {
		txn := db.WriteTxn(table)
		for _, id := range ids {
			tag := "odd"
			if id%2 == 0 {
				tag = "even"
			}
			_, _, err := table.Insert(txn, testObject{ID: id, Tags: part.NewStringSet(tag)})
			if err != nil {
				b.Fatalf("Insert error: %s", err)
			}
		}
		txn.Abort()
	}
	b.StopTimer()

	b.ReportMetric(float64(numObjectsToInsert*b.N)/b.Elapsed().Seconds(), "objects/sec")
}

func BenchmarkDB_SequentialInsert(b *testing.B) {
	db, table := newTestDBWithMetrics(b, &NopMetrics{})
	b.ResetTimer()

	for j := 0; j < b.N; j++ {
		txn := db.WriteTxn(table)
		for id := uint64(0); id < uint64(numObjectsToInsert); id++ {
			_, _, err := table.Insert(txn, testObject{ID: id, Tags: part.StringSet})
			if err != nil {
				b.Fatalf("Insert error: %s", err)
			}
		}
		txn.Commit()
	}
	b.StopTimer()

	require.EqualValues(b, table.NumObjects(db.ReadTxn()), numObjectsToInsert)
	b.ReportMetric(float64(numObjectsToInsert*b.N)/b.Elapsed().Seconds(), "objects/sec")
}

func BenchmarkDB_Changes_Baseline(b *testing.B) {
	db, table := newTestDBWithMetrics(b, &NopMetrics{})
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		txn := db.WriteTxn(table)
		for i := uint64(0); i < numObjectsToInsert; i++ {
			_, _, err := table.Insert(txn, testObject{ID: uint64(i), Tags: part.StringSet})
			if err != nil {
				b.Fatalf("Insert: %s", err)
			}
		}
		txn.Commit()

		// Delete all objects to time the baseline without deletion tracking.
		txn = db.WriteTxn(table)
		table.DeleteAll(txn)
		txn.Commit()
	}
	b.ReportMetric(float64(b.N*numObjectsToInsert)/b.Elapsed().Seconds(), "objects/sec")
}

func BenchmarkDB_Changes(b *testing.B) {
	db, table := newTestDBWithMetrics(b, &NopMetrics{})
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		// Create the change iterator.
		txn := db.WriteTxn(table)
		iter, err := table.Changes(txn)
		txn.Commit()
		require.NoError(b, err)

		// Create objects
		txn = db.WriteTxn(table)
		for i := 0; i < numObjectsToInsert; i++ {
			_, _, err := table.Insert(txn, testObject{ID: uint64(i), Tags: part.StringSet})
			if err != nil {
				b.Fatalf("Insert: %s", err)
			}
		}
		txn.Commit()

		// Iterator created before insertions should be empty.
		for ev, _, ok := iter.Next(); ok; ev, _, ok = iter.Next() {
			b.Fatalf("did not expect change: %v", ev)
		}

		// Refresh to observe the insertions.
		<-iter.Watch(db.ReadTxn())
		nDeleted := 0
		nExists := 0
		for ev, _, ok := iter.Next(); ok; ev, _, ok = iter.Next() {
			if ev.Deleted {
				b.Fatalf("expected create for %v", ev)
			}
			nExists++
		}
		require.EqualValues(b, numObjectsToInsert, nExists)

		// Delete all objects to time the cost for deletion tracking.
		txn = db.WriteTxn(table)
		table.DeleteAll(txn)
		txn.Commit()

		// Refresh to observe the deletions.
		<-iter.Watch(db.ReadTxn())
		for ev, _, ok := iter.Next(); ok; ev, _, ok = iter.Next() {
			if ev.Deleted {
				nDeleted++
				nExists--
			} else {
				b.Fatalf("expected deleted for %v", ev)
			}
		}
		require.EqualValues(b, numObjectsToInsert, nDeleted)
		require.EqualValues(b, 0, nExists)
		iter.Close()
	}
	b.StopTimer()
	eventuallyGraveyardIsEmpty(b, db)
	b.ReportMetric(float64(b.N*numObjectsToInsert)/b.Elapsed().Seconds(), "objects/sec")
}

func BenchmarkDB_RandomLookup(b *testing.B) {
	db, table := newTestDBWithMetrics(b, &NopMetrics{})

	wtxn := db.WriteTxn(table)
	queries := []Query[testObject]{}
	for i := 0; i < numObjectsToInsert; i++ {
		queries = append(queries, idIndex.Query(uint64(i)))
		_, _, err := table.Insert(wtxn, testObject{ID: uint64(i), Tags: part.StringSet})
		require.NoError(b, err)
	}
	wtxn.Commit()
	rand.Shuffle(numObjectsToInsert, func(i, j int) {
		queries[i], queries[j] = queries[j], queries[i]
	})
	b.ResetTimer()

	for j := 0; j < b.N; j++ {
		txn := db.ReadTxn()
		for _, q := range queries {
			_, _, ok := table.Get(txn, q)
			if !ok {
				b.Fatal("object not found")
			}
		}
	}
	b.ReportMetric(float64(numObjectsToInsert*b.N)/b.Elapsed().Seconds(), "objects/sec")
}

func BenchmarkDB_SequentialLookup(b *testing.B) {
	db, table := newTestDBWithMetrics(b, &NopMetrics{})
	wtxn := db.WriteTxn(table)
	ids := []uint64{}
	queries := []Query[testObject]{}
	for i := 0; i < numObjectsToInsert; i++ {
		queries = append(queries, idIndex.Query(uint64(i)))
		ids = append(ids, uint64(i))
		_, _, err := table.Insert(wtxn, testObject{ID: uint64(i), Tags: part.StringSet})
		require.NoError(b, err)
	}
	wtxn.Commit()
	b.ResetTimer()

	txn := db.ReadTxn()
	for n := 0; n < b.N; n++ {
		for _, q := range queries {
			_, _, ok := table.Get(txn, q)
			if !ok {
				b.Fatalf("Object not found")
			}
		}
	}
	b.ReportMetric(float64(numObjectsToInsert*b.N)/b.Elapsed().Seconds(), "objects/sec")
}

const numObjectsIteration = 100000

func BenchmarkDB_FullIteration_All(b *testing.B) {
	db, table := newTestDBWithMetrics(b, &NopMetrics{})
	wtxn := db.WriteTxn(table)
	for i := 0; i < numObjectsIteration; i++ {
		_, _, err := table.Insert(wtxn, testObject{ID: uint64(i), Tags: part.StringSet})
		require.NoError(b, err)
	}
	wtxn.Commit()
	b.ResetTimer()

	for j := 0; j < b.N; j++ {
		txn := db.ReadTxn()
		iter, _ := table.All(txn)
		i := uint64(0)
		for obj, _, ok := iter.Next(); ok; obj, _, ok = iter.Next() {
			if obj.ID != i {
				b.Fatalf("expected ID %d, got %d", i, obj.ID)
			}
			i++
		}
		if numObjectsIteration != i {
			b.Fatalf("expected to iterate %d objects, got %d", numObjectsIteration, i)
		}
	}
	b.ReportMetric(float64(numObjectsIteration*b.N)/b.Elapsed().Seconds(), "objects/sec")
}

func BenchmarkDB_FullIteration_Get(b *testing.B) {
	db, table := newTestDBWithMetrics(b, &NopMetrics{})
	wtxn := db.WriteTxn(table)
	ids := []uint64{}
	queries := []Query[testObject]{}
	for i := 0; i < numObjectsIteration; i++ {
		queries = append(queries, idIndex.Query(uint64(i)))
		ids = append(ids, uint64(i))
		_, _, err := table.Insert(wtxn, testObject{ID: uint64(i), Tags: part.StringSet})
		require.NoError(b, err)
	}
	wtxn.Commit()
	b.ResetTimer()

	txn := db.ReadTxn()
	for n := 0; n < b.N; n++ {
		for _, q := range queries {
			_, _, ok := table.Get(txn, q)
			if !ok {
				b.Fatalf("Object not found")
			}
		}
	}
	b.ReportMetric(float64(numObjectsIteration*b.N)/b.Elapsed().Seconds(), "objects/sec")
}

type testObject2 testObject

var (
	id2Index = Index[testObject2, uint64]{
		Name: "id",
		FromObject: func(t testObject2) index.KeySet {
			return index.NewKeySet(index.Uint64(t.ID))
		},
		FromKey: index.Uint64,
		Unique:  true,
	}
)

// BenchmarkDB_PropagationDelay tests the propagation delay when changes from one
// table are propagated to another.
func BenchmarkDB_PropagationDelay(b *testing.B) {
	const batchSize = 10

	var (
		db     *DB
		table1 = MustNewTable("test", idIndex)
		table2 = MustNewTable("test2", id2Index)
	)

	h := hive.NewWithOptions(
		hive.Options{Logger: logger},

		Cell, // DB
		cell.Invoke(func(db_ *DB) error {
			db = db_
			return db.RegisterTable(table1, table2)
		}),
	)

	require.NoError(b, h.Start(context.TODO()))
	b.Cleanup(func() {
		assert.NoError(b, h.Stop(context.TODO()))
	})

	b.ResetTimer()

	var (
		revision = Revision(0)
		watch1   = closedWatchChannel
	)

	samples := []time.Duration{}

	// Test the propagation delay for microbatch
	// Doing b.N/batchSize rounds to get per-object cost versus per
	// batch cost.
	for i := 0; i < b.N/batchSize; i++ {
		start := time.Now()

		// Commit a batch to the first table.
		wtxn := db.WriteTxn(table1)
		for i := 0; i < batchSize; i++ {
			table1.Insert(wtxn, testObject{ID: uint64(i), Tags: part.StringSet})
		}
		wtxn.Commit()

		// Wait for the trigger
		<-watch1

		// Grab a watch channel on the second table
		txn := db.ReadTxn()
		_, watch2 := table2.All(txn)

		// Propagate the batch from first table to the second table
		var iter Iterator[testObject]
		iter, watch1 = table1.LowerBound(txn, ByRevision[testObject](revision))
		wtxn = db.WriteTxn(table2)
		for obj, _, ok := iter.Next(); ok; obj, _, ok = iter.Next() {
			table2.Insert(wtxn, testObject2(obj))
		}
		wtxn.Commit()
		revision = table1.Revision(txn)

		// Wait for trigger on second table
		<-watch2

		samples = append(samples, time.Since(start))
	}
	b.StopTimer()

	if len(samples) > 100 {
		sort.Slice(samples,
			func(i, j int) bool {
				return samples[i] < samples[j]
			})
		b.ReportMetric(float64(samples[len(samples)/2]/time.Microsecond), "50th_µs")
		b.ReportMetric(float64(samples[len(samples)*9/10]/time.Microsecond), "90th_µs")
		b.ReportMetric(float64(samples[len(samples)*99/100]/time.Microsecond), "99th_µs")
	}

}
