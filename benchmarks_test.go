// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package statedb

import (
	"context"
	"iter"
	"log/slog"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cilium/hive"
	"github.com/cilium/hive/cell"
	"github.com/cilium/hive/hivetest"
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
		_, _, err := table.Insert(txn, testObject{ID: 123})
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

func benchmarkDB_WriteTxn_batch(b *testing.B, batchSize int) {
	db, table := newTestDBWithMetrics(b, &NopMetrics{})
	n := b.N
	b.ResetTimer()

	for n > 0 {
		txn := db.WriteTxn(table)
		toWrite := min(n, batchSize)
		for i := range toWrite {
			_, _, err := table.Insert(txn, testObject{ID: uint64(i)})
			if err != nil {
				b.Fatalf("Insert error: %s", err)
			}
		}
		txn.Commit()
		n -= toWrite
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "objects/sec")
}

func BenchmarkDB_WriteTxn_100_SecondaryIndex(b *testing.B) {
	db, table := newTestDBWithMetrics(b, &NopMetrics{}, tagsIndex)
	batchSize := 100
	n := b.N
	tagSet := part.NewSet("test")

	for n > 0 {
		txn := db.WriteTxn(table)
		toWrite := min(n, batchSize)
		for i := range toWrite {
			_, _, err := table.Insert(txn, testObject{ID: uint64(i), Tags: tagSet})
			if err != nil {
				b.Fatalf("Insert error: %s", err)
			}
		}
		txn.Commit()
		n -= toWrite
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "objects/sec")
}

func BenchmarkDB_Delete(b *testing.B) {
	db, table := newTestDBWithMetrics(b, &NopMetrics{})

	b.ResetTimer()
	for range b.N {
		txn := db.WriteTxn(table)
		for i := range numObjectsToInsert {
			table.Insert(txn, testObject{ID: uint64(i)})
		}
		txn.Commit()

		txn = db.WriteTxn(table)
		table.DeleteAll(txn)
		txn.Commit()
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N*numObjectsToInsert)/b.Elapsed().Seconds(), "insert+delete/sec")
}

func BenchmarkDB_Delete_With_Changes(b *testing.B) {
	db, table := newTestDBWithMetrics(b, &NopMetrics{})
	txn := db.WriteTxn(table)
	iter, err := table.Changes(txn)
	txn.Commit()
	require.NoError(b, err)

	b.ResetTimer()
	for range b.N {
		txn := db.WriteTxn(table)
		for i := range numObjectsToInsert {
			table.Insert(txn, testObject{ID: uint64(i)})
		}
		rxn := txn.Commit()

		// Observe inserts
		numChanges := 0
		changes, _ := iter.Next(rxn)
		for range changes {
			numChanges++
		}
		if numChanges != numObjectsToInsert {
			b.Fatalf("expected %d changes, got %d\n", numObjectsToInsert, numChanges)
		}

		txn = db.WriteTxn(table)
		table.DeleteAll(txn)
		rxn = txn.Commit()

		// Observe deletes
		changes, _ = iter.Next(rxn)
		numChanges = 0
		for range changes {
			numChanges++
		}
		if numChanges != numObjectsToInsert {
			b.Fatalf("expected %d changes, got %d\n", numObjectsToInsert, numChanges)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N*numObjectsToInsert)/b.Elapsed().Seconds(), "insert+delete/sec")
}

func BenchmarkDB_Modify(b *testing.B) {
	benchmarkDB_Modify_vs_GetInsert(b, false)
}

func BenchmarkDB_GetInsert(b *testing.B) {
	benchmarkDB_Modify_vs_GetInsert(b, true)
}

func benchmarkDB_Modify_vs_GetInsert(b *testing.B, doGetInsert bool) {
	db, table := newTestDBWithMetrics(b, &NopMetrics{})

	ids := []uint64{}
	for i := 0; i < numObjectsToInsert; i++ {
		ids = append(ids, uint64(i))
	}
	rand.Shuffle(numObjectsToInsert, func(i, j int) {
		ids[i], ids[j] = ids[j], ids[i]
	})
	txn := db.WriteTxn(table)
	for _, id := range ids {
		_, _, err := table.Insert(txn, testObject{ID: id})
		if err != nil {
			b.Fatalf("Insert error: %s", err)
		}
	}
	txn.Commit()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		txn := db.WriteTxn(table)
		for _, id := range ids {
			if doGetInsert {
				old, _, _ := table.Get(txn, idIndex.Query(id))
				table.Insert(txn, old)
			} else {
				table.Modify(
					txn,
					testObject{ID: id},
					func(old testObject, new testObject) testObject {
						return new
					})
			}
		}
		txn.Commit()
	}
	b.ReportMetric(float64(b.N*len(ids))/b.Elapsed().Seconds(), "objects/sec")
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
			_, _, err := table.Insert(txn, testObject{ID: id, Tags: part.Set[string]{}})
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
		table.Insert(txn, testObject{ID: uint64(i), Tags: part.NewSet(tag)})
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
			_, _, err := table.Insert(txn, testObject{ID: id, Tags: part.NewSet(tag)})
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
			_, _, err := table.Insert(txn, testObject{ID: id})
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
			_, _, err := table.Insert(txn, testObject{ID: uint64(i)})
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

	// Create the change iterator.
	txn := db.WriteTxn(table)
	require.Zero(b, table.NumObjects(txn))
	iter, err := table.Changes(txn)
	txn.Commit()
	require.NoError(b, err)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		// Create objects
		txn = db.WriteTxn(table)
		for i := 0; i < numObjectsToInsert; i++ {
			_, _, err := table.Insert(txn, testObject{ID: uint64(i)})
			if err != nil {
				b.Fatalf("Insert: %s", err)
			}
		}
		txn.Commit()

		// Observe the creations.
		changes, watch := iter.Next(db.ReadTxn())
		nDeleted := 0
		nExists := 0

		for change := range changes {
			if change.Deleted {
				b.Fatalf("expected create for %v", change)
			}
			nExists++
		}
		if numObjectsToInsert != nExists {
			b.Fatalf("expected to observe %d, got %d", numObjectsToInsert, nExists)
		}

		// Delete all objects to time the cost for deletion tracking.
		txn = db.WriteTxn(table)
		table.DeleteAll(txn)
		txn.Commit()

		// Watch channel should be closed now.
		<-watch

		// Observe the deletions.
		changes, watch = iter.Next(db.ReadTxn())
		for change := range changes {
			if change.Deleted {
				nDeleted++
				nExists--
			} else {
				b.Fatalf("expected deleted for %v", change)
			}
		}
		if numObjectsToInsert != nDeleted {
			b.Fatalf("expected to see %d deleted, got %d", numObjectsToInsert, nDeleted)
		}
	}
	b.StopTimer()

	b.ReportMetric(float64(b.N*numObjectsToInsert)/b.Elapsed().Seconds(), "objects/sec")
}

func BenchmarkDB_RandomLookup(b *testing.B) {
	db, table := newTestDBWithMetrics(b, &NopMetrics{})

	wtxn := db.WriteTxn(table)
	queries := []Query[testObject]{}
	for i := 0; i < numObjectsToInsert; i++ {
		queries = append(queries, idIndex.Query(uint64(i)))
		_, _, err := table.Insert(wtxn, testObject{ID: uint64(i)})
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
		_, _, err := table.Insert(wtxn, testObject{ID: uint64(i)})
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

func BenchmarkDB_Prefix_SecondaryIndex(b *testing.B) {
	db, table := newTestDBWithMetrics(b, &NopMetrics{}, tagsIndex)
	tagSet := part.NewSet("test")
	txn := db.WriteTxn(table)
	for i := 0; i < numObjectsToInsert; i++ {
		_, _, err := table.Insert(txn, testObject{ID: uint64(i), Tags: tagSet})
		require.NoError(b, err)
	}
	rtxn := txn.Commit()
	b.ResetTimer()

	q := tagsIndex.Query("test")
	for n := 0; n < b.N; n++ {
		count := 0
		for range table.Prefix(rtxn, q) {
			count++
		}
		if count != numObjectsToInsert {
			b.Fatalf("wrong number of objects, expected %d, got %d", numObjectsToInsert, count)
		}
	}

	b.ReportMetric(float64(numObjectsToInsert*b.N)/b.Elapsed().Seconds(), "objects/sec")
}

const numObjectsIteration = 100000

func BenchmarkDB_FullIteration_All(b *testing.B) {
	db, table := newTestDBWithMetrics(b, &NopMetrics{})
	wtxn := db.WriteTxn(table)
	for i := 0; i < numObjectsIteration; i++ {
		_, _, err := table.Insert(wtxn, testObject{ID: uint64(i)})
		require.NoError(b, err)
	}
	wtxn.Commit()
	b.ResetTimer()

	for j := 0; j < b.N; j++ {
		txn := db.ReadTxn()
		i := uint64(0)
		for obj := range table.All(txn) {
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
		_, _, err := table.Insert(wtxn, testObject{ID: uint64(i)})
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

	h := hive.New(
		Cell, // DB
		cell.Invoke(func(db_ *DB) error {
			db = db_
			return db.RegisterTable(table1, table2)
		}),
	)

	log := hivetest.Logger(b, hivetest.LogLevel(slog.LevelError))
	require.NoError(b, h.Start(log, context.TODO()))
	b.Cleanup(func() {
		assert.NoError(b, h.Stop(log, context.TODO()))
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
			table1.Insert(wtxn, testObject{ID: uint64(i)})
		}
		wtxn.Commit()

		// Wait for the trigger
		<-watch1

		// Grab a watch channel on the second table
		txn := db.ReadTxn()
		_, watch2 := table2.AllWatch(txn)

		// Propagate the batch from first table to the second table
		var seq iter.Seq2[testObject, Revision]
		seq, watch1 = table1.LowerBoundWatch(txn, ByRevision[testObject](revision))
		wtxn = db.WriteTxn(table2)
		for obj := range seq {
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
