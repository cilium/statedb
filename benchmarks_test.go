// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package statedb

import (
	"context"
	"math/rand"
	"sort"
	"testing"
	"time"

	iradix "github.com/hashicorp/go-immutable-radix/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cilium/hive"
	"github.com/cilium/hive/cell"
	"github.com/cilium/statedb/index"
)

// Number of objects to insert in tests that do repeated inserts.
const numObjectsToInsert = 1000

func BenchmarkDB_WriteTxn_1(b *testing.B) {
	db, table := newTestDBWithMetrics(b, &NopMetrics{})
	for i := 0; i < b.N; i++ {
		txn := db.WriteTxn(table)
		_, _, err := table.Insert(txn, testObject{ID: 123, Tags: nil})
		if err != nil {
			b.Fatalf("Insert error: %s", err)
		}
		txn.Commit()
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "objects/sec")
}

func BenchmarkDB_WriteTxn_10(b *testing.B) {
	db, table := newTestDBWithMetrics(b, &NopMetrics{})
	n := b.N
	for n > 0 {
		txn := db.WriteTxn(table)
		for j := 0; j < 10; j++ {
			_, _, err := table.Insert(txn, testObject{ID: uint64(j), Tags: nil})
			if err != nil {
				b.Fatalf("Insert error: %s", err)
			}
		}
		txn.Commit()
		n -= 10
	}
	txn := db.WriteTxn(table)
	for j := 0; j < n; j++ {
		_, _, err := table.Insert(txn, testObject{ID: uint64(j), Tags: nil})
		if err != nil {
			b.Fatalf("Insert error: %s", err)
		}
	}
	txn.Commit()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "objects/sec")
}

func BenchmarkDB_WriteTxn_100(b *testing.B) {
	db, table := newTestDBWithMetrics(b, &NopMetrics{})
	n := b.N
	for n > 0 {
		txn := db.WriteTxn(table)
		for j := 0; j < 100; j++ {
			_, _, err := table.Insert(txn, testObject{ID: uint64(j), Tags: nil})
			if err != nil {
				b.Fatalf("Insert error: %s", err)
			}
		}
		txn.Commit()
		n -= 100
	}
	txn := db.WriteTxn(table)
	for j := 0; j < n; j++ {
		_, _, err := table.Insert(txn, testObject{ID: uint64(j), Tags: nil})
		if err != nil {
			b.Fatalf("Insert error: %s", err)
		}
	}
	txn.Commit()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "objects/sec")
}

func BenchmarkDB_WriteTxn_100_SecondaryIndex(b *testing.B) {
	db, table := newTestDBWithMetrics(b, &NopMetrics{})
	n := b.N
	tags := []string{"test"}
	for n > 0 {
		txn := db.WriteTxn(table)
		for j := 0; j < 100; j++ {
			_, _, err := table.Insert(txn, testObject{ID: uint64(j), Tags: tags})
			if err != nil {
				b.Fatalf("Insert error: %s", err)
			}
		}
		txn.Commit()
		n -= 100
	}
	txn := db.WriteTxn(table)
	for j := 0; j < n; j++ {
		_, _, err := table.Insert(txn, testObject{ID: uint64(j), Tags: tags})
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
			_, _, err := table.Insert(txn, testObject{ID: id, Tags: nil})
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
		table.Insert(txn, testObject{ID: uint64(i), Tags: []string{tag}})
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
			_, _, err := table.Insert(txn, testObject{ID: id, Tags: []string{tag}})
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
			_, _, err := table.Insert(txn, testObject{ID: id, Tags: nil})
			require.NoError(b, err)
		}
		txn.Commit()
	}
	b.StopTimer()

	require.EqualValues(b, table.NumObjects(db.ReadTxn()), numObjectsToInsert)
	b.ReportMetric(float64(numObjectsToInsert*b.N)/b.Elapsed().Seconds(), "objects/sec")
}

func BenchmarkDB_Baseline_SingleRadix_Insert(b *testing.B) {
	for i := 0; i < b.N; i++ {
		tree := iradix.New[uint64]()
		txn := tree.Txn()
		for j := uint64(0); j < numObjectsToInsert; j++ {
			txn.Insert(index.Uint64(j), j)
		}
		tree = txn.Commit()
		require.Equal(b, tree.Len(), numObjectsToInsert)
	}
	b.ReportMetric(float64(numObjectsToInsert*b.N)/b.Elapsed().Seconds(), "objects/sec")
}

func BenchmarkDB_Baseline_SingleRadix_TrackMutate_Insert(b *testing.B) {
	for i := 0; i < b.N; i++ {
		tree := iradix.New[uint64]()
		txn := tree.Txn()
		txn.TrackMutate(true) // Enable the watch channels
		for j := uint64(0); j < numObjectsToInsert; j++ {
			txn.Insert(index.Uint64(j), j)
		}
		tree = txn.Commit() // Commit and notify
		require.Equal(b, tree.Len(), numObjectsToInsert)
	}
	b.ReportMetric(float64(numObjectsToInsert*b.N)/b.Elapsed().Seconds(), "objects/sec")
}
func BenchmarkDB_Baseline_SingleRadix_Lookup(b *testing.B) {
	tree := iradix.New[uint64]()
	for j := uint64(0); j < numObjectsToInsert; j++ {
		tree, _, _ = tree.Insert(index.Uint64(j), j)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := uint64(0); j < numObjectsToInsert; j++ {
			v, ok := tree.Get(index.Uint64(j))
			if v != j || !ok {
				b.Fatalf("impossible: %d != %d || %v", v, j, ok)
			}
		}

	}
	b.ReportMetric(float64(numObjectsToInsert*b.N)/b.Elapsed().Seconds(), "objects/sec")
}

func BenchmarkDB_Baseline_Hashmap_Insert(b *testing.B) {
	for i := 0; i < b.N; i++ {
		m := map[uint64]uint64{}
		for j := uint64(0); j < numObjectsToInsert; j++ {
			m[j] = j
		}
		if len(m) != numObjectsToInsert {
			b.Fatalf("%d != %d", len(m), numObjectsToInsert)
		}
	}
	b.ReportMetric(float64(numObjectsToInsert*b.N)/b.Elapsed().Seconds(), "objects/sec")
}

func BenchmarkDB_Baseline_Hashmap_Lookup(b *testing.B) {
	m := map[uint64]uint64{}
	for j := uint64(0); j < numObjectsToInsert; j++ {
		m[j] = j
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := uint64(0); j < numObjectsToInsert; j++ {
			if m[j] != j {
				b.Fatalf("impossible: %d != %d", m[j], j)
			}
		}
	}
	b.ReportMetric(float64(numObjectsToInsert*b.N)/b.Elapsed().Seconds(), "objects/sec")
}

func BenchmarkDB_DeleteTracker_Baseline(b *testing.B) {
	db, table := newTestDBWithMetrics(b, &NopMetrics{})
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		txn := db.WriteTxn(table)
		for i := uint64(0); i < numObjectsToInsert; i++ {
			_, _, err := table.Insert(txn, testObject{ID: uint64(i), Tags: nil})
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

func BenchmarkDB_DeleteTracker(b *testing.B) {
	db, table := newTestDBWithMetrics(b, &NopMetrics{})
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		// Create objects
		txn := db.WriteTxn(table)
		dt, err := table.DeleteTracker(txn, "test")
		require.NoError(b, err)
		defer dt.Close()
		for i := 0; i < numObjectsToInsert; i++ {
			_, _, err := table.Insert(txn, testObject{ID: uint64(i), Tags: nil})
			if err != nil {
				b.Fatalf("Insert: %s", err)
			}
		}
		txn.Commit()

		// Delete all objects to time the cost for deletion tracking.
		txn = db.WriteTxn(table)
		table.DeleteAll(txn)
		txn.Commit()

		// Iterate over the deleted objects
		nDeleted := 0
		dt.Iterate(
			db.ReadTxn(),
			func(obj testObject, deleted bool, _ Revision) {
				nDeleted++
			})
		require.EqualValues(b, nDeleted, numObjectsToInsert)
		dt.Close()
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
		_, _, err := table.Insert(wtxn, testObject{ID: uint64(i), Tags: nil})
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
	for i := 0; i < numObjectsToInsert; i++ {
		ids = append(ids, uint64(i))
		_, _, err := table.Insert(wtxn, testObject{ID: uint64(i), Tags: nil})
		require.NoError(b, err)
	}
	wtxn.Commit()
	b.ResetTimer()

	txn := db.ReadTxn()
	for n := 0; n < b.N; n++ {
		for _, id := range ids {
			obj, _, ok := table.Get(txn, idIndex.Query(id))
			if !ok {
				b.Fatalf("Object not found")
			}
			if obj.ID != id {
				b.Fatalf("expected ID %d, got %d", id, obj.ID)
			}
		}
	}
	b.ReportMetric(float64(numObjectsToInsert*b.N)/b.Elapsed().Seconds(), "objects/sec")
}

func BenchmarkDB_FullIteration_All(b *testing.B) {
	db, table := newTestDBWithMetrics(b, &NopMetrics{})
	wtxn := db.WriteTxn(table)
	for i := 0; i < numObjectsToInsert; i++ {
		_, _, err := table.Insert(wtxn, testObject{ID: uint64(i), Tags: nil})
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
		require.EqualValues(b, i, numObjectsToInsert)
	}
	b.ReportMetric(float64(numObjectsToInsert*b.N)/b.Elapsed().Seconds(), "objects/sec")
}

func BenchmarkDB_FullIteration_Get(b *testing.B) {
	db, table := newTestDBWithMetrics(b, &NopMetrics{}, tagsIndex)
	wtxn := db.WriteTxn(table)
	for i := 0; i < numObjectsToInsert; i++ {
		_, _, err := table.Insert(wtxn, testObject{ID: uint64(i), Tags: []string{"foo"}})
		require.NoError(b, err)
	}
	wtxn.Commit()
	b.ResetTimer()

	for j := 0; j < b.N; j++ {
		txn := db.ReadTxn()
		iter := table.Select(txn, tagsIndex.Query("foo"))
		i := uint64(0)
		for obj, _, ok := iter.Next(); ok; obj, _, ok = iter.Next() {
			if obj.ID != i {
				b.Fatalf("expected ID %d, got %d", i, obj.ID)
			}
			i++
		}
		require.EqualValues(b, i, numObjectsToInsert)
	}
	b.ReportMetric(float64(numObjectsToInsert*b.N)/b.Elapsed().Seconds(), "objects/sec")
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
		table1 = MustNewTable[testObject]("test", idIndex)
		table2 = MustNewTable[testObject2]("test2", id2Index)
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
			table1.Insert(wtxn, testObject{ID: uint64(i), Tags: nil})
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
