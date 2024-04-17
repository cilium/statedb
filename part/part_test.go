// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package part

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_commonPrefix(t *testing.T) {
	check := func(a, b, common string) {
		actual := string(commonPrefix([]byte(a), []byte(b)))
		if actual != common {
			t.Fatalf("expected commonPrefix(%q, %q) to equal %q, but got %q",
				a, b, common, actual)
		}
	}

	check("", "", "")
	check("", "a", "")
	check("a", "", "")
	check("a", "a", "a")
	check("a", "b", "")
	check("abc", "d", "")
	check("d", "abc", "")
	check("ab", "abc", "ab")
	check("abc", "ab", "ab")
}

func Test_search(t *testing.T) {
	tree := New[[]byte](RootOnlyWatch)
	_, _, tree = tree.Insert([]byte("a"), []byte("a"))
	_, _, tree = tree.Insert([]byte("ba"), []byte("ba"))
	_, _, tree = tree.Insert([]byte("bb"), []byte("bb"))
	_, _, tree = tree.Insert([]byte("c"), []byte("c"))
	_, _, tree = tree.Insert([]byte("ca"), []byte("ca"))

	v, _, ok := tree.Get([]byte("nope"))
	if ok {
		t.Fatalf("found unexpected value: %v", v)
	}

	for _, key := range []string{"a", "ba", "bb"} {
		v, _, ok = tree.Get([]byte(key))
		if !ok || string(v) != key {
			t.Fatalf("%q not found (%v) or mismatch %q", key, ok, v)
		}
	}
}

func uint64Key(n uint64) []byte {
	return binary.BigEndian.AppendUint64(nil, n)
}

func uint32Key(n uint32) []byte {
	return binary.BigEndian.AppendUint32(nil, n)
}

func Test_simple_delete(t *testing.T) {
	tree := New[uint64]()
	txn := tree.Txn()

	_, hadOld := txn.Insert(uint64Key(1), 1)
	require.False(t, hadOld)

	_, hadOld = txn.Insert(uint64Key(2), 2)
	require.False(t, hadOld)

	_, hadOld = txn.Delete(uint64Key(1))
	require.True(t, hadOld)

	_, _, ok := txn.Get(uint64Key(1))
	require.False(t, ok)
}

func Test_delete(t *testing.T) {
	tree := New[uint64]()

	// Do multiple rounds with the same tree.
	for round := 0; round < 100; round++ {
		// Use a random amount of keys in random order to exercise different
		// tree structures each time.
		numKeys := 10 + rand.Intn(1000)
		t.Logf("numKeys=%d", numKeys)

		keys := []uint64{}
		for i := uint64(1); i < uint64(numKeys); i++ {
			keys = append(keys, i)
		}
		hadOld := false

		// Insert the keys in random order.
		rand.Shuffle(len(keys), func(i, j int) {
			keys[i], keys[j] = keys[j], keys[i]
		})

		txn := tree.Txn()
		for _, i := range keys {
			_, hadOld = txn.Insert(uint64Key(i), i)
			assert.False(t, hadOld)
			v, _, ok := txn.Get(uint64Key(i))
			assert.True(t, ok)
			assert.EqualValues(t, v, i)
		}
		tree = txn.Commit()
		assert.Equal(t, len(keys), tree.Len())

		// Delete the keys in random order.
		rand.Shuffle(len(keys), func(i, j int) {
			keys[i], keys[j] = keys[j], keys[i]
		})

		txn = tree.Txn()
		for _, i := range keys {
			v, _, ok := txn.Get(uint64Key(i))
			assert.True(t, ok)
			assert.EqualValues(t, v, i)
			v, hadOld = txn.Delete(uint64Key(i))
			assert.True(t, hadOld)
			assert.EqualValues(t, v, i)
			_, _, ok = txn.Get(uint64Key(i))
			assert.False(t, ok)
		}
		tree = txn.Commit()

		assert.Equal(t, 0, tree.Len())
		for _, i := range keys {
			_, _, ok := tree.Get(uint64Key(i))
			assert.False(t, ok)
		}

		// And finally insert the keys back one more time
		// in random order.
		rand.Shuffle(len(keys), func(i, j int) {
			keys[i], keys[j] = keys[j], keys[i]
		})

		watches := map[uint64]<-chan struct{}{}

		txn = tree.Txn()
		for _, i := range keys {
			_, hadOld = txn.Insert(uint64Key(i), i)
			assert.False(t, hadOld)
			v, watch, ok := txn.Get(uint64Key(i))
			watches[i] = watch
			assert.True(t, ok)
			assert.EqualValues(t, v, i)
		}
		tree = txn.Commit()
		assert.Equal(t, len(keys), tree.Len())

		// Do few rounds of lookups and deletions.
		for step := 0; step < 2; step++ {
			// Lookup with a Txn
			txn = tree.Txn()
			for _, i := range keys {
				v, _, ok := txn.Get(uint64Key(i))
				assert.True(t, ok)
				assert.EqualValues(t, v, i)
			}
			txn = nil

			// Test that full iteration is ordered
			iter := tree.Iterator()
			prev := uint64(0)
			num := 0
			for {
				_, v, ok := iter.Next()
				if !ok {
					break
				}
				num++
				require.Greater(t, v, prev)
				prev = v
			}
			assert.Equal(t, num, len(keys))

			// Test that lowerbound iteration is ordered and correct
			idx := len(keys) / 2
			prev = keys[idx]
			num = 0
			start := prev + 1
			iter = tree.LowerBound(uint64Key(start))
			obs := []uint64{}
			for {
				_, v, ok := iter.Next()
				if !ok {
					break
				}
				num++
				obs = append(obs, v)
				require.Greater(t, v, prev)
				prev = v
			}
			exp := 0
			for _, k := range keys {
				if k >= start {
					exp++
				}
			}
			if !assert.Equal(t, exp, num) {
				t.Logf("LowerBound from %d failed", start)
				t.Logf("observed: %v", obs)
				tree.PrintTree()
				t.Fatal()
			}

			// Test that prefix iteration is ordered and correct
			prev = 0
			iter, _ = tree.Prefix([]byte{})
			for {
				_, v, ok := iter.Next()
				if !ok {
					break
				}
				require.Greater(t, v, prev)
				prev = v
			}

			// Remove half the keys
			for _, k := range keys[:len(keys)/2] {
				_, _, tree = tree.Delete(uint64Key(k))
			}
			keys = keys[len(keys)/2:]
		}

		// Remove everything remaining with iteration
		txn = tree.Txn()
		iter := txn.Iterator()
		for k, _, ok := iter.Next(); ok; k, _, ok = iter.Next() {
			_, hadOld = txn.Delete(k)
			assert.True(t, hadOld)

			_, _, ok := txn.Get(k)
			assert.False(t, ok)
		}

		// Check that we can iterate with the transaction and
		// everything is gone.
		iter = txn.Iterator()
		_, _, ok := iter.Next()
		assert.False(t, ok)

		tree = txn.Commit()

		// Check that all the watch channels closed
		for _, watch := range watches {
			<-watch
		}

		// Check that everything is gone after commit.
		for _, i := range keys {
			_, _, ok := tree.Get(uint64Key(i))
			assert.False(t, ok)
		}

		assert.Equal(t, 0, tree.Len())
	}
}

func Test_watch(t *testing.T) {
	tree := New[[]byte]()
	_, _, tree = tree.Insert([]byte("a"), []byte("a"))

	_, watch, ok := tree.Get([]byte("a"))
	if !ok {
		t.Fatal("expected to find 'a'")
	}
	select {
	case <-watch:
		t.Fatal("did not expect watch to be closed")
	default:
	}

	_, _, tree = tree.Insert([]byte("a"), []byte("b"))

	select {
	case <-watch:
	case <-time.After(10 * time.Second):
		t.Fatal("expected watch channel to close")
	}

	v, _, ok := tree.Get([]byte("a"))
	if !ok {
		t.Fatal("expected to find 'a'")
	}
	if string(v) != "b" {
		t.Fatalf("expected value 'b', got '%s'", v)
	}
}

func Test_insert(t *testing.T) {
	tree := New[int]()
	for i := 0; i < 10000; i++ {
		key := binary.NativeEndian.AppendUint32(nil, uint32(i))
		_, _, tree = tree.Insert(key, i)
	}
	for i := 0; i < 10000; i++ {
		key := binary.NativeEndian.AppendUint32(nil, uint32(i))
		_, _, ok := tree.Get(key)
		if !ok {
			t.Fatalf("%d not found", i)
		}
	}
}

func Test_replaceRoot(t *testing.T) {
	tree := New[int]()
	keyA := []byte{'a'}
	keyB := []byte{'a', 'b'}
	_, _, tree = tree.Insert(keyA, 1)
	_, _, tree = tree.Insert(keyB, 3)
	_, _, tree = tree.Delete(keyA)
	_, _, tree = tree.Insert(keyA, 2)
	val, _, ok := tree.Get(keyA)
	if !ok || val != 2 {
		t.Fatalf("%v not found", keyA)
	}
	val, _, ok = tree.Get(keyB)
	if !ok || val != 3 {
		t.Fatalf("%v not found", keyB)
	}
}

func Test_deleteRoot(t *testing.T) {
	tree := New[int]()
	keyA := []byte{'a'}
	_, _, tree = tree.Insert(keyA, 1)
	_, _, tree = tree.Delete(keyA)
	_, _, ok := tree.Get(keyA)
	if ok {
		t.Fatal("Root exists")
	}
}

func Test_deleteIntermediate(t *testing.T) {
	tree := New[int]()
	keyA := []byte{'a'}
	keyAB := []byte{'a', 'b'}
	keyABC := []byte{'a', 'b', 'c'}
	_, _, tree = tree.Insert(keyA, 1)
	_, _, tree = tree.Insert(keyAB, 2)
	_, _, tree = tree.Insert(keyABC, 3)
	_, _, tree = tree.Delete(keyAB)
	_, _, ok := tree.Get(keyA)
	if !ok {
		t.Fatal("A doesn't exist")
	}
	_, _, ok = tree.Get(keyAB)
	if ok {
		t.Fatal("AB exists")
	}
	_, _, ok = tree.Get(keyABC)
	if !ok {
		t.Fatal("ABC doesn't exist")
	}
}

func Test_deleteNonExistantIntermediate(t *testing.T) {
	tree := New[int]()
	keyAB := []byte{'a', 'b'}
	keyAC := []byte{'a', 'c'}
	_, _, tree = tree.Insert(keyAB, 1)
	_, _, tree = tree.Insert(keyAC, 2)
	_, _, tree = tree.Delete([]byte{'a'})
	_, _, ok := tree.Get(keyAB)
	if !ok {
		t.Fatal("AB doesn't exist")
	}
	_, _, ok = tree.Get(keyAC)
	if !ok {
		t.Fatal("AC doesn't exist")
	}
}

func Test_deleteNonExistantCommonPrefix(t *testing.T) {
	tree := New[int]()
	keyAB := []byte{'a', 'b', 'c'}
	_, _, tree = tree.Insert(keyAB, 1)
	_, _, tree = tree.Delete([]byte{'a', 'b', 'e'})
	_, _, ok := tree.Get(keyAB)
	if !ok {
		t.Fatal("AB doesn't exist")
	}
}

func Test_replace(t *testing.T) {
	tree := New[int]()
	key := binary.BigEndian.AppendUint32(nil, uint32(0))

	var v int
	var hadOld bool
	_, hadOld, tree = tree.Insert(key, 1)
	require.False(t, hadOld)

	v, hadOld, tree = tree.Insert(key, 2)
	require.True(t, hadOld)
	require.EqualValues(t, 1, v)
}

func Test_prefix(t *testing.T) {
	tree := New[[]byte]()
	ins := func(s string) { _, _, tree = tree.Insert([]byte(s), []byte(s)) }
	ins("a")
	ins("ab")
	ins("abc")
	ins("abcd")

	iter, _ := tree.Prefix([]byte("ab"))
	k, v, ok := iter.Next()
	assert.True(t, ok)
	assert.Equal(t, []byte("ab"), k)
	assert.Equal(t, []byte("ab"), v)

	k, v, ok = iter.Next()
	assert.True(t, ok)
	assert.Equal(t, []byte("abc"), k)
	assert.Equal(t, []byte("abc"), v)

	k, v, ok = iter.Next()
	assert.True(t, ok)
	assert.Equal(t, []byte("abcd"), k)
	assert.Equal(t, []byte("abcd"), v)

	_, _, ok = iter.Next()
	assert.False(t, ok)
}

func Test_txn(t *testing.T) {
	tree := New[uint64]()
	ins := func(n uint64) { _, _, tree = tree.Insert(uint64Key(n), n) }

	var iter *Iterator[uint64]
	next := func(exOK bool, exVal int) {
		t.Helper()
		_, v, ok := iter.Next()
		if assert.Equal(t, exOK, ok) {
			assert.EqualValues(t, exVal, v)
		}
	}

	for i := 1; i <= 3; i++ {
		ins(1)
		ins(2)
		ins(3)
	}

	txn := tree.Txn()
	txn.Delete(uint64Key(2))
	txn.Delete(uint64Key(3))
	txn.Insert(uint64Key(4), 4)

	iter = txn.Iterator()
	next(true, 1)
	next(true, 4)
	next(false, 0)

	_ = txn.Commit() // Ignore the new tree

	// Original tree should be untouched.
	for i := 1; i <= 3; i++ {
		_, _, ok := tree.Get(uint64Key(uint64(i)))
		assert.True(t, ok, "Get(%d)", i)
	}

	iter = tree.Iterator()
	next(true, 1)
	next(true, 2)
	next(true, 3)
	next(false, 0)
}

func Test_lowerbound(t *testing.T) {
	tree := New[uint64]()
	ins := func(n int) { _, _, tree = tree.Insert(uint64Key(uint64(n)), uint64(n)) }

	// Insert 1..3
	for i := 1; i <= 3; i++ {
		ins(i)
	}

	var iter *Iterator[uint64]
	next := func(exOK bool, exVal int) {
		t.Helper()
		_, v, ok := iter.Next()
		require.Equal(t, exOK, ok)
		require.EqualValues(t, exVal, v)
	}

	iter = tree.LowerBound([]byte{})
	next(true, 1)
	next(true, 2)
	next(true, 3)
	next(false, 0)

	iter = tree.LowerBound(uint64Key(0))
	next(true, 1)
	next(true, 2)
	next(true, 3)
	next(false, 0)

	iter = tree.LowerBound(uint64Key(3))
	next(true, 3)
	next(false, 0)

	iter = tree.LowerBound(uint64Key(4))
	next(false, 0)
}

func Test_lowerbound_edge_cases(t *testing.T) {
	tree := New[uint32]()
	keys := []uint32{}
	ins := func(n uint32) {
		_, _, tree = tree.Insert(uint32Key(n), n)
		keys = append(keys, n)
		fmt.Printf("%x:\n", keys)
		tree.root.printTree(2)
	}

	var iter *Iterator[uint32]
	next := func(exOK bool, exVal uint32) {
		t.Helper()
		_, v, ok := iter.Next()
		assert.Equal(t, exOK, ok)
		require.Equal(t, exVal, v)
	}

	// Empty tree
	iter = tree.LowerBound([]byte{})
	next(false, 0)
	iter = tree.LowerBound(uint32Key(0x1))
	next(false, 0)

	// case 0: Leaf at the root
	ins(0x1)
	iter = tree.LowerBound([]byte{})
	next(true, 0x1)
	next(false, 0)
	iter = tree.LowerBound(uint32Key(0x1))
	next(true, 0x1)
	next(false, 0)
	iter = tree.LowerBound(uint32Key(0x2))
	next(false, 0)

	// Two leafs, node4 root
	ins(0x2)
	iter = tree.LowerBound([]byte{})
	next(true, 0x1)
	next(true, 0x2)
	next(false, 0)
	iter = tree.LowerBound(uint32Key(0x2))
	next(true, 0x2)
	next(false, 0)
	iter = tree.LowerBound(uint32Key(0x3))
	next(false, 0)

	// Different prefix
	ins(0x0101)
	iter = tree.LowerBound(uint32Key(0x100))
	next(true, 0x101)
	next(false, 0)

	// case -1: Matching prefix (0x1??) but only smaller nodes behind it
	ins(0x1100)
	iter = tree.LowerBound(uint32Key(0x102))
	next(true, 0x1100)
	next(false, 0)

	// Short search keys
	ins(0x010000)

	iter = tree.LowerBound([]byte{1})
	next(false, 0)

	iter = tree.LowerBound([]byte{0, 0})
	next(true, 0x1)
	next(true, 0x2)
	next(true, 0x0101)
	next(true, 0x1100)
	next(true, 0x010000)
	next(false, 0)

	iter = tree.LowerBound([]byte{0, 1, 0})
	next(true, 0x010000)
	next(false, 0)

	// Node256
	fmt.Println("node256:")
	for i := 1; i < 50; i += 2 { // add less than 256 for some holes in node256.children
		n := uint32(0x20000 + i)
		_, _, tree = tree.Insert(uint32Key(n), n)
		keys = append(keys, n)
	}
	tree.PrintTree()

	iter = tree.LowerBound(uint32Key(0x20000))
	for i := 1; i < 50; i += 2 {
		n := uint32(0x20000 + i)
		next(true, n)
	}
	next(false, 0)

	iter = tree.LowerBound([]byte{})
	for i := range keys {
		next(true, keys[i])
	}
	next(false, 0)

}

func Test_lowerbound_regression(t *testing.T) {
	// Regression test for bug in lowerbound() where the lowerbound search ended up
	// in a smaller node and thought there were no larger nodes in the tree to iterate
	// over.

	tree := New[uint64]()
	ins := func(n uint64) { _, _, tree = tree.Insert(uint64Key(uint64(n)), uint64(n)) }

	values := []uint64{
		70370, // ... 1 18 226
		70411, // ... 1 19 11
		70412,
	}

	for _, v := range values {
		ins(v)
	}

	tree.PrintTree()

	iter := tree.LowerBound(uint64Key(70399))
	i := 1
	for _, obj, ok := iter.Next(); ok; _, obj, ok = iter.Next() {
		require.Equal(t, values[i], obj)
		i++
	}
	require.Equal(t, len(values), i)
}

func Test_iterate(t *testing.T) {
	sizes := []int{0, 1, 10, 100, 1000, rand.Intn(1000)}
	for _, size := range sizes {
		t.Logf("size=%d", size)
		tree := New[uint64]()
		for i := 0; i < size; i++ {
			_, _, tree = tree.Insert(uint64Key(uint64(i)), uint64(i))
		}

		iter := tree.LowerBound([]byte{})
		i := uint64(0)
		for _, obj, ok := iter.Next(); ok; _, obj, ok = iter.Next() {
			if obj != uint64(i) {
				t.Fatalf("expected %d,  got %d", i, obj)
			}
			i++
		}
		require.EqualValues(t, i, size)

		_, _, ok := iter.Next()
		require.False(t, ok, "expected exhausted iterator to keep returning false")
	}

}

func Test_lowerbound_bigger(t *testing.T) {
	tree := New[uint64]()
	ins := func(n int) { _, _, tree = tree.Insert(uint64Key(uint64(n)), uint64(n)) }

	// Insert 5..10
	for i := 5; i <= 10; i++ {
		ins(i)
	}

	iter := tree.LowerBound([]byte{4})
	_, _, ok := iter.Next()
	require.False(t, ok)
}

func Benchmark_Insert_RootOnlyWatch(b *testing.B) {
	benchmark_Insert(b, RootOnlyWatch)
}

func Benchmark_Insert(b *testing.B) {
	benchmark_Insert(b)
}

func benchmark_Insert(b *testing.B, opts ...Option) {
	numObjs := 1000
	for n := 0; n < b.N; n++ {
		tree := New[int](opts...)
		txn := tree.Txn()
		for i := 0; i < numObjs; i++ {
			key := binary.BigEndian.AppendUint32(nil, uint32(numObjs+i))
			txn.Insert(key, numObjs+i)
		}
		txn.Commit()
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N*numObjs)/b.Elapsed().Seconds(), "objects/sec")
}

func Benchmark_Replace(b *testing.B) {
	benchmark_Replace(b, true)
}

func Benchmark_Replace_RootOnlyWatch(b *testing.B) {
	benchmark_Replace(b, false)
}

func benchmark_Replace(b *testing.B, watching bool) {
	numObjs := 1000

	tree := New[int](RootOnlyWatch)
	txn := tree.Txn()
	for i := 0; i < numObjs; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(numObjs+i))
		txn.Insert(key, numObjs+i)
	}

	b.ResetTimer()
	key := binary.BigEndian.AppendUint32(nil, uint32(0))
	for n := 0; n < b.N; n++ {
		txn.Insert(key, 0)
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "objects/sec")
}

func Benchmark_txn_1(b *testing.B) {
	benchmark_txn_batch(b, 1)
}

func Benchmark_txn_10(b *testing.B) {
	benchmark_txn_batch(b, 10)
}

func Benchmark_txn_100(b *testing.B) {
	benchmark_txn_batch(b, 100)
}

func Benchmark_txn_1000(b *testing.B) {
	benchmark_txn_batch(b, 1000)
}

func Benchmark_txn_10000(b *testing.B) {
	benchmark_txn_batch(b, 10000)
}

func Benchmark_txn_100000(b *testing.B) {
	benchmark_txn_batch(b, 100000)
}

func benchmark_txn_batch(b *testing.B, batchSize int) {
	tree := New[int](RootOnlyWatch)
	n := b.N
	for n > 0 {
		txn := tree.Txn()
		for j := 0; j < batchSize; j++ {
			txn.Insert(uint64Key(uint64(j)), j)
		}
		tree = txn.Commit()
		n -= batchSize
	}
	txn := tree.Txn()
	for j := 0; j < n; j++ {
		txn.Insert(uint64Key(uint64(j)), j)
	}
	txn.Commit()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "objects/sec")
}

func Benchmark_txn_delete_1(b *testing.B) {
	benchmark_txn_delete_batch(b, 1)
}

func Benchmark_txn_delete_10(b *testing.B) {
	benchmark_txn_delete_batch(b, 10)
}

func Benchmark_txn_delete_100(b *testing.B) {
	benchmark_txn_delete_batch(b, 100)
}

func Benchmark_txn_delete_1000(b *testing.B) {
	benchmark_txn_delete_batch(b, 1000)
}

func Benchmark_txn_delete_10000(b *testing.B) {
	benchmark_txn_delete_batch(b, 10000)
}

func Benchmark_txn_delete_100000(b *testing.B) {
	benchmark_txn_delete_batch(b, 100000)
}

func benchmark_txn_delete_batch(b *testing.B, batchSize int) {
	tree := New[int](RootOnlyWatch)
	for j := 0; j < batchSize; j++ {
		_, _, tree = tree.Insert(uint64Key(uint64(j)), j)
	}
	b.ResetTimer()

	n := b.N
	for n > 0 {
		txn := tree.Txn()
		for j := 0; j < batchSize; j++ {
			txn.Delete(uint64Key(uint64(j)))
		}
		n -= batchSize
	}
	txn := tree.Txn()
	for j := 0; j < n; j++ {
		txn.Delete(uint64Key(uint64(j)))
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "objects/sec")
}
