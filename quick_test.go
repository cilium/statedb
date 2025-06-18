package statedb

import (
	"bytes"
	"cmp"
	"fmt"
	"iter"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"testing/quick"

	"github.com/cilium/statedb/index"
	"github.com/stretchr/testify/require"
)

var quickConfig = &quick.Config{
	MaxCount: 5000,

	// Make 1-8 byte long strings as input data. Keep the strings shorter
	// than the default quick value generation to hit the more interesting cases
	// often.
	Values: func(args []reflect.Value, rand *rand.Rand) {
		for i := range args {
			numBytes := 1 + rand.Intn(8)
			bs := make([]byte, numBytes)
			rand.Read(bs)
			args[i] = reflect.ValueOf(string(bs))
		}
	},
}

// Use an object with strings for both primary and secondary
// indexing. With non-unique indexes we'll create a composite
// key out of them by concatenation and thus we need test that
// this does not break iteration order etc.
type quickObj struct {
	A, B string
}

func (q quickObj) String() string {
	return fmt.Sprintf("%x %x", []byte(q.A), []byte(q.B))
}

var (
	aIndex = Index[quickObj, string]{
		Name: "a",
		FromObject: func(t quickObj) index.KeySet {
			return index.NewKeySet(index.String(t.A))
		},
		FromKey:    index.String,
		FromString: index.FromString,
		Unique:     true,
	}

	bIndex = Index[quickObj, string]{
		Name: "b",
		FromObject: func(t quickObj) index.KeySet {
			return index.NewKeySet(index.String(t.B))
		},
		FromKey:    index.String,
		FromString: index.FromString,
		Unique:     false,
	}
)

func isOrdered(t *testing.T, aFirst bool, it iter.Seq2[quickObj, Revision]) bool {
	var prev quickObj
	for a := range it {
		if aFirst {
			if ret := cmp.Compare(a.A, prev.A); ret < 0 {
				t.Logf("isOrdered(A): %s < %s!", a, prev)
				return false
			} else if ret == 0 && cmp.Compare(a.B, prev.B) < 0 {
				t.Logf("isOrdered(B): %s < %s!", a, prev)
				return false
			}
		} else {
			if ret := cmp.Compare(a.B, prev.B); ret < 0 {
				t.Logf("isOrdered(B): %s < %s!", a, prev)
				return false
			} else if ret == 0 && cmp.Compare(a.A, prev.A) < 0 {
				t.Logf("isOrdered(A): %s < %s!", a, prev)
				return false
			}
		}

		prev = a
	}
	return true
}

func seqLen[A, B any](it iter.Seq2[A, B]) int {
	n := 0
	for range it {
		n++
	}
	return n
}

func TestDB_Quick(t *testing.T) {
	table, err := NewTable("test", aIndex, bIndex)
	require.NoError(t, err, "NewTable")
	db := New()
	require.NoError(t, db.RegisterTable(table), "RegisterTable")

	anyTable := AnyTable{table}

	numExpected := 0

	check := func(a, b string) bool {
		txn := db.WriteTxn(table)
		_, hadOld, err := table.Insert(txn, quickObj{a, b})
		require.NoError(t, err, "Insert")
		if !hadOld {
			numExpected++
		}
		if numExpected != table.NumObjects(txn) {
			t.Logf("wrong object count")
			return false
		}

		txn.Commit()
		rtxn := db.ReadTxn()

		if numExpected != table.NumObjects(rtxn) {
			t.Logf("wrong object count")
			return false
		}

		//
		// Check queries against the primary index
		//

		if numExpected != seqLen(table.All(rtxn)) {
			t.Logf("All() via aIndex wrong length")
			return false
		}
		if numExpected != seqLen(table.Prefix(rtxn, aIndex.Query(""))) {
			t.Logf("Prefix() via aIndex wrong length")
			return false
		}
		if numExpected != seqLen(table.LowerBound(rtxn, aIndex.Query(""))) {
			t.Logf("LowerBound() via aIndex wrong length")
			return false
		}

		obj, _, found := table.Get(rtxn, aIndex.Query(a))
		if !found || obj.A != a {
			t.Logf("Get() via aIndex")
			return false
		}
		for obj := range table.Prefix(rtxn, aIndex.Query(a)) {
			if !strings.HasPrefix(obj.A, a) {
				t.Logf("Prefix() returned object with wrong prefix via aIndex")
				return false
			}
		}
		anyObjs, err := anyTable.Prefix(rtxn, "a", a)
		require.NoError(t, err, "AnyTable.Prefix")
		for anyObj := range anyObjs {
			obj := anyObj.(quickObj)
			if !strings.HasPrefix(obj.A, a) {
				t.Logf("AnyTable.Prefix() returned object with wrong prefix via aIndex")
				return false
			}
		}

		for obj := range table.LowerBound(rtxn, aIndex.Query(a)) {
			if cmp.Compare(obj.A, a) < 0 {
				t.Logf("LowerBound() order wrong")
				return false
			}
		}
		anyObjs, err = anyTable.LowerBound(rtxn, "a", a)
		require.NoError(t, err, "AnyTable.LowerBound")
		for anyObj := range anyObjs {
			obj := anyObj.(quickObj)
			if cmp.Compare(obj.A, a) < 0 {
				t.Logf("AnyTable.LowerBound(%x) order wrong: %x < %x", []byte(a), []byte(obj.A), []byte(a))
				return false
			}
		}

		if !isOrdered(t, true, table.All(rtxn)) {
			t.Logf("All() wrong order")
			return false
		}
		if !isOrdered(t, true, table.Prefix(rtxn, aIndex.Query(""))) {
			t.Logf("Prefix() via aIndex wrong order")
			return false
		}
		if !isOrdered(t, true, table.LowerBound(rtxn, aIndex.Query(""))) {
			t.Logf("LowerBound() via aIndex wrong order")
			return false
		}

		//
		// Check against the secondary (non-unique index)
		//

		// Non-unique indexes return the same number of objects as we've inserted.
		if numExpected != seqLen(table.Prefix(rtxn, bIndex.Query(""))) {
			t.Logf("Prefix() via bIndex wrong length")
			return false
		}

		if numExpected != seqLen(table.LowerBound(rtxn, bIndex.Query(""))) {
			t.Logf("LowerBound() via bIndex wrong length")
			return false
		}

		// Get returns the first match, but since the index is non-unique, this might
		// not be the one that we just inserted.
		obj, _, found = table.Get(rtxn, bIndex.Query(b))
		if !found || obj.B != b {
			t.Logf("Get(%q) via bIndex not found (%v) or wrong B (%q vs %q)", b, found, obj.B, b)
			return false
		}

		found = false
		for obj := range table.List(rtxn, bIndex.Query(b)) {
			if obj.B != b {
				t.Logf("List() via bIndex wrong B")
				return false
			}
			if obj.A == a {
				found = true
			}
		}
		if !found {
			t.Logf("List() via bIndex, object not found")
			return false
		}

		visited := map[string]struct{}{}
		for obj := range table.Prefix(rtxn, bIndex.Query(b)) {
			if !strings.HasPrefix(obj.B, b) {
				t.Logf("Prefix() via bIndex has wrong prefix")
				return false
			}
			if _, found := visited[obj.A]; found {
				t.Logf("Prefix() visited object %q twice", obj.A)
				return false
			}
			visited[obj.A] = struct{}{}
		}

		anyObjs, err = anyTable.Prefix(rtxn, "b", b)
		require.NoError(t, err, "AnyTable.Prefix")
		for anyObj := range anyObjs {
			obj := anyObj.(quickObj)
			if !strings.HasPrefix(obj.B, b) {
				t.Logf("AnyTable.Prefix() via bIndex has wrong prefix: %q vs %q", obj.B, b)
				return false
			}
		}

		visited = map[string]struct{}{}
		for obj := range table.LowerBound(rtxn, bIndex.Query(b)) {
			if cmp.Compare(obj.B, b) < 0 {
				t.Logf("LowerBound() via bIndex has wrong objects, expected %v >= %v", []byte(obj.B), []byte(b))
				return false
			}
			if _, found := visited[obj.A]; found {
				t.Logf("Prefix() visited object %q twice", obj.A)
				return false
			}
			visited[obj.A] = struct{}{}
		}

		anyObjs, err = anyTable.LowerBound(rtxn, "b", b)
		require.NoError(t, err, "AnyTable.LowerBound")
		for anyObj := range anyObjs {
			obj := anyObj.(quickObj)
			if cmp.Compare(obj.B, b) < 0 {
				t.Logf("AnyTable.LowerBound() via bIndex has wrong objects, expected %v >= %v", []byte(obj.B), []byte(b))
				return false
			}
		}

		// Iterating over the secondary index returns the objects in order
		// defined by the "B" key first and then by the "A" key.
		if !isOrdered(t, false, table.Prefix(rtxn, bIndex.Query(""))) {
			t.Logf("Prefix() via bIndex wrong order")
			rtxn.mustIndexReadTxn(table, table.indexPos("b")).PrintTree()
			return false
		}
		if !isOrdered(t, false, table.LowerBound(rtxn, bIndex.Query(""))) {
			t.Logf("Prefix() via bIndex wrong order")
			return false
		}
		return true
	}

	require.NoError(t, quick.Check(check, quickConfig))
}

func Test_Quick_nonUniqueKey(t *testing.T) {
	check := func(p1, s1, p2, s2 []byte) bool {
		key1 := encodeNonUniqueKey(p1, s1)
		expectedLen := encodedLength(p1) + 1 + encodedLength(s1) + 2
		minLen := len(p1) + 1 + len(s1) + 2
		if expectedLen < minLen {
			t.Logf("expected length too short (%d), must be >= %d", expectedLen, minLen)
			return false
		}
		if len(key1) != expectedLen {
			t.Logf("length mismatch, expected %d, got %d", expectedLen, len(key1))
			return false
		}

		nuk1 := nonUniqueKey(key1)
		if len(nuk1.encodedPrimary()) < len(p1) {
			t.Logf("encodedPrimary() length (%d) shorter than original (%d)", len(nuk1.encodedPrimary()), len(p1))
			return false
		}
		if len(nuk1.encodedPrimary()) != encodedLength(p1) {
			t.Logf("encodedPrimary() length (%d) does not match encodedLength() (%d)", len(nuk1.encodedPrimary()), len(p1))
			return false
		}
		if len(nuk1.encodedSecondary()) < len(s1) {
			t.Logf("encodedSecondary() length (%d) shorter than original (%d)", len(nuk1.encodedSecondary()), len(s1))
		}
		if len(nuk1.encodedSecondary()) != encodedLength(s1) {
			t.Logf("encodedSecondary() length (%d) does not match encodedLength() (%d)", len(nuk1.encodedSecondary()), len(s1))
			return false
		}

		// Do another key and check that ordering is preserved.
		key2 := encodeNonUniqueKey(p2, s2)
		scmp := bytes.Compare(s1, s2)
		pcmp := bytes.Compare(p1, p2)
		kcmp := bytes.Compare(key1, key2)

		if (scmp == 0 && pcmp != kcmp) /* secondary key matches, primary key determines order */ ||
			(scmp != 0 && scmp != kcmp) /* secondary key determines order */ {
			t.Logf("ordering not preserved: key1=%v key2=%v, p1=%v, s1=%v, p2=%v, s2=%v", key1, key2, p1, s1, p2, s2)
			return false
		}
		return true
	}
	require.NoError(t, quick.Check(check, &quick.Config{
		MaxCount: 50000,
		Values: func(args []reflect.Value, rand *rand.Rand) {
			for i := range args {
				numBytes := 1 + rand.Intn(8)
				bs := make([]byte, numBytes)
				rand.Read(bs)
				args[i] = reflect.ValueOf(bs)
			}
		},
	}))
}
