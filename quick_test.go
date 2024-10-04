package statedb

import (
	"cmp"
	"iter"
	"strings"
	"testing"
	"testing/quick"

	"github.com/cilium/statedb/index"
	"github.com/stretchr/testify/require"
)

var quickConfig = &quick.Config{
	MaxCount: 5000,
}

// Use an object with strings for both primary and secondary
// indexing. With non-unique indexes we'll create a composite
// key out of them by concatenation and thus we need test that
// this does not break iteration order etc.
type quickObj struct {
	A, B string
}

func (q quickObj) getA() string {
	return q.A
}
func (q quickObj) getB() string {
	return q.B
}

var (
	aIndex = Index[quickObj, string]{
		Name: "a",
		FromObject: func(t quickObj) index.KeySet {
			return index.NewKeySet(index.String(t.A))
		},
		FromKey: index.String,
		Unique:  true,
	}

	bIndex = Index[quickObj, string]{
		Name: "b",
		FromObject: func(t quickObj) index.KeySet {
			return index.NewKeySet(index.String(t.B))
		},
		FromKey: index.String,
		Unique:  false,
	}
)

func isOrdered[A cmp.Ordered, B any](t *testing.T, it iter.Seq2[A, B]) bool {
	var prev A
	for a := range it {
		if cmp.Compare(a, prev) < 0 {
			t.Logf("isOrdered: %#v < %#v!", a, prev)
			return false
		}
		prev = a
	}
	return true
}

func seqLen[A cmp.Ordered, B any](it iter.Seq2[A, B]) int {
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

		if numExpected != seqLen(Map(table.All(rtxn), quickObj.getA)) {
			t.Logf("All() via aIndex wrong length")
			return false
		}
		if numExpected != seqLen(Map(table.Prefix(rtxn, aIndex.Query("")), quickObj.getA)) {
			t.Logf("Prefix() via aIndex wrong length")
			return false
		}
		if numExpected != seqLen(Map(table.LowerBound(rtxn, aIndex.Query("")), quickObj.getA)) {
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
		for anyObj := range anyTable.Prefix(rtxn, a) {
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
		for anyObj := range anyTable.LowerBound(rtxn, a) {
			obj := anyObj.(quickObj)
			if cmp.Compare(obj.A, a) < 0 {
				t.Logf("AnyTable.LowerBound() order wrong")
				return false
			}
		}

		if !isOrdered(t, Map(table.All(rtxn), quickObj.getA)) {
			t.Logf("All() wrong order")
			return false
		}
		if !isOrdered(t, Map(table.Prefix(rtxn, aIndex.Query("")), quickObj.getA)) {
			t.Logf("Prefix() via aIndex wrong order")
			return false
		}
		if !isOrdered(t, Map(table.LowerBound(rtxn, aIndex.Query("")), quickObj.getA)) {
			t.Logf("LowerBound() via aIndex wrong order")
			return false
		}

		//
		// Check against the secondary (non-unique index)
		//

		// Non-unique indexes return at least as many objects as we've inserted.
		if numExpected > seqLen(Map(table.Prefix(rtxn, bIndex.Query("")), quickObj.getB)) {
			t.Logf("Prefix() via bIndex wrong length")
			return false
		}
		if numExpected > seqLen(Map(table.LowerBound(rtxn, bIndex.Query("")), quickObj.getB)) {
			t.Logf("LowerBOund() via bIndex wrong length")
			return false
		}

		// Get returns the first match, but since the index is non-unique, this might
		// not be the one that we just inserted.
		obj, _, found = table.Get(rtxn, bIndex.Query(b))
		if !found || obj.B != b {
			t.Logf("Get() via bIndex not found or wrong B")
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

		for obj := range table.Prefix(rtxn, bIndex.Query(b)) {
			if !strings.HasPrefix(obj.B, b) {
				t.Logf("Prefix() via bIndex has wrong prefix")
				return false
			}
		}
		for obj := range table.LowerBound(rtxn, bIndex.Query(b)) {
			if cmp.Compare(obj.B, b) < 0 {
				t.Logf("LowerBound() via bIndex has wrong objects, expected %v >= %v", []byte(obj.B), []byte(b))
				return false
			}
		}

		// Iterating over the secondary index returns the objects in order
		// defined by the "B" key.
		if !isOrdered(t, Map(table.Prefix(rtxn, bIndex.Query("")), quickObj.getB)) {
			t.Logf("Prefix() via bIndex has wrong order")
			return false
		}
		if !isOrdered(t, Map(table.LowerBound(rtxn, bIndex.Query("")), quickObj.getB)) {
			t.Logf("LowerBound() via bIndex has wrong order")
			return false
		}
		return true
	}

	require.NoError(t, quick.Check(check, quickConfig))
}
