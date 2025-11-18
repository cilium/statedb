// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package statedb

import (
	"encoding/binary"
	"fmt"
	"iter"
	"math/rand"
	"net/netip"
	"reflect"
	"testing"
	"testing/quick"

	"github.com/cilium/statedb/index"
	"github.com/cilium/statedb/lpm"
	"github.com/stretchr/testify/require"
)

type lpmTestObject struct {
	ID            uint16
	Prefix        netip.Prefix
	Port          uint16
	PortPrefixLen lpm.PrefixLen
}

// TableHeader implements TableWritable.
func (l lpmTestObject) TableHeader() []string {
	return []string{
		"ID",
		"Prefix",
	}
}

// TableRow implements TableWritable.
func (l lpmTestObject) TableRow() []string {
	return []string{
		fmt.Sprintf("%d", l.ID),
		l.Prefix.String(),
	}
}

var _ TableWritable = lpmTestObject{}

var (
	lpmIDIndex = Index[lpmTestObject, uint16]{
		Name: "id",
		FromObject: func(obj lpmTestObject) index.KeySet {
			return index.NewKeySet(index.Uint16(obj.ID))
		},
		FromKey:    index.Uint16,
		FromString: index.Uint16String,
		Unique:     true,
	}

	lpmPrefixIndex = NetIPPrefixIndex[lpmTestObject]{
		Name: "prefix",
		FromObject: func(obj lpmTestObject) iter.Seq[netip.Prefix] {
			return Just(obj.Prefix)
		},
	}

	lpmPortIndex = LPMIndex[lpmTestObject]{
		Name: "port",
		FromObject: func(obj lpmTestObject) iter.Seq2[[]byte, lpm.PrefixLen] {
			return Just2(
				binary.BigEndian.AppendUint16(nil, obj.Port),
				obj.PortPrefixLen)
		},
	}
)

func newLPMTestTable(db *DB) RWTable[lpmTestObject] {
	t, err := NewTable(
		db,
		"lpm-test",
		lpmIDIndex,
		lpmPrefixIndex,
		lpmPortIndex,
	)
	if err != nil {
		panic(err)
	}
	return t
}

func TestLPMIndex(t *testing.T) {
	db := New()
	tbl := newLPMTestTable(db)

	wtxn := db.WriteTxn(tbl)
	tbl.Insert(wtxn, lpmTestObject{
		ID:            0,
		Prefix:        netip.MustParsePrefix("1.0.0.0/8"),
		PortPrefixLen: 16,
	})
	tbl.Insert(wtxn, lpmTestObject{
		ID:            1,
		Prefix:        netip.MustParsePrefix("10.0.0.0/8"),
		Port:          0xff00,
		PortPrefixLen: 8,
	})
	tbl.Insert(wtxn, lpmTestObject{
		ID:            2,
		Prefix:        netip.MustParsePrefix("10.0.0.0/24"),
		PortPrefixLen: 16,
	})
	tbl.Insert(wtxn, lpmTestObject{
		ID:            3,
		Prefix:        netip.MustParsePrefix("10.0.0.1/32"),
		PortPrefixLen: 16,
	})
	tbl.Insert(wtxn, lpmTestObject{
		ID:            4,
		Prefix:        netip.MustParsePrefix("192.168.0.0/24"),
		PortPrefixLen: 16,
	})
	txn := wtxn.Commit()

	obj, _, found := tbl.Get(txn, lpmPortIndex.Query([]byte{0xff, 0x11}, 16))
	require.True(t, found)
	require.EqualValues(t, 1, obj.ID)

	obj, _, found = tbl.Get(txn, lpmPrefixIndex.Query(netip.MustParseAddr("10.0.1.1")))
	require.True(t, found)
	require.EqualValues(t, 1, obj.ID)

	objs := Collect(tbl.Prefix(txn, lpmPrefixIndex.QueryPrefix(netip.MustParsePrefix("10.0.0.0/8"))))
	require.Len(t, objs, 3)
	require.EqualValues(t, objs[0].ID, 1)
	require.EqualValues(t, objs[1].ID, 2)
	require.EqualValues(t, objs[2].ID, 3)

	objs = Collect(tbl.LowerBound(txn, lpmPrefixIndex.QueryPrefix(netip.MustParsePrefix("10.0.0.0/8"))))
	require.Len(t, objs, 4)
	require.EqualValues(t, objs[0].ID, 1)
	require.EqualValues(t, objs[1].ID, 2)
	require.EqualValues(t, objs[2].ID, 3)
	require.EqualValues(t, objs[3].ID, 4)

	// Since the LPM indexes are unique List() is the same as Get()
	objs = Collect(tbl.List(txn, lpmPrefixIndex.QueryPrefix(netip.MustParsePrefix("10.0.0.0/8"))))
	require.Len(t, objs, 1)
	require.EqualValues(t, objs[0].ID, 1)

	wtxn = db.WriteTxn(tbl)
	old, found, _ := tbl.Delete(wtxn, lpmTestObject{ID: 4})
	require.True(t, found)
	require.EqualValues(t, 4, old.ID)
	old, found, _ = tbl.Delete(wtxn, lpmTestObject{ID: 3})
	require.True(t, found)
	require.EqualValues(t, 3, old.ID)
	old, found, _ = tbl.Delete(wtxn, lpmTestObject{ID: 2})
	require.True(t, found)
	require.EqualValues(t, 2, old.ID)
	old, found, _ = tbl.Delete(wtxn, lpmTestObject{ID: 1})
	require.True(t, found)
	require.EqualValues(t, 1, old.ID)
	old, found, _ = tbl.Delete(wtxn, lpmTestObject{ID: 0})
	require.True(t, found)
	require.EqualValues(t, 0, old.ID)
	txn = wtxn.Commit()

	wtxn = db.WriteTxn(tbl)
	tbl.Insert(wtxn, lpmTestObject{
		ID:     4,
		Prefix: netip.MustParsePrefix("192.168.0.0/24"),
	})
	txn = wtxn.Commit()

}

func TestQuickLPMIndex(t *testing.T) {
	db := New()
	tbl := newLPMTestTable(db)
	values := map[uint16]netip.Prefix{}

	check := func(id uint16, prefix netip.Prefix, remove bool) bool {
		oldPrefix, oldExists := values[id]
		if oldExists {
			obj, _, found := tbl.Get(db.ReadTxn(), lpmIDIndex.Query(id))
			if !found {
				t.Logf("inserted object not found")
				return false
			}
			if obj.Prefix != oldPrefix {
				t.Logf("object has mismatching prefix")
				return false
			}
		}

		txn := db.WriteTxn(tbl)
		defer txn.Abort()
		if remove {
			oldObj, found, _ := tbl.Delete(txn, lpmTestObject{
				ID: id,
			})
			if oldExists != found {
				t.Logf("Delete did not return the old object")
				return false
			}
			if oldPrefix != oldObj.Prefix {
				t.Logf("prefix did not match the returned one")
			}
			txn.Commit()
			delete(values, id)
			return true
		}

		tbl.Insert(txn, lpmTestObject{
			ID:     id,
			Prefix: prefix,
		})
		values[id] = prefix

		check := func(rtxn ReadTxn) bool {
			obj, _, found := tbl.Get(rtxn, lpmIDIndex.Query(id))
			if !found {
				t.Logf("inserted object not found")
				return false
			}
			if obj.ID != id || obj.Prefix != prefix {
				t.Logf("object returned with wrong data")
				return false
			}

			obj, _, found = tbl.Get(rtxn, lpmPrefixIndex.QueryPrefix(prefix))
			if !found {
				t.Logf("object not found by prefix")
				return false
			}
			if obj.ID != id || obj.Prefix != prefix {
				t.Logf("object returned with wrong data")
				return false
			}

			for obj := range tbl.Prefix(rtxn, lpmPrefixIndex.QueryPrefix(prefix)) {
				if !prefix.Overlaps(obj.Prefix) {
					t.Logf("Prefix() returned object that was not under queried prefix")
					return false
				}
				if obj.Prefix.Bits() < prefix.Bits() {
					t.Logf("Prefix() returned object with wider prefix: %s is wider than %s", obj.Prefix, prefix)
					return false
				}
			}

			for obj := range tbl.LowerBound(rtxn, lpmPrefixIndex.Query(prefix.Addr())) {
				if obj.Prefix.Addr().Compare(prefix.Addr()) < 0 {
					t.Logf("LowerBound() returned object whose address was less than query address")
				}
			}

			return true
		}
		// Try queries against the WriteTxn first
		if !check(txn) {
			t.Logf("query failed against WriteTxn")
			return false
		}
		// And once more after commmitting with a ReadTxn
		return check(txn.Commit())
	}
	require.NoError(t, quick.Check(check, &quick.Config{
		MaxCount: 5000,

		// Make 1-8 byte long strings as input data. Keep the strings shorter
		// than the default quick value generation to hit the more interesting cases
		// often.
		Values: func(args []reflect.Value, rand *rand.Rand) {
			if len(args) != 3 {
				panic("unexpected args count")
			}
			bits := rand.Intn(32)
			var addrBytes [4]byte
			rand.Read(addrBytes[:])
			addr := netip.AddrFrom4(addrBytes)
			args[0] = reflect.ValueOf(uint16(rand.Intn(65535)))
			args[1] = reflect.ValueOf(netip.PrefixFrom(addr, bits).Masked())
			args[2] = reflect.ValueOf(rand.Intn(2) == 1)
		},
	}))
}

func BenchmarkDB_WriteTxn_100_LPMIndex(b *testing.B) {
	db := New()
	tbl := newLPMTestTable(db)
	batchSize := 100
	n := b.N

	var addrs []netip.Prefix
	for i := range n {
		addr, _ := netip.AddrFromSlice(binary.BigEndian.AppendUint32(nil, uint32(i)))
		addrs = append(addrs, netip.PrefixFrom(addr, 32))
	}
	b.ResetTimer()

	for n > 0 {
		txn := db.WriteTxn(tbl)
		toWrite := min(n, batchSize)
		for i := range toWrite {
			_, _, err := tbl.Insert(txn,
				lpmTestObject{
					ID:     uint16(i),
					Prefix: addrs[i],
				})
			if err != nil {
				b.Fatalf("Insert error: %s", err)
			}
		}
		txn.Commit()
		n -= toWrite
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "objects/sec")
}

func BenchmarkDB_WriteTxn_1_LPMIndex(b *testing.B) {
	db := New()
	tbl := newLPMTestTable(db)

	var addrs []netip.Prefix
	for i := range b.N {
		addr, _ := netip.AddrFromSlice(binary.LittleEndian.AppendUint32(nil, uint32(i)))
		addrs = append(addrs, netip.PrefixFrom(addr, 32))
	}
	b.ResetTimer()

	for i := range b.N {
		txn := db.WriteTxn(tbl)
		_, _, err := tbl.Insert(txn,
			lpmTestObject{
				ID:     uint16(i),
				Prefix: addrs[i],
			})
		if err != nil {
			b.Fatalf("Insert error: %s", err)
		}
		txn.Commit()
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "objects/sec")
}

func BenchmarkDB_LPMIndex_Get(b *testing.B) {
	db := New()
	tbl := newLPMTestTable(db)
	wtxn := db.WriteTxn(tbl)
	queries := []Query[lpmTestObject]{}
	for i := range 10000 {
		addr, _ := netip.AddrFromSlice(binary.BigEndian.AppendUint32(nil, uint32(i)))
		queries = append(queries, lpmPrefixIndex.Query(addr))
		tbl.Insert(wtxn,
			lpmTestObject{
				ID:     uint16(i),
				Prefix: netip.PrefixFrom(addr, 32),
			})
	}
	txn := wtxn.Commit()
	b.ResetTimer()

	for b.Loop() {
		for _, q := range queries {
			_, _, found := tbl.Get(txn, q)
			if !found {
				b.Fatalf("not found")
			}
		}
	}
	b.ReportMetric(float64(b.N*len(queries))/b.Elapsed().Seconds(), "objects/sec")
}
