package statedb

import (
	"encoding/binary"
	"fmt"
	"iter"
	"math/rand"
	"net/netip"
	"reflect"
	"slices"
	"testing"
	"testing/quick"

	"github.com/cilium/statedb/index"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type lpmTestObject struct {
	ID            uint16
	Prefix        netip.Prefix
	Port          uint16
	PortPrefixLen PrefixLen
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

	lpmPrefixIndex = AddrLPMIndex[lpmTestObject]{
		Name: "prefix",
		FromObject: func(obj lpmTestObject) iter.Seq[netip.Prefix] {
			return Just(obj.Prefix)
		},
	}

	lpmPortIndex = LPMIndex[lpmTestObject]{
		Name: "port",
		FromObject: func(obj lpmTestObject) iter.Seq2[[]byte, PrefixLen] {
			return Just2(
				binary.BigEndian.AppendUint16(nil, obj.Port),
				obj.PortPrefixLen)
		},
		MaxPrefixLen: 16,
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

func TestQuick_LPM(t *testing.T) {
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

			return true
		}
		// Try queries against the WriteTxn first
		if !check(txn) {
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

	for n > 0 {
		txn := db.WriteTxn(tbl)
		toWrite := min(n, batchSize)
		for i := range toWrite {
			addr, _ := netip.AddrFromSlice(binary.BigEndian.AppendUint32(nil, uint32(i)))
			_, _, err := tbl.Insert(txn,
				lpmTestObject{
					ID:     uint16(i),
					Prefix: netip.PrefixFrom(addr, 32),
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

func TestLPM(t *testing.T) {
	lpm := newLPMTrie[int](32)

	cases := []struct {
		addr string
		len  int
	}{
		{"10.1.1.1", 32},    // 0
		{"10.0.0.0", 8},     // 1
		{"192.168.1.0", 24}, // 2
		{"192.168.1.5", 32}, // 3
	}

	prefix := func(p string) index.Key {
		key := netip.MustParsePrefix(p)
		return encodeLPMKey(key.Addr().AsSlice(), uint16(key.Bits()))
	}

	txn := lpm.Txn()
	for i, c := range cases {
		txn.Insert(prefix(fmt.Sprintf("%s/%d", c.addr, c.len)), i)
	}
	lpm = txn.Commit()

	lookupAddr := func(addrS string) (int, bool) {
		addr := netip.MustParseAddr(addrS)
		return lpm.Lookup(encodeLPMKey(addr.AsSlice(), 32))
	}

	for i, c := range cases {
		v, found := lookupAddr(c.addr)
		assert.True(t, found)
		assert.Equal(t, i, v)
	}
	v, found := lookupAddr("10.0.0.1")
	assert.True(t, found)
	assert.Equal(t, 1, v)
	v, found = lookupAddr("192.168.1.4")
	assert.True(t, found)
	assert.Equal(t, 2, v)
	v, found = lookupAddr("192.168.1.6")
	assert.True(t, found)
	assert.Equal(t, 2, v)

	var err error
	txn = lpm.Txn()
	txn.Insert(prefix("10.1.1.1/32"), 999)
	lpm = txn.Commit()
	require.NoError(t, err)
	v, found = lookupAddr("10.1.1.1")
	assert.True(t, found)
	assert.Equal(t, 999, v)

	txn = lpm.Txn()

	values := slices.Collect(Values(txn.Prefix(prefix("0.0.0.0/0"))))
	require.Equal(t, []int{1, 999, 2, 3}, values)

	values = slices.Collect(Values(txn.Prefix(prefix("10.0.0.0/8"))))
	require.Equal(t, []int{1, 999}, values)

	values = slices.Collect(Values(txn.Prefix(prefix("10.1.0.0/16"))))
	require.Equal(t, []int{999}, values)

	values = slices.Collect(Values(txn.Prefix(prefix("192.168.0.0/16"))))
	require.Equal(t, []int{2, 3}, values)

	values = slices.Collect(Values(txn.LowerBound(prefix("0.0.0.0/0"))))
	require.Equal(t, []int{1, 999, 2, 3}, values)

	values = slices.Collect(Values(txn.LowerBound(prefix("10.1.1.0/24"))))
	require.Equal(t, []int{999, 2, 3}, values)

	values = slices.Collect(Values(txn.LowerBound(prefix("100.1.2.3/32"))))
	require.Equal(t, []int{2, 3}, values)

	values = slices.Collect(Values(txn.LowerBound(prefix("192.168.1.4/32"))))
	require.Equal(t, []int{3}, values)

	values = slices.Collect(Values(txn.LowerBound(prefix("192.168.1.6/32"))))
	require.Empty(t, values)

	txn = lpm.Txn()
	txn.Delete(encodeLPMKey(netip.MustParseAddr("10.1.1.1").AsSlice(), 32))
	lpm = txn.Commit()

}

func TestEncodeDecodeLPMKey(t *testing.T) {
	roundtrip := func(data []byte, prefixLen PrefixLen) {
		key := encodeLPMKey(data, prefixLen)
		assert.Len(t, key, 2+((int(prefixLen)+7)/8))
		data2, prefixLen2 := decodeLPMKey(key)
		assert.Equal(t, prefixLen, prefixLen2)
		assert.Equal(t, data2, data[:len(data2)])
	}
	roundtrip([]byte{}, 0)
	roundtrip([]byte{0xa, 0xb}, 1)
	roundtrip([]byte{0xa, 0xb}, 7)
	roundtrip([]byte{0xa, 0xb}, 15)
	roundtrip([]byte{0xa, 0xb}, 16)

}
