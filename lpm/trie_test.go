// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package lpm

import (
	"encoding/binary"
	"fmt"
	"iter"
	"net/netip"
	"slices"
	"testing"
	"testing/quick"

	"github.com/cilium/statedb/index"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func iteratorToValues[T any](it *Iterator[T]) iter.Seq[T] {
	return func(yield func(T) bool) {
		for _, x := range it.All {
			if !yield(x) {
				break
			}
		}
	}
}

func TestTrie(t *testing.T) {
	lpm := New[int]()

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
		return EncodeLPMKey(key.Addr().AsSlice(), uint16(key.Bits()))
	}

	txn := lpm.Txn()
	for i, c := range cases {
		txn.Insert(prefix(fmt.Sprintf("%s/%d", c.addr, c.len)), i)
		assert.Equal(t, i+1, txn.size)
		// Double insert should not increase size
		txn.Insert(prefix(fmt.Sprintf("%s/%d", c.addr, c.len)), i)
	}
	lpm = txn.Commit()
	assert.Equal(t, len(cases), lpm.size)
	lpm.Print()

	lookupAddr := func(addrS string) (int, bool) {
		addr := netip.MustParseAddr(addrS)
		return lpm.Lookup(EncodeLPMKey(addr.AsSlice(), 32))
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

	values := slices.Collect(iteratorToValues(txn.Prefix(prefix("0.0.0.0/0"))))
	require.Equal(t, []int{1, 999, 2, 3}, values)

	values = slices.Collect(iteratorToValues(txn.Prefix(prefix("10.0.0.0/8"))))
	require.Equal(t, []int{1, 999}, values)

	values = slices.Collect(iteratorToValues(txn.Prefix(prefix("10.1.0.0/16"))))
	require.Equal(t, []int{999}, values)

	values = slices.Collect(iteratorToValues(txn.Prefix(prefix("192.168.0.0/16"))))
	require.Equal(t, []int{2, 3}, values)

	values = slices.Collect(iteratorToValues(txn.LowerBound(prefix("0.0.0.0/0"))))
	require.Equal(t, []int{1, 999, 2, 3}, values)

	values = slices.Collect(iteratorToValues(txn.LowerBound(prefix("10.1.1.0/24"))))
	require.Equal(t, []int{999, 2, 3}, values)

	values = slices.Collect(iteratorToValues(txn.LowerBound(prefix("100.1.2.3/32"))))
	require.Equal(t, []int{2, 3}, values)

	values = slices.Collect(iteratorToValues(txn.LowerBound(prefix("192.168.1.4/32"))))
	require.Equal(t, []int{3}, values)

	values = slices.Collect(iteratorToValues(txn.LowerBound(prefix("192.168.1.6/32"))))
	require.Empty(t, values)

	txn = lpm.Txn()
	v, found = txn.Delete(EncodeLPMKey(netip.MustParseAddr("10.1.1.1").AsSlice(), 32))
	lpm = txn.Commit()
	require.True(t, found)
	require.Equal(t, 999, v)
}

func TestEncodeDecodeLPMKey(t *testing.T) {
	maskData := func(data []byte, prefixLen PrefixLen) []byte {
		dataLen := (prefixLen + 7) / 8
		if dataLen == 0 {
			return []byte{}
		}
		masked := make([]byte, dataLen)
		copy(masked, data[:dataLen])
		if rem := prefixLen % 8; rem != 0 {
			masked[dataLen-1] &= 0xff << (8 - rem)
		}
		return masked
	}
	roundtrip := func(data []byte, prefixLen PrefixLen) {
		key := EncodeLPMKey(data, prefixLen)
		assert.Len(t, key, 2+((int(prefixLen)+7)/8))
		data2, prefixLen2 := DecodeLPMKey(key)
		assert.Equal(t, prefixLen, prefixLen2)
		assert.Equal(t, data2, maskData(data, prefixLen))
	}
	roundtrip([]byte{}, 0)
	roundtrip([]byte{0xa, 0xb}, 1)
	roundtrip([]byte{0xa, 0xb}, 7)
	roundtrip([]byte{0xa, 0xb}, 15)
	roundtrip([]byte{0xa, 0xb}, 16)
}

func TestQuickRoundTripEncodeDecodeLPMKey(t *testing.T) {
	maskData := func(data []byte, prefixLen PrefixLen) []byte {
		dataLen := (prefixLen + 7) / 8
		if dataLen == 0 {
			return []byte{}
		}
		masked := make([]byte, dataLen)
		copy(masked, data[:dataLen])
		if rem := prefixLen % 8; rem != 0 {
			masked[dataLen-1] &= 0xff << (8 - rem)
		}
		return masked
	}
	check := func(data []byte, prefixLen uint8) bool {
		prefixLen = prefixLen % 128
		prefixLen = min(prefixLen, uint8(len(data)*8))
		key := EncodeLPMKey(data, PrefixLen(prefixLen))
		assert.Len(t, key, 2+((int(prefixLen)+7)/8))
		data2, prefixLen2 := DecodeLPMKey(key)
		assert.Equal(t, PrefixLen(prefixLen), prefixLen2)
		assert.Equal(t, data2, maskData(data, PrefixLen(prefixLen)))
		return !t.Failed()
	}
	err := quick.Check(check, &quick.Config{MaxCount: 10000})
	require.NoError(t, err)
}

func TestQuickLPMTrie(t *testing.T) {
	trie := New[netip.Prefix]()
	values := map[netip.Prefix]netip.Prefix{}

	check := func(addrInt uint32, prefixLen uint8, shouldDelete bool) bool {
		prefixLen = prefixLen % 32
		var buf [4]byte
		binary.BigEndian.PutUint32(buf[:], addrInt)
		addr := netip.AddrFrom4(buf)

		// Find closest matching prefix from [values] to compare with
		// the LPM lookup results.
		var matchPrefix netip.Prefix
		var matchBits int
		for _, prefix := range values {
			if prefix.Contains(addr) && prefix.Bits() >= matchBits {
				matchPrefix = prefix
				matchBits = prefix.Bits()
			}
		}

		// Looking up by address will return a prefix that contains it.
		key := NetIPPrefix4ToIndexKey(netip.PrefixFrom(addr, 32))
		oldValue, oldFound := trie.Lookup(key)
		assert.Equal(t, matchPrefix.IsValid(), oldFound, "expected to match %s with %s (key %s)", matchPrefix, oldValue, showKey(key))
		if oldFound {
			assert.True(t, oldValue.Contains(addr))
			assert.Equal(t, matchPrefix.String(), oldValue.String(), "expected different prefix with address %s", addr)
		}

		prefix := netip.PrefixFrom(addr, int(prefixLen)).Masked()
		key = NetIPPrefix4ToIndexKey(prefix)

		// Look up if there's an existing prefix that contains the prefix
		// we're trying to insert.
		oldValue, oldFound = trie.Lookup(key)
		if oldFound {
			assert.True(t, oldValue.Overlaps(prefix))
		}

		// Check if the exact prefix was inserted earlier and verify it matches.
		expected, expectedFound := values[prefix]
		if expectedFound {
			assert.Equal(t, oldFound, expectedFound, "old value not found (key %s)", showKey(key))
			assert.Equal(t, oldValue, expected, "old value not equal (key %s)", showKey(key))
		}

		if shouldDelete {
			txn := trie.Txn()
			v, found := txn.Delete(key)
			if expectedFound {
				assert.True(t, found, "expected Delete to return the old object that existed (key %s)", showKey(key))
				assert.Equal(t, v.String(), values[prefix].String())
				delete(values, prefix)
			}
			trie = txn.Commit()
		} else {
			txn := trie.Txn()
			txn.Insert(key, prefix)
			values[prefix] = prefix
			trie = txn.Commit()
			newValue, found := trie.Lookup(key)
			assert.True(t, found)
			assert.Equal(t, prefix, newValue)
		}

		// Check that all previous values are still there after insert or delete.
		for prefix := range values {
			v, found := trie.Lookup(NetIPPrefix4ToIndexKey(prefix))
			assert.True(t, found)
			assert.Equal(t, v.String(), prefix.String())
		}

		return !t.Failed()
	}

	err := quick.Check(check, &quick.Config{MaxCount: 1000})
	if err != nil {
		trie.Print()
	}
	require.NoError(t, err)
}

// FuzzLPMTrie is a port of pkg/container/bitlpm/fuzz_test.go from cilium/cilium
// with bit of extension.
func FuzzLPMTrie(f *testing.F) {
	// has the fuzzing engine generate a set of []uint8, which it interprets as
	// a sequence of (val, prefixlen) pairs.

	// Then, checks invariants

	f.Add([]byte{0b1111_1111, 4})

	f.Fuzz(func(t *testing.T, sequence []byte) {

		type testEntry struct {
			k    uint8
			plen uint8
			val  uint16 // a placeholder
		}

		tree := New[testEntry]()

		seen := map[string]testEntry{}
		mask := func(v, bitcnt uint8) uint8 {
			return v & ^(0xff >> bitcnt)
		}

		// Insert every item in to the tree, recording the prefix in to a hash as well
		// so we know what we've set
		for i := 0; i < len(sequence)-1; i += 2 {
			k := sequence[i]
			prefixLen := sequence[i+1] % 8

			seenk := fmt.Sprintf("%#b/%d", mask(k, prefixLen), prefixLen)

			seen[seenk] = testEntry{
				k:    k,
				plen: prefixLen,
				val:  uint16(k)<<8 + uint16(prefixLen),
			}

			txn := tree.Txn()
			txn.Insert(EncodeLPMKey([]byte{k}, PrefixLen(prefixLen)), seen[seenk])
			tree = txn.Commit()
			if tree.size != len(seen) {
				t.Errorf("unexpected length after insert of %s: %d (expected %d), root %+v", seenk, tree.size, len(seen), tree.root)
			}
		}

		// Now, validate
		for seenK, seenV := range seen {
			val, found := tree.Lookup(EncodeLPMKey([]byte{seenV.k}, PrefixLen(seenV.plen)))
			if !found {
				t.Errorf("value %s not found", seenK)
			}
			if val.val != seenV.val {
				t.Errorf("seenKey %s: got val %#b expected %#b", seenK, val.val, seenV.val)
			}
		}

		// Now, delete seen keys and validate
		expectedLength := len(seen)
		for seenK, seenV := range seen {
			oldTree := tree

			// Check that the value is still in the tree
			val, found := tree.Lookup(EncodeLPMKey([]byte{seenV.k}, PrefixLen(seenV.plen)))
			if !found {
				t.Errorf("value %s not found", seenK)
			} else if val.val != seenV.val {
				t.Errorf("seenKey %s: got val %#b expected %#b", seenK, val.val, seenV.val)
			}

			txn := tree.Txn()
			v, found := txn.Delete(EncodeLPMKey([]byte{seenV.k}, PrefixLen(seenV.plen)))
			tree = txn.Commit()
			if !found {
				t.Errorf("value %s to be deleted not found, root %+v, size %d", seenK, oldTree.root, oldTree.size)
			} else if v.val != seenV.val {
				t.Errorf("delete %s: got val %#b expected %#b", seenK, v.val, seenV.val)
			}
			expectedLength--

			if tree.size != expectedLength {
				t.Errorf("unexpected length after deletion: %d (expected %d)", tree.size, expectedLength)
			}
		}
	})
}

func BenchmarkDB_LPMTxn_100_Insert(b *testing.B) {
	trie := New[int]()
	batchSize := 100

	var addrs []netip.Prefix
	for i := range b.N {
		addr, _ := netip.AddrFromSlice(binary.LittleEndian.AppendUint32(nil, uint32(i)))
		addrs = append(addrs, netip.PrefixFrom(addr, 32))
	}

	b.ResetTimer()

	n := b.N
	idx := 0
	for n > 0 {
		txn := trie.Txn()
		for range min(n, batchSize) {
			txn.Insert(NetIPPrefixToIndexKey(addrs[idx]), idx)
			idx++
			n--
		}
		trie = txn.Commit()
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "objects/sec")
}

func BenchmarkDB_LPMTxn_1_Insert(b *testing.B) {
	trie := New[int]()

	var addrs []netip.Prefix
	for i := range b.N {
		addr, _ := netip.AddrFromSlice(binary.BigEndian.AppendUint32(nil, uint32(i)))
		addrs = append(addrs, netip.PrefixFrom(addr, 32))
	}

	b.ResetTimer()

	for i := range b.N {
		txn := trie.Txn()
		txn.Insert(NetIPPrefixToIndexKey(addrs[i]), i)
		trie = txn.Commit()
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "objects/sec")
}
