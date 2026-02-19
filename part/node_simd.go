//go:build goexperiment.simd

package part

import (
	"math/bits"
	"simd/archsimd"
)

func (n *header[T]) findIndexN16(key byte) (*header[T], int) {
	n16 := n.node16()
	size := n16.size()

	if archsimd.X86.AVX() && archsimd.X86.AVX2() {
		keys := archsimd.LoadUint8x16(&n16.keys)
		key16 := archsimd.BroadcastUint8x16(key)
		sizeMask := (uint16(1) << size) - 1
		eqmask := keys.Equal(key16).ToBits() & sizeMask
		if eqmask == 0 {
			gtmask := keys.Greater(key16).ToBits() & sizeMask
			return nil, min(bits.TrailingZeros16(gtmask), int(size))
		}

		idx := bits.TrailingZeros16(eqmask)
		return n16.children[idx], idx
	}

	// Fallback non-SIMD implementation.
	for i := 0; i < int(size); i++ {
		k := n16.keys[i]
		if k >= key {
			if k == key {
				return n16.children[i], i
			}
			return nil, i
		}
	}

	return nil, size

}
