//go:build !goexperiment.simd

package part

func (n *header[T]) findIndexN16(key byte) (*header[T], int) {
	n16 := n.node16()
	size := n16.size()
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
