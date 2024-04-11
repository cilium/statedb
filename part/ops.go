// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package part

// Ops is the common operations that can be performed with a Tree
// or Txn.
type Ops[T any] interface {
	Len() int
	Get(key []byte) (T, <-chan struct{}, bool)
	Prefix(key []byte) (*Iterator[T], <-chan struct{})
	LowerBound(key []byte) *Iterator[T]
	RootWatch() <-chan struct{}
	Iterator() *Iterator[T]
	PrintTree()
}

var (
	_ Ops[int] = &Tree[int]{}
	_ Ops[int] = &Txn[int]{}
)
