// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package lpm

import "slices"

type Iterator[T any] struct {
	start *lpmNode[T]
	stack []*lpmNode[T]
}

type reverseFrame[T any] struct {
	node    *lpmNode[T]
	visited bool
}

type ReverseIterator[T any] struct {
	start *lpmNode[T]
	stack []reverseFrame[T]
}

func (it *Iterator[T]) All(yield func([]byte, T) bool) {
	if it == nil {
		return
	}
	var (
		// Use a stack allocated array for holding the next children
		// to explore. If this isn't large enough [append] will heap
		// allocate.
		stackArray [32]*lpmNode[T]

		stack []*lpmNode[T]
	)

	if it.start != nil {
		stack = stackArray[0:1:32]
		stack[0] = it.start
	} else if len(it.stack) < cap(stackArray) {
		stack = stackArray[:len(it.stack)]
		copy(stack, it.stack)
	} else {
		stack = slices.Clone(it.stack)
	}

	for len(stack) > 0 {
		node := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if node.children[1] != nil {
			stack = append(stack, node.children[1])
		}
		if node.children[0] != nil {
			stack = append(stack, node.children[0])
		}
		if !node.imaginary {
			if !yield(node.key, node.value) {
				return
			}
		}
	}
}

func (it *Iterator[T]) Next() (key []byte, value T, ok bool) {
	if it == nil {
		return
	}
	if it.start != nil {
		it.stack = []*lpmNode[T]{it.start}
		it.start = nil
	}

	for len(it.stack) > 0 {
		node := it.stack[len(it.stack)-1]
		it.stack = it.stack[:len(it.stack)-1]
		if node.children[1] != nil {
			it.stack = append(it.stack, node.children[1])
		}
		if node.children[0] != nil {
			it.stack = append(it.stack, node.children[0])
		}
		if !node.imaginary {
			return node.key, node.value, true
		}
	}
	return
}

func (it *ReverseIterator[T]) All(yield func([]byte, T) bool) {
	if it == nil {
		return
	}
	var (
		// Use a stack allocated array for holding the next nodes
		// to explore. If this isn't large enough [append] will heap
		// allocate.
		stackArray [32]reverseFrame[T]

		stack []reverseFrame[T]
	)

	if it.start != nil {
		stack = stackArray[0:1:32]
		stack[0] = reverseFrame[T]{node: it.start}
	} else if len(it.stack) < cap(stackArray) {
		stack = stackArray[:len(it.stack)]
		copy(stack, it.stack)
	} else {
		stack = slices.Clone(it.stack)
	}

	for len(stack) > 0 {
		frame := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if frame.node == nil {
			continue
		}
		if frame.visited {
			if !frame.node.imaginary {
				if !yield(frame.node.key, frame.node.value) {
					return
				}
			}
			continue
		}

		stack = append(stack, reverseFrame[T]{node: frame.node, visited: true})
		if frame.node.children[0] != nil {
			stack = append(stack, reverseFrame[T]{node: frame.node.children[0]})
		}
		if frame.node.children[1] != nil {
			stack = append(stack, reverseFrame[T]{node: frame.node.children[1]})
		}
	}
}

func (it *ReverseIterator[T]) Next() (key []byte, value T, ok bool) {
	if it == nil {
		return
	}
	if it.start != nil {
		it.stack = []reverseFrame[T]{{node: it.start}}
		it.start = nil
	}

	for len(it.stack) > 0 {
		frame := it.stack[len(it.stack)-1]
		it.stack = it.stack[:len(it.stack)-1]
		if frame.node == nil {
			continue
		}
		if frame.visited {
			if !frame.node.imaginary {
				return frame.node.key, frame.node.value, true
			}
			continue
		}

		it.stack = append(it.stack, reverseFrame[T]{node: frame.node, visited: true})
		if frame.node.children[0] != nil {
			it.stack = append(it.stack, reverseFrame[T]{node: frame.node.children[0]})
		}
		if frame.node.children[1] != nil {
			it.stack = append(it.stack, reverseFrame[T]{node: frame.node.children[1]})
		}
	}
	return
}
