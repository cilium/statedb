// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package part

import (
	"sync"
	"testing"
)

func TestWatchStateCloseBeforeChannel(t *testing.T) {
	w := newWatchState()
	w.close()

	select {
	case <-w.channel():
	default:
		t.Fatal("watch created after close is open")
	}
}

func TestWatchStateConcurrentChannelAndClose(t *testing.T) {
	for range 100 {
		w := newWatchState()
		channels := make([]<-chan struct{}, 32)
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := range channels {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				if i%4 == 0 {
					w.close()
				}
				channels[i] = w.channel()
			}()
		}
		close(start)
		wg.Wait()
		w.close()

		for _, ch := range channels {
			select {
			case <-ch:
			default:
				t.Fatal("watch remained open after close")
			}
		}
	}
}

func TestNoWatchOperationsDoNotCreateChannels(t *testing.T) {
	tree := New[int]()
	_, _, tree = tree.Insert([]byte("ab"), 1)
	_, _, tree = tree.Insert([]byte("ac"), 2)

	if _, ok := tree.Get([]byte("ab")); !ok {
		t.Fatal("Get did not find key")
	}
	iter := tree.Prefix([]byte("a"))
	if _, _, ok := iter.Next(); !ok {
		t.Fatal("Prefix did not find key")
	}

	if tree.rootWatch.value.Load() != nil {
		t.Fatal("root watch channel created by a no-watch operation")
	}
	var check func(*header[int])
	check = func(n *header[int]) {
		if n.watch != nil && n.watch.value.Load() != nil {
			t.Fatalf("node %p has a channel after no-watch operations", n)
		}
		if leaf := n.getLeaf(); leaf != nil && !n.isLeaf() && leaf.watch.value.Load() != nil {
			t.Fatalf("leaf %p has a channel after no-watch operations", leaf)
		}
		for _, child := range n.children() {
			if child != nil {
				check(child)
			}
		}
	}
	check(tree.root)
}

func TestLateWatchOnInvalidatedSnapshot(t *testing.T) {
	tree := New[int]()
	_, _, tree = tree.Insert([]byte("key"), 1)
	oldTree := tree

	txn := tree.Txn()
	txn.Insert([]byte("key"), 2)
	_ = txn.Commit()
	txn.Notify()

	_, watch, ok := oldTree.GetWatch([]byte("key"))
	if !ok {
		t.Fatal("old snapshot lost key")
	}
	select {
	case <-watch:
	default:
		t.Fatal("watch requested after notification is open")
	}
}

func TestLateWatchAfterWatchPreservingRewrite(t *testing.T) {
	tree := New[int]()
	_, _, tree = tree.Insert([]byte("ab"), 1)
	oldTree := tree

	// Splitting the leaf changes its compressed prefix but preserves its
	// logical watch identity.
	_, _, tree = tree.Insert([]byte("abc"), 2)
	_, _, tree = tree.Insert([]byte("ab"), 3)

	_, watch, ok := oldTree.GetWatch([]byte("ab"))
	if !ok {
		t.Fatal("old snapshot lost key")
	}
	select {
	case <-watch:
	default:
		t.Fatal("watch on rewritten old snapshot is open after key changed")
	}
}
