// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package reconciler

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/cilium/statedb/index"
)

func TestRetries(t *testing.T) {
	objectToKey := func(o any) index.Key {
		return index.Uint64(o.(uint64))
	}
	rq := newRetries(time.Millisecond, 100*time.Millisecond, objectToKey)

	obj1, obj2, obj3 := uint64(1), uint64(2), uint64(3)

	// Add objects to be retried in order. We assume here that 'time.Time' has
	// enough granularity for these to be added with rising retryAt times.
	err := errors.New("some error")
	rq.Add(obj1, 1, false, err)
	rq.Add(obj2, 2, false, err)
	rq.Add(obj3, 3, false, err)

	errs := rq.errors()
	assert.Len(t, errs, 3)
	assert.Equal(t, err, errs[0])

	<-rq.Wait()
	item, ok := rq.Top()
	if assert.True(t, ok) {
		rq.Pop()
		rq.Clear(item.object)
		assert.True(t, item.retryAt.Before(time.Now()), "expected item to be expired")
		assert.Equal(t, item.object, obj1)
	}

	<-rq.Wait()
	item, ok = rq.Top()
	if assert.True(t, ok) {
		rq.Pop()
		rq.Clear(item.object)
		assert.True(t, item.retryAt.Before(time.Now()), "expected item to be expired")
		assert.Equal(t, item.object, obj2)
	}

	<-rq.Wait()
	item, ok = rq.Top()
	if assert.True(t, ok) {
		rq.Pop()
		assert.True(t, item.retryAt.Before(time.Now()), "expected item to be expired")
		assert.Equal(t, item.object, obj3)
	}

	// Retry 'obj3' and since it was added back without clearing it'll be retried
	// later. Add obj1 and check that 'obj3' has later retry time.
	rq.Add(obj3, 4, false, err)
	rq.Add(obj1, 5, false, err)

	<-rq.Wait()
	item, ok = rq.Top()
	if assert.True(t, ok) {
		rq.Pop()
		rq.Clear(item.object)
		assert.True(t, item.retryAt.Before(time.Now()), "expected item to be expired")
		assert.Equal(t, item.object, obj1)
	}
	retryAt1 := item.retryAt

	<-rq.Wait()
	item, ok = rq.Top()
	if assert.True(t, ok) {
		rq.Pop()
		rq.Clear(item.object)
		assert.True(t, retryAt1.Before(item.retryAt), "expected obj1 before obj3")
		assert.True(t, item.retryAt.Before(time.Now()), "expected item to be expired")
		assert.Equal(t, item.object, obj3)
	}

	_, ok = rq.Top()
	assert.False(t, ok)

	// Test that object can be cleared from the queue without popping it.
	rq.Add(obj1, 6, false, err)
	rq.Add(obj2, 7, false, err)
	rq.Add(obj3, 8, false, err)

	rq.Clear(obj1) // Remove obj1, testing that it'll fix the queue correctly.
	rq.Pop()       // Pop and remove obj2 and clear it to test that Clear doesn't mess with queue
	rq.Clear(obj2)
	item, ok = rq.Top()
	if assert.True(t, ok) {
		rq.Pop()
		rq.Clear(item.object)
		assert.Equal(t, item.object, obj3)
	}
	_, ok = rq.Top()
	assert.False(t, ok)

}
