// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package reconciler

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

	// Adding an item a second time will increment the number of retries and
	// recalculate when it should be retried.
	rq.Add(obj3, 3, false, err)

	<-rq.Wait()
	item1, ok := rq.Top()
	if assert.True(t, ok) {
		assert.True(t, item1.retryAt.Before(time.Now()), "expected item to be expired")
		assert.Equal(t, item1.object, obj1)

		rq.Pop()
		rq.Clear(item1.object)
	}

	<-rq.Wait()
	item2, ok := rq.Top()
	if assert.True(t, ok) {
		assert.True(t, item2.retryAt.Before(time.Now()), "expected item to be expired")
		assert.True(t, item2.retryAt.After(item1.retryAt), "expected item to expire later than previous")
		assert.Equal(t, item2.object, obj2)

		rq.Pop()
		rq.Clear(item2.object)
	}

	// Pop the last object. But don't clear its retry count.
	<-rq.Wait()
	item3, ok := rq.Top()
	if assert.True(t, ok) {
		assert.True(t, item3.retryAt.Before(time.Now()), "expected item to be expired")
		assert.True(t, item3.retryAt.After(item2.retryAt), "expected item to expire later than previous")
		assert.Equal(t, item3.object, obj3)

		rq.Pop()
	}

	// Queue should be empty now.
	item, ok := rq.Top()
	assert.False(t, ok)

	// Retry 'obj3' and since it was added back without clearing it'll be retried
	// later. Add obj1 and check that 'obj3' has later retry time.
	rq.Add(obj3, 4, false, err)
	rq.Add(obj1, 5, false, err)

	<-rq.Wait()
	item4, ok := rq.Top()
	if assert.True(t, ok) {
		assert.True(t, item4.retryAt.Before(time.Now()), "expected item to be expired")
		assert.Equal(t, item4.object, obj1)

		rq.Pop()
		rq.Clear(item4.object)
	}

	<-rq.Wait()
	item5, ok := rq.Top()
	if assert.True(t, ok) {
		assert.True(t, item5.retryAt.Before(time.Now()), "expected item to be expired")
		assert.True(t, item5.retryAt.After(item4.retryAt), "expected obj1 before obj3")
		assert.Equal(t, obj3, item5.object)

		// numRetries is 3 since 'obj3' was added to the queue 3 times and it has not
		// been cleared.
		assert.Equal(t, 3, item5.numRetries)

		rq.Pop()
		rq.Clear(item5.object)
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

func TestExponentialBackoff(t *testing.T) {
	backoff := exponentialBackoff{
		min: time.Millisecond,
		max: time.Second,
	}

	for i := range 1000 {
		dur := backoff.Duration(i)
		require.GreaterOrEqual(t, dur, backoff.min)
		require.LessOrEqual(t, dur, backoff.max)
	}
	require.Equal(t, backoff.Duration(0)*2, backoff.Duration(1))
}
