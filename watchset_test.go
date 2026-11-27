// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package statedb

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cilium/statedb/part"
	"github.com/stretchr/testify/require"
)

func TestWatchSet(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		// NOTE: TestMain calls goleak.VerifyTestMain so we know this test doesn't leak goroutines.

		ws := NewWatchSet()

		// Empty watch set, cancelled context.
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := ws.Wait(ctx, time.Second)
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, receivedWatchCount(ws))

		// One closed channel with non-zero settle time
		ch1 := make(chan struct{})
		close(ch1)
		ws.Add(ch1)
		t0 := time.Now()
		duration := 50 * time.Millisecond
		err = ws.Wait(context.Background(), duration)
		require.NoError(t, err)
		require.True(t, ws.Received(ch1))
		require.Equal(t, duration, time.Since(t0), "expected to wait to settle")

		// Few channels, cancelled context.
		ch1 = make(chan struct{})
		ch2 := make(chan struct{})
		ch3 := make(chan struct{})
		ws.Add(ch1, ch2, ch3)
		ctx, cancel = context.WithCancel(context.Background())
		cancel()
		err = ws.Wait(ctx, time.Second)
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, receivedWatchCount(ws))

		// Few channels, timed out context.
		duration = 10 * time.Millisecond
		t0 = time.Now()
		ctx, cancel = context.WithTimeout(context.Background(), duration)

		// Wait with a settle time of 1ns. Since settle time only comes to play
		// after at least one channel has closed this will block until [ctx]
		// cancels (e.g. for [duration]).
		err = ws.Wait(ctx, time.Nanosecond)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Zero(t, receivedWatchCount(ws))
		require.Equal(t, duration, time.Since(t0), "expected to wait until context times out")
		cancel()

		// One closed channel. Should wait until 'settleTime' expires.
		close(ch1)
		t0 = time.Now()
		err = ws.Wait(context.Background(), duration)
		require.NoError(t, err)
		require.True(t, ws.Received(ch1))
		require.Equal(t, duration, time.Since(t0), "expected to wait until settle time expires")

		// One closed channel, 0 wait time.
		ws = NewWatchSet()
		ws.Add(ch2)
		close(ch2)
		err = ws.Wait(context.Background(), 0)
		require.NoError(t, err)
		require.True(t, ws.Received(ch2))

		// Many channels
		for _, numChans := range []int{2, 16, 31, 1024} {
			var chans []chan struct{}
			var rchans []<-chan struct{}
			for range numChans {
				ch := make(chan struct{})
				chans = append(chans, ch)
				rchans = append(rchans, ch)
			}
			ws.Clear()
			ws.Add(rchans...)

			i, j := rand.Intn(numChans), rand.Intn(numChans)
			for j == i {
				j = rand.Intn(numChans)
			}
			close(chans[i])
			close(chans[j])
			err := ws.Wait(context.Background(), 50*time.Millisecond)
			require.NoError(t, err)
			for k, ch := range rchans {
				require.Equal(t, k == i || k == j, ws.Received(ch))
			}
			cancel()
		}
	})
}

func TestWatchSetInQueries(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		db, table := newTestDBWithMetrics(t, &NopMetrics{}, tagsIndex)

		ws := NewWatchSet()
		txn := db.ReadTxn()
		_, watchAll := table.AllWatch(txn)

		// Should timeout as watches should not have closed yet.
		ws.Add(watchAll)
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		err := ws.Wait(ctx, time.Second)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Zero(t, receivedWatchCount(ws))
		cancel()

		// Insert some objects
		wtxn := db.WriteTxn(table)
		table.Insert(wtxn, &testObject{ID: 1})
		table.Insert(wtxn, &testObject{ID: 2})
		table.Insert(wtxn, &testObject{ID: 3})
		txn = wtxn.Commit()

		// The 'watchAll' channel should now have closed and Wait() returns.
		ws.Add(watchAll)
		err = ws.Wait(context.Background(), 100*time.Millisecond)
		require.NoError(t, err)
		require.True(t, ws.Received(watchAll))
		ws.Clear()

		// Try watching specific objects for changes.
		_, _, watch1, _ := table.GetWatch(txn, idIndex.Query(1))
		_, _, watch2, _ := table.GetWatch(txn, idIndex.Query(2))
		_, _, watch3, _ := table.GetWatch(txn, idIndex.Query(3))

		wtxn = db.WriteTxn(table)
		table.Insert(wtxn, &testObject{ID: 1, Tags: part.NewSet("foo")})
		wtxn.Commit()

		// Use a new WatchSet and merge it. This allows having "subsets" that we
		// can then use to check whether the closed channel affected the subset.
		ws2 := NewWatchSet()
		ws2.Add(watch3, watch2, watch1)

		// Merge into the larger WatchSet. This still leaves all the channels
		// in ws2.
		ws.Merge(ws2)

		err = ws.Wait(context.Background(), 100*time.Millisecond)
		require.NoError(t, err)
		require.True(t, ws.Received(watch1))
		require.False(t, ws.Received(watch2, watch3))
		require.True(t, ws.ReceivedSet(ws2))

		ws2.Clear()
		require.False(t, ws.ReceivedSet(ws2))
	})
}

func TestWatchSetCancellationDuringSettle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ws := NewWatchSet()

		// Add a closed watch channel
		ch1 := make(chan struct{})
		close(ch1)
		ws.Add(ch1)

		// Use a context that times out in less time than settle time.
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()

		// Wait should now go into settle time wait. The context would cancel
		// before the settle time expires.
		err := ws.Wait(ctx, time.Second)

		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.True(t, ws.Received(ch1), "readiness persists until next Wait")

		// Test again with more than one channel
		ch2 := make(chan struct{})
		ws.Add(ch1, ch2)
		ctx, cancel = context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		err = ws.Wait(ctx, time.Second)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.True(t, ws.Received(ch1), "readiness persists until next Wait")
		require.False(t, ws.Received(ch2))
	})
}

func TestWatchSetSettleWindow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ws := NewWatchSet()
		first := time.After(2 * time.Second)
		within := time.After(2500 * time.Millisecond)
		outside := time.After(3500 * time.Millisecond)
		ws.Add(first, within, outside)

		start := time.Now()
		require.NoError(t, ws.Wait(context.Background(), time.Second))
		require.Equal(t, 3*time.Second, time.Since(start))
		require.True(t, ws.Received(first))
		require.True(t, ws.Received(within))
		require.False(t, ws.Received(outside))

		require.NoError(t, ws.Wait(context.Background(), 0))
		require.Equal(t, 3500*time.Millisecond, time.Since(start))
		require.True(t, ws.Received(outside))
	})
}

func TestWatchSetHas(t *testing.T) {
	type namedChannel chan int
	type namedReceiveChannel <-chan int
	var ws WatchSet
	ch := make(chan int, 1)
	other := make(chan int)
	require.False(t, ws.Has(ch))
	require.False(t, ws.Has((<-chan int)(nil)))
	ch <- 42
	ws.Add(ch)
	require.True(t, ws.Has(ch))
	require.True(t, ws.Has((<-chan int)(ch)))
	require.True(t, ws.Has(namedChannel(ch)))
	require.True(t, ws.Has(namedReceiveChannel(ch)))
	require.False(t, ws.Has(other))
	require.False(t, ws.Received(ch))
	require.Len(t, ch, 1, "Has must not receive from the channel")

	require.NoError(t, ws.Wait(context.Background(), 0))
	require.True(t, ws.Has(ch), "received channels remain until the next Wait")
	require.True(t, ws.Received(ch))
	value, found := ws.Get(ch)
	require.True(t, found)
	require.Equal(t, 42, value, "Has must not clear the received value")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, ws.Wait(ctx, 0), context.Canceled)
	require.False(t, ws.Has(ch), "the next Wait removes received channels")
	ws.Add(ch)
	require.ErrorIs(t, ws.Wait(ctx, 0), context.Canceled)
	require.True(t, ws.Has(ch), "cancellation retains pending channels")
	ws.Clear()
	require.False(t, ws.Has(ch))

	var nilChannel <-chan int
	ws.Add(nilChannel)
	require.True(t, ws.Has(nilChannel), "membership includes added nil channels")
	ws.Clear()
	require.False(t, ws.Has(nilChannel))
}

func TestWatchSetDelete(t *testing.T) {
	type namedReceiveChannel <-chan int
	var ws WatchSet
	pending := make(chan int, 1)
	pending <- 1
	require.False(t, ws.Delete(pending))
	ws.Add(pending)
	require.True(t, ws.Delete(namedReceiveChannel(pending)))
	require.False(t, ws.Has(pending))
	require.False(t, ws.Delete(pending))

	received := make(chan int, 1)
	received <- 42
	ws.Add(received)
	require.NoError(t, ws.Wait(context.Background(), 0))
	require.Len(t, pending, 1, "Wait must not consume from the deleted channel")
	value, ok := ws.Get(received)
	require.True(t, ok)
	require.Equal(t, 42, value)

	require.True(t, ws.Delete((<-chan int)(received)))
	require.False(t, ws.Has(received))
	require.False(t, ws.Received(received))
	_, ok = ws.Get(received)
	require.False(t, ok)
	require.False(t, ws.Delete(received))
}

func TestWatchSetTypedChannels(t *testing.T) {
	ws := NewWatchSet()
	ch := make(chan int, 2)
	ch <- 42
	ch <- 43
	ws.Add(ch, ch, ch)
	require.NoError(t, ws.Wait(context.Background(), 0))
	require.Len(t, ch, 1, "duplicates must consume only one value")
	require.Len(t, ws.channels, 1)
	require.True(t, ws.Received(ch))
	require.True(t, ws.ReceivedSeq(slices.Values([]<-chan int{ch})))
	later := make(chan int)
	ws.Add(later)
	require.True(t, ws.Received(ch), "Add preserves the last Wait's readiness")
	require.False(t, ws.Received(later))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, ws.Wait(ctx, 0), context.Canceled)
	require.False(t, ws.Received(ch), "previous received channel is removed")
	ws.Clear()
	require.Empty(t, ws.channels)
	require.False(t, ws.Received(ch, later))
}

func TestWatchSetTimer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ws := NewWatchSet()
		timer := time.NewTimer(time.Millisecond)
		defer timer.Stop()
		ws.Add(timer.C)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		require.NoError(t, ws.Wait(ctx, 0))
		require.True(t, ws.Received(timer.C))
	})
}

func TestWatchSetSelfOperations(t *testing.T) {
	ws := NewWatchSet()
	require.True(t, ws.ReceivedSet(ws))
}

func TestWatchSetDedupAndMerge(t *testing.T) {
	ws, other := NewWatchSet(), NewWatchSet()
	received := make(chan int, 2)
	received <- 1
	received <- 2
	other.Add(received)
	require.NoError(t, other.Wait(context.Background(), 0))
	ws.Merge(other)
	require.Empty(t, ws.channels, "Merge excludes previously received channels")

	pending := make(chan string)
	other.Add(pending)
	ws.Merge(other)
	require.Len(t, ws.channels, 1)
	var nilChannel <-chan int
	ws.Add(nilChannel, nilChannel)
	for range 128 {
		ws.Add(received)
	}
	require.NoError(t, ws.Wait(context.Background(), 0))
	require.True(t, ws.Received(received))
	require.False(t, ws.Received(nilChannel))
	require.False(t, ws.Received(pending))
	require.Empty(t, received, "deduplication consumes exactly one additional value")
	require.Len(t, ws.channels, 3)

	// Explicitly adding a previously received channel rearms it for the next Wait.
	received <- 3
	ws.Add(received)
	require.NoError(t, ws.Wait(context.Background(), 0))
	require.True(t, ws.Received(received))
	require.Len(t, ws.channels, 3)
}

func TestWatchSetReceivedAcrossWaits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ws := NewWatchSet()
		chans := make([]chan int, 8)
		for i := range chans {
			chans[i] = make(chan int)
			ws.Add(chans[i])
			if i%2 == 0 {
				close(chans[i])
			}
		}
		require.NoError(t, ws.Wait(context.Background(), 10*time.Millisecond))
		require.Equal(t, 4, receivedWatchCount(ws))
		require.Len(t, ws.channels, 8)
		require.True(t, ws.ReceivedSet(ws))
		for i, ch := range chans {
			require.Equal(t, i%2 == 0, ws.Received(ch))
			state := ws.channels[(<-chan int)(ch)]
			require.Equal(t, i%2 == 0, state.IsValid())
		}
		appended := make(chan string)
		close(appended)
		ws.Add(appended)
		require.Equal(t, 4, receivedWatchCount(ws), "Add preserves the previous results")
		for i, ch := range chans {
			if i%2 != 0 {
				close(ch)
			}
		}
		require.NoError(t, ws.Wait(context.Background(), 10*time.Millisecond))
		require.Equal(t, 5, receivedWatchCount(ws))
		require.Len(t, ws.channels, 5)
		require.True(t, ws.Received(appended))
		for i, ch := range chans {
			require.Equal(t, i%2 != 0, ws.Received(ch))
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.ErrorIs(t, ws.Wait(ctx, 0), context.Canceled)
		require.Empty(t, ws.channels)
		require.Zero(t, receivedWatchCount(ws))
	})
}

func receivedWatchCount(ws *WatchSet) int {
	count := 0
	for _, state := range ws.channels {
		if state.IsValid() {
			count++
		}
	}
	return count
}

func TestWatchSetManyReceivedChannels(t *testing.T) {
	for _, count := range []int{16, 17, 64} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ws := NewWatchSet()
				var received, pending []<-chan int
				for range count {
					ch := make(chan int)
					close(ch)
					received = append(received, ch)
				}
				for range 8 {
					pending = append(pending, make(chan int))
				}
				ws.Add(received...)
				ws.Add(pending...)
				require.NoError(t, ws.Wait(context.Background(), 50*time.Millisecond))
				require.Equal(t, count, receivedWatchCount(ws))
				for _, ch := range received {
					require.True(t, ws.Received(ch))
				}
				for _, ch := range pending {
					require.False(t, ws.Received(ch))
				}
				merged := NewWatchSet()
				merged.Merge(ws)
				require.Len(t, merged.channels, len(pending), "Merge excludes received channels")
				require.False(t, ws.ReceivedSet(merged))
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				require.ErrorIs(t, ws.Wait(ctx, 0), context.Canceled)
				require.Zero(t, receivedWatchCount(ws))
				require.Len(t, ws.channels, len(pending))
			})
		})
	}
}

func TestWatchSetAddDeduplicates(t *testing.T) {
	ws := NewWatchSet()
	ch := make(chan int)
	ws.Add(ch, ch)
	require.Len(t, ws.channels, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, ws.Wait(ctx, 0), context.Canceled)
	ws.Add(ch, ch)
	require.Len(t, ws.channels, 1)
	require.ErrorIs(t, ws.Wait(ctx, 0), context.Canceled)
}

func TestWatchSetGet(t *testing.T) {
	ws := NewWatchSet()
	ch := make(chan int, 2)
	ch <- 42
	ch <- 43
	_, found := ws.Get[int](ch)
	require.False(t, found)
	ws.Add(ch)
	require.NoError(t, ws.Wait(context.Background(), 0))
	value, found := ws.Get(ch)
	require.True(t, found)
	require.Equal(t, 42, value)
	value, found = ws.Get[int](ch)
	require.True(t, found)
	require.Equal(t, 42, value, "Get does not consume another value")
	require.Len(t, ch, 1)
	for _, missing := range []<-chan int{nil, make(chan int)} {
		_, found = ws.Get[int](missing)
		require.False(t, found)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, ws.Wait(ctx, 0), context.Canceled)
	_, found = ws.Get[int](ch)
	require.False(t, found, "next Wait discards recorded values")
	ws.Add(ch)
	require.NoError(t, ws.Wait(context.Background(), 0))
	value, found = ws.Get[int](ch)
	require.True(t, found)
	require.Equal(t, 43, value)
	ws.Clear()
	_, found = ws.Get[int](ch)
	require.False(t, found)
}

func TestWatchSetGetInterfacesAndClosed(t *testing.T) {
	ws := NewWatchSet()
	closed := make(chan int)
	close(closed)
	ws.Add(closed)
	require.NoError(t, ws.Wait(context.Background(), 0))
	zero, found := ws.Get[int](closed)
	require.True(t, found)
	require.Zero(t, zero)

	dynamic := make(chan any, 1)
	dynamic <- "hello"
	ws.Add(dynamic)
	require.NoError(t, ws.Wait(context.Background(), 0))
	text, found := ws.Get(dynamic)
	require.True(t, found)
	require.Equal(t, "hello", text)

	dynamic <- nil
	ws.Add(dynamic)
	require.NoError(t, ws.Wait(context.Background(), 0))
	empty, found := ws.Get[any](dynamic)
	require.True(t, found, "nil interface values are still received values")
	require.Nil(t, empty)

	dynamic <- (*int)(nil)
	ws.Add(dynamic)
	require.NoError(t, ws.Wait(context.Background(), 0))
	pointer, found := ws.Get(dynamic)
	require.True(t, found)
	require.IsType(t, (*int)(nil), pointer)
	require.Nil(t, pointer)
}

func TestWatchSetGetManyChannels(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ws := NewWatchSet()
		channels := make([]chan int, 19)
		for i := range channels {
			channels[i] = make(chan int, 1)
			channels[i] <- i + 1
			ws.Add(channels[i])
		}
		require.NoError(t, ws.Wait(context.Background(), 50*time.Millisecond))
		require.Equal(t, len(channels), receivedWatchCount(ws))
		for i, ch := range channels {
			value, found := ws.Get(ch)
			require.True(t, found)
			require.Equal(t, i+1, value, "channels must retain their received values")
		}
	})
}

func TestWatchSetChannelDirections(t *testing.T) {
	type namedChannel chan int
	type namedReceiveChannel <-chan int
	ch := make(chan int)
	close(ch)
	ws := NewWatchSet()
	ws.Add(ch)
	require.NoError(t, ws.Wait(context.Background(), 0))
	require.True(t, ws.ReceivedSeq(slices.Values([]chan int{ch})))
	require.True(t, ws.ReceivedSeq(slices.Values([]<-chan int{ch})))
	require.True(t, ws.ReceivedSeq(slices.Values([]namedChannel{namedChannel(ch)})))
	require.True(t, ws.ReceivedSeq(slices.Values([]namedReceiveChannel{namedReceiveChannel(ch)})))
	require.False(t, ws.ReceivedSeq(slices.Values([]chan int{make(chan int)})))
	require.False(t, ws.ReceivedSeq(slices.Values([]chan int(nil))))
	checkWatchSetChannelDirection(t, ws, []chan int{ch})
	checkWatchSetChannelDirection(t, ws, []<-chan int{ch})
	checkWatchSetChannelDirection(t, ws, []namedChannel{namedChannel(ch)})
	checkWatchSetChannelDirection(t, ws, []namedReceiveChannel{namedReceiveChannel(ch)})
}

func checkWatchSetChannelDirection[Ch someChannel[int]](t *testing.T, ws *WatchSet, channels []Ch) {
	t.Helper()
	require.True(t, ws.Received(channels...))
	value, found := ws.Get(channels[0])
	require.True(t, found)
	require.Zero(t, value)
}

func TestWatchSetAppendedBatches(t *testing.T) {
	pool := make([]<-chan int, 128)
	for i := range pool {
		pool[i] = make(chan int)
	}
	pool = append(pool, nil)
	rng := rand.New(rand.NewSource(123))
	ws := NewWatchSet()
	expected := map[<-chan int]bool{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for iteration := range 100 {
		if iteration == 50 {
			ws.Clear()
			clear(expected)
		}
		count := []int{0, 1, 4, 16, 64, 128}[iteration%6]
		var batch []<-chan int
		for range count {
			ch := pool[rng.Intn(len(pool))]
			batch = append(batch, ch)
			expected[ch] = true
		}
		ws.Add(batch...)
		require.Len(t, ws.channels, len(expected))
		require.ErrorIs(t, ws.Wait(ctx, 0), context.Canceled)
		require.Len(t, ws.channels, len(expected))
		for ch, state := range ws.channels {
			require.True(t, expected[ch.(<-chan int)])
			require.False(t, state.IsValid())
		}
	}
}

func TestWatchSetSelectLimit(t *testing.T) {
	channels := make([]<-chan struct{}, watchSetMaxChannels+1)
	for i := range channels {
		channels[i] = make(chan struct{})
	}
	ws := NewWatchSet()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ws.Add(channels[:watchSetMaxChannels]...)
	require.ErrorIs(t, ws.Wait(ctx, 0), context.Canceled, "the context fits at the exact limit")
	ws.Add(channels[watchSetMaxChannels])
	err := ws.Wait(context.Background(), 0)
	require.ErrorIs(t, err, ErrWatchSetTooLarge)
	require.Contains(t, err.Error(), "65536 watch channels, maximum 65535")

	ws.Clear()
	for i := range channels {
		channels[i] = channels[0]
	}
	ws.Add(channels...)
	require.ErrorIs(t, ws.Wait(ctx, 0), context.Canceled, "duplicates do not count against the limit")
	require.Len(t, ws.channels, 1)
}

func TestWatchSetMergeAdditions(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ws := NewWatchSet()
		pending := make(chan int)
		previous := make(chan int, 1)
		previous <- 1
		ws.Add(pending, previous)
		require.NoError(t, ws.Wait(context.Background(), 0))
		first, second := make(chan int, 1), make(chan int, 1)
		first <- 2
		second <- 3
		ws.Add(first, second)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		require.NoError(t, ws.Wait(ctx, 10*time.Millisecond))
		value, found := ws.Get(first)
		require.True(t, found)
		require.Equal(t, 2, value)
		value, found = ws.Get(second)
		require.True(t, found)
		require.Equal(t, 3, value)
		require.False(t, ws.Received(previous, pending))
		require.Len(t, ws.channels, 3)
	})
}

func TestWatchSetMergeExcludesReceived(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ws := NewWatchSet()
		for range 4 {
			ch := make(chan int)
			close(ch)
			ws.Add(ch)
		}
		require.NoError(t, ws.Wait(context.Background(), 20*time.Millisecond))
		require.Equal(t, 4, receivedWatchCount(ws))
		merged := NewWatchSet()
		merged.Merge(ws)
		require.Empty(t, merged.channels)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.ErrorIs(t, ws.Wait(ctx, 0), context.Canceled)
		require.Empty(t, ws.channels)
	})
}

func TestWatchSetRearmClearsResult(t *testing.T) {
	var ws WatchSet
	ch := make(chan int, 1)
	ch <- 42
	ws.Add(ch)
	require.NoError(t, ws.Wait(context.Background(), 0))
	value, found := ws.Get(ch)
	require.True(t, found)
	require.Equal(t, 42, value)
	ws.Add(ch)
	value, found = ws.Get(ch)
	require.False(t, found)
	require.Zero(t, value)
	require.False(t, ws.Received(ch), "re-adding clears the previous result")
	merged := NewWatchSet()
	merged.Merge(&ws)
	require.Len(t, merged.channels, 1, "Merge includes rearmed channels")
	require.False(t, merged.Received(ch), "Merge does not copy receive results")
	ch <- 43
	require.NoError(t, ws.Wait(context.Background(), 0))
	value, found = ws.Get(ch)
	require.True(t, found)
	require.Equal(t, 43, value)
	ws.Merge(merged)
	_, found = ws.Get(ch)
	require.False(t, found, "Merge clears results for channels it adds")
	require.False(t, ws.Received(ch))
}

func TestWatchSetReceiveAndClose(t *testing.T) {
	ws := NewWatchSet()
	ch := make(chan int, 1)
	ch <- 7
	close(ch)
	ws.Add(ch)
	require.NoError(t, ws.Wait(context.Background(), 0))
	value, found := ws.Get(ch)
	require.True(t, found)
	require.Equal(t, 7, value)
	ws.Add(ch)
	require.NoError(t, ws.Wait(context.Background(), 0))
	value, found = ws.Get(ch)
	require.True(t, found)
	require.Zero(t, value)
}

var watchSetSink *WatchSet

func BenchmarkWatchSetNew(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		watchSetSink = NewWatchSet()
	}
}

// Wait uses a cancelled context and open channels to measure preparation and
// selection without changing the set between iterations.
func BenchmarkWatchSetOperations(b *testing.B) {
	for _, n := range []int{0, 1, 4, 16, 128, 1024, 4096} {
		chans := make([]<-chan struct{}, n)
		for i := range chans {
			chans[i] = make(chan struct{})
		}
		b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
			b.Run("AddFresh", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					ws := NewWatchSet()
					ws.Add(chans...)
					watchSetSink = ws
				}
			})
			b.Run("AddReuse", func(b *testing.B) {
				ws := NewWatchSet()
				ws.Add(chans...)
				ws.Clear()
				b.ReportAllocs()
				for b.Loop() {
					ws.Add(chans...)
					ws.Clear()
				}
			})
			b.Run("Wait", func(b *testing.B) {
				ws := NewWatchSet()
				ws.Add(chans...)
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				_ = ws.Wait(ctx, 0)
				b.ReportAllocs()
				for b.Loop() {
					_ = ws.Wait(ctx, 0)
				}
			})
			b.Run("AddWait", func(b *testing.B) {
				ws := NewWatchSet()
				ws.Add(chans...)
				b.ReportAllocs()
				for b.Loop() {
					ws.Add(closedWatchChannel)
					_ = ws.Wait(context.Background(), 0)
				}
			})
		})
	}
}

// BenchmarkWatchSetBatchCycle re-adds existing watch channels along with one
// closed notification per Wait. This exercises deduplication across batches.
func BenchmarkWatchSetBatchCycle(b *testing.B) {
	for _, n := range []int{16, 128, 1024, 4096} {
		chans := make([]<-chan struct{}, n)
		for i := range chans {
			chans[i] = make(chan struct{})
		}
		for _, size := range []int{4, 16, 64} {
			batch := make([]<-chan struct{}, size)
			for i := range batch {
				batch[i] = chans[i%n]
			}
			batch[size-1] = closedWatchChannel
			b.Run(fmt.Sprintf("N=%d/Batch=%d", n, size), func(b *testing.B) {
				ws := NewWatchSet()
				ws.Add(chans...)
				b.ReportAllocs()
				for b.Loop() {
					ws.Add(batch...)
					_ = ws.Wait(context.Background(), 0)
				}
			})
		}
	}
}
