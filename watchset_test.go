// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package statedb

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/cilium/statedb/part"
	"github.com/stretchr/testify/require"
)

func TestWatchSet(t *testing.T) {
	t.Parallel()
	// NOTE: TestMain calls goleak.VerifyTestMain so we know this test doesn't leak goroutines.

	ws := NewWatchSet()

	// Empty watch set, cancelled context.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	chs, err := ws.Wait(ctx, time.Second)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, chs)

	// Few channels, cancelled context.
	ch1 := make(chan struct{})
	ch2 := make(chan struct{})
	ch3 := make(chan struct{})
	ws.Add(ch1, ch2, ch3)
	ctx, cancel = context.WithCancel(context.Background())
	cancel()
	chs, err = ws.Wait(ctx, time.Second)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, chs)

	// Few channels, timed out context. With tiny 'settleTime' we wait for the context to cancel.
	duration := 10 * time.Millisecond
	ctx, cancel = context.WithTimeout(context.Background(), duration)
	t0 := time.Now()
	chs, err = ws.Wait(ctx, time.Nanosecond)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Empty(t, chs)
	require.True(t, time.Since(t0) > duration, "expected to wait until context cancels")
	cancel()

	// One closed channel. Should wait until 'settleTime' expires.
	close(ch1)
	t0 = time.Now()
	chs, err = ws.Wait(context.Background(), duration)
	require.NoError(t, err)
	require.ElementsMatch(t, chs, []<-chan struct{}{ch1})
	require.True(t, time.Since(t0) > duration, "expected to wait until settle time expires")

	// One closed channel, 0 wait time.
	ws = NewWatchSet()
	ws.Add(ch2)
	close(ch2)
	chs, err = ws.Wait(context.Background(), 0)
	require.NoError(t, err)
	require.ElementsMatch(t, chs, []<-chan struct{}{ch2})

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
		closed, err := ws.Wait(context.Background(), 50*time.Millisecond)
		require.NoError(t, err)
		require.ElementsMatch(t, closed, []<-chan struct{}{chans[i], chans[j]}, "i=%d, j=%d", i, j)
		cancel()
	}
}

func TestWatchSetInQueries(t *testing.T) {
	t.Parallel()
	db, table := newTestDBWithMetrics(t, &NopMetrics{}, tagsIndex)

	ws := NewWatchSet()
	txn := db.ReadTxn()
	_, watchAll := table.AllWatch(txn)

	// Should timeout as watches should not have closed yet.
	ws.Add(watchAll)
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	closed, err := ws.Wait(ctx, time.Second)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Empty(t, closed)
	cancel()

	// Insert some objects
	wtxn := db.WriteTxn(table)
	table.Insert(wtxn, testObject{ID: 1})
	table.Insert(wtxn, testObject{ID: 2})
	table.Insert(wtxn, testObject{ID: 3})
	wtxn.Commit()
	txn = db.ReadTxn()

	// The 'watchAll' channel should now have closed and Wait() returns.
	ws.Add(watchAll)
	closed, err = ws.Wait(context.Background(), 100*time.Millisecond)
	require.NoError(t, err)
	require.Len(t, closed, 1)
	require.True(t, closed[0] == watchAll)
	ws.Clear()

	// Try watching specific objects for changes.
	_, _, watch1, _ := table.GetWatch(txn, idIndex.Query(1))
	_, _, watch2, _ := table.GetWatch(txn, idIndex.Query(2))
	_, _, watch3, _ := table.GetWatch(txn, idIndex.Query(3))

	wtxn = db.WriteTxn(table)
	table.Insert(wtxn, testObject{ID: 1, Tags: part.NewSet("foo")})
	wtxn.Commit()

	// Use a new WatchSet and merge it. This allows having "subsets" that we
	// can then use to check whether the closed channel affected the subset.
	ws2 := NewWatchSet()
	ws2.Add(watch3, watch2, watch1)

	// Merge into the larger WatchSet. This still leaves all the channels
	// in ws2.
	ws.Merge(ws2)

	closed, err = ws.Wait(context.Background(), 100*time.Millisecond)
	require.NoError(t, err)
	require.Len(t, closed, 1)
	require.True(t, closed[0] == watch1)
	require.True(t, ws2.Has(closed[0]))
	require.True(t, ws2.HasAny(closed))

	ws2.Clear()
	require.False(t, ws2.Has(closed[0]))
}

func benchmarkWatchSet(b *testing.B, numChans int) {
	ws := NewWatchSet()
	for range numChans - 1 {
		ws.Add(make(chan struct{}))
	}

	for b.Loop() {
		ws.Add(closedWatchChannel)
		ws.Wait(context.TODO(), 0)
	}
}

func BenchmarkWatchSet_4(b *testing.B) {
	benchmarkWatchSet(b, 4)
}

func BenchmarkWatchSet_16(b *testing.B) {
	benchmarkWatchSet(b, 16)
}

func BenchmarkWatchSet_128(b *testing.B) {
	benchmarkWatchSet(b, 128)
}

func BenchmarkWatchSet_1024(b *testing.B) {
	benchmarkWatchSet(b, 1024)
}
