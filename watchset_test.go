package statedb

import (
	"context"
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
	go cancel()
	err := ws.Wait(ctx)
	require.ErrorIs(t, err, context.Canceled)

	// Few channels, cancelled context.
	ch1 := make(chan struct{})
	ch2 := make(chan struct{})
	ch3 := make(chan struct{})
	ws.Add(ch1, ch2, ch3)
	ctx, cancel = context.WithCancel(context.Background())
	go cancel()
	err = ws.Wait(ctx)
	require.ErrorIs(t, err, context.Canceled)

	// Many channels
	for _, numChans := range []int{0, 1, 8, 12, 16, 31, 32, 61, 64, 121} {
		for i := range numChans {
			var chans []chan struct{}
			var rchans []<-chan struct{}
			for range numChans {
				ch := make(chan struct{})
				chans = append(chans, ch)
				rchans = append(rchans, ch)
			}
			ws.Add(rchans...)

			close(chans[i])
			ctx, cancel = context.WithCancel(context.Background())
			err = ws.Wait(ctx)
			require.NoError(t, err)
			cancel()
		}
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	err := ws.Wait(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	cancel()

	// Insert some objects
	wtxn := db.WriteTxn(table)
	table.Insert(wtxn, testObject{ID: 1})
	table.Insert(wtxn, testObject{ID: 2})
	table.Insert(wtxn, testObject{ID: 3})
	txn = wtxn.Commit()

	// The 'watchAll' channel should now have closed and Wait() returns.
	ws.Add(watchAll)
	err = ws.Wait(context.Background())
	require.NoError(t, err)

	// Try watching specific objects for changes.
	_, _, watch1, _ := table.GetWatch(txn, idIndex.Query(1))
	_, _, watch2, _ := table.GetWatch(txn, idIndex.Query(2))
	_, _, watch3, _ := table.GetWatch(txn, idIndex.Query(3))
	ws.Add(watch3, watch2, watch1)
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Millisecond)
	err = ws.Wait(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	cancel()

	wtxn = db.WriteTxn(table)
	table.Insert(wtxn, testObject{ID: 1, Tags: part.NewSet("foo")})
	wtxn.Commit()

	ws.Add(watch3, watch2, watch1)
	err = ws.Wait(context.Background())
	require.NoError(t, err)
}
