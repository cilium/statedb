// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package hive

import (
	"context"
	"testing"

	"github.com/cilium/statedb"
	"github.com/cilium/stream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestObservable(t *testing.T) {
	t.Parallel()

	db := statedb.New()
	table := newTestTable(t, db, "test")
	require.NoError(t, db.Start())
	t.Cleanup(func() {
		assert.NoError(t, db.Stop())
	})

	ctx, cancel := context.WithCancel(context.Background())
	events := stream.ToChannel(ctx, Observable(db, table))

	txn := db.WriteTxn(table)
	_, hadOld, err := table.Insert(txn, &testObject{ID: uint64(1)})
	require.False(t, hadOld, "Expected no prior object")
	require.NoError(t, err, "Insert failed")
	_, hadOld, err = table.Insert(txn, &testObject{ID: uint64(2)})
	require.False(t, hadOld, "Expected no prior object")
	require.NoError(t, err, "Insert failed")
	txn.Commit()

	event := <-events
	require.False(t, event.Deleted, "expected insert")
	require.Equal(t, uint64(1), event.Object.ID)
	event = <-events
	require.False(t, event.Deleted, "expected insert")
	require.Equal(t, uint64(2), event.Object.ID)

	txn = db.WriteTxn(table)
	_, hadOld, err = table.Delete(txn, &testObject{ID: uint64(1)})
	require.True(t, hadOld, "Expected that object was deleted")
	require.NoError(t, err, "Delete failed")
	_, hadOld, err = table.Delete(txn, &testObject{ID: uint64(2)})
	require.True(t, hadOld, "Expected that object was deleted")
	require.NoError(t, err, "Delete failed")
	txn.Commit()

	event = <-events
	require.True(t, event.Deleted, "expected delete")
	require.Equal(t, uint64(1), event.Object.ID)
	event = <-events
	require.True(t, event.Deleted, "expected delete")
	require.Equal(t, uint64(2), event.Object.ID)

	cancel()
	ev, ok := <-events
	require.False(t, ok, "expected channel to close, got event: %+v", ev)
}
