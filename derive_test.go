// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package statedb

import (
	"context"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cilium/hive"
	"github.com/cilium/hive/cell"
	"github.com/cilium/hive/hivetest"
	"github.com/cilium/hive/job"
	"github.com/cilium/statedb/index"
	"github.com/cilium/statedb/part"
)

type derived struct {
	ID      uint64
	Deleted bool
}

var derivedIdIndex = Index[derived, uint64]{
	Name: "id",
	FromObject: func(t derived) index.KeySet {
		return index.NewKeySet(index.Uint64(t.ID))
	},
	FromKey: index.Uint64,
	Unique:  true,
}

type nopHealth struct {
}

// Degraded implements cell.Health.
func (*nopHealth) Degraded(reason string, err error) {
}

// NewScope implements cell.Health.
func (h *nopHealth) NewScope(name string) cell.Health {
	return h
}

// OK implements cell.Health.
func (*nopHealth) OK(status string) {
}

// Stopped implements cell.Health.
func (*nopHealth) Stopped(reason string) {
}

func (*nopHealth) Close() {}

func newNopHealth() (cell.Health, *nopHealth) {
	h := &nopHealth{}
	return h, h
}

var _ cell.Health = &nopHealth{}

func TestDerive(t *testing.T) {
	var (
		db       *DB
		inTable  RWTable[testObject]
		outTable RWTable[derived]
	)

	transform := func(obj testObject, deleted bool) (derived, DeriveResult) {
		t.Logf("transform(%v, %v)", obj, deleted)

		tags := slices.Collect(obj.Tags.All())
		if obj.Tags.Len() > 0 && tags[0] == "skip" {
			return derived{}, DeriveSkip
		}
		if deleted {
			if obj.Tags.Len() > 0 && tags[0] == "delete" {
				return derived{ID: obj.ID}, DeriveDelete
			}
			return derived{ID: obj.ID, Deleted: true}, DeriveUpdate
		}
		return derived{ID: obj.ID, Deleted: false}, DeriveInsert
	}

	h := hive.New(
		Cell, // DB
		job.Cell,
		cell.Provide(newNopHealth),
		cell.Module(
			"test", "Test",

			cell.Provide(func(db_ *DB) (Table[testObject], RWTable[derived], error) {
				db = db_
				inTable = MustNewTable(db, "test", idIndex)
				outTable = MustNewTable(db, "derived", derivedIdIndex)
				return inTable, outTable, nil
			}),

			cell.Invoke(Derive("testObject-to-derived", transform)),
		),
	)
	log := hivetest.Logger(t, hivetest.LogLevel(slog.LevelError))
	require.NoError(t, h.Start(log, context.TODO()), "Start")

	getDerived := func() []derived {
		txn := db.ReadTxn()
		objs := Collect(outTable.All(txn))
		// Log so we can trace the failed eventually calls
		t.Logf("derived: %+v", objs)
		return objs
	}

	// Insert 1, 2 and 3 (skipped) and validate.
	wtxn := db.WriteTxn(inTable)
	_, _, err := inTable.Insert(wtxn, testObject{ID: 1})
	require.NoError(t, err, "Insert failed")
	_, _, err = inTable.Insert(wtxn, testObject{ID: 2})
	require.NoError(t, err, "Insert failed")
	_, _, err = inTable.Insert(wtxn, testObject{ID: 3, Tags: part.NewSet("skip")})
	require.NoError(t, err, "Insert failed")
	wtxn.Commit()

	require.Eventually(t,
		func() bool {
			objs := getDerived()
			return len(objs) == 2 && // 3 is skipped
				objs[0].ID == 1 && objs[1].ID == 2
		},
		time.Second,
		10*time.Millisecond,
		"expected 1 & 2 to be derived",
	)

	// Delete 2 (testing DeriveUpdate)
	wtxn = db.WriteTxn(inTable)
	_, hadOld, err := inTable.Delete(wtxn, testObject{ID: 2})
	require.NoError(t, err, "Delete failed")
	require.True(t, hadOld, "Expected object to be deleted")
	wtxn.Commit()

	require.Eventually(t,
		func() bool {
			objs := getDerived()
			return len(objs) == 2 && // 3 is skipped
				objs[0].ID == 1 && !objs[0].Deleted &&
				objs[1].ID == 2 && objs[1].Deleted
		},
		time.Second,
		10*time.Millisecond,
		"expected 1 & 2, with 2 marked deleted",
	)

	// Delete 1 (testing DeriveDelete)
	wtxn = db.WriteTxn(inTable)
	_, _, err = inTable.Insert(wtxn, testObject{ID: 1, Tags: part.NewSet("delete")})
	require.NoError(t, err, "Insert failed")
	wtxn.Commit()
	wtxn = db.WriteTxn(inTable)
	_, _, err = inTable.Delete(wtxn, testObject{ID: 1})
	require.NoError(t, err, "Delete failed")
	wtxn.Commit()

	require.Eventually(t,
		func() bool {
			objs := getDerived()
			return len(objs) == 1 &&
				objs[0].ID == 2 && objs[0].Deleted
		},
		time.Second,
		10*time.Millisecond,
		"expected 1 to be gone, and 2 mark deleted",
	)

	require.NoError(t, h.Stop(log, context.TODO()), "Stop")
}
