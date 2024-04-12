// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package statedb

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cilium/statedb/index"
)

func httpFixture(t *testing.T) (*DB, Table[testObject], *httptest.Server) {
	db, table, _ := newTestDB(t, tagsIndex)

	ts := httptest.NewServer(db.HTTPHandler())
	t.Cleanup(ts.Close)

	wtxn := db.WriteTxn(table)
	table.Insert(wtxn, testObject{1, []string{"foo"}})
	table.Insert(wtxn, testObject{2, []string{"foo"}})
	table.Insert(wtxn, testObject{3, []string{"foobar"}})
	table.Insert(wtxn, testObject{4, []string{"baz"}})
	wtxn.Commit()

	return db, table, ts
}

func Test_http_dump(t *testing.T) {
	_, _, ts := httpFixture(t)

	resp, err := http.Get(ts.URL + "/dump")
	require.NoError(t, err, "Get(/dump)")
	require.Equal(t, http.StatusOK, resp.StatusCode)

	dump, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.NoError(t, err, "ReadAll")
	fmt.Printf("%s", dump)

	resp, err = http.Get(ts.URL + "/dump/test")
	require.NoError(t, err, "Get(/dump/test)")
	require.Equal(t, http.StatusOK, resp.StatusCode)

	dump, err = io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("%s", dump)

}

func Test_runQuery(t *testing.T) {
	db, table, _ := httpFixture(t)
	txn := db.ReadTxn()

	// idIndex, unique
	indexTxn, err := txn.getTxn().indexReadTxn(table, table.indexPos(idIndex.Name))
	require.NoError(t, err)
	items := []object{}
	onObject := func(obj object) error {
		items = append(items, obj)
		return nil
	}
	runQuery(indexTxn, false, index.Uint64(1), onObject)
	if assert.Len(t, items, 1) {
		assert.EqualValues(t, items[0].data.(testObject).ID, 1)
	}

	// tagsIndex, non-unique
	indexTxn, err = txn.getTxn().indexReadTxn(table, table.indexPos(tagsIndex.Name))
	require.NoError(t, err)
	items = nil
	runQuery(indexTxn, false, index.String("foo"), onObject)

	if assert.Len(t, items, 2) {
		assert.EqualValues(t, items[0].data.(testObject).ID, 1)
		assert.EqualValues(t, items[1].data.(testObject).ID, 2)
	}

	// lower-bound on revision index
	indexTxn, err = txn.getTxn().indexReadTxn(table, RevisionIndexPos)
	require.NoError(t, err)
	items = nil
	runQuery(indexTxn, true, index.Uint64(0), onObject)
	if assert.Len(t, items, 4) {
		// Items are in revision (creation) order
		assert.EqualValues(t, items[0].data.(testObject).ID, 1)
		assert.EqualValues(t, items[1].data.(testObject).ID, 2)
		assert.EqualValues(t, items[2].data.(testObject).ID, 3)
		assert.EqualValues(t, items[3].data.(testObject).ID, 4)
	}
}

func Test_RemoteTable(t *testing.T) {
	ctx := context.TODO()
	_, table, ts := httpFixture(t)

	base, err := url.Parse(ts.URL)
	require.NoError(t, err, "ParseURL")

	remoteTable := NewRemoteTable[testObject](base, table.Name())

	iter, errs := remoteTable.Get(ctx, idIndex.Query(1))
	items := Collect(iter)
	assert.NoError(t, <-errs, "Get(1)")
	if assert.Len(t, items, 1) {
		assert.EqualValues(t, 1, items[0].ID)
	}

	iter, errs = remoteTable.LowerBound(ctx, idIndex.Query(0))
	items = Collect(iter)
	assert.NoError(t, <-errs, "LowerBound(0)")
	if assert.Len(t, items, 4) {
		assert.EqualValues(t, 1, items[0].ID)
		assert.EqualValues(t, 2, items[1].ID)
		assert.EqualValues(t, 3, items[2].ID)
		assert.EqualValues(t, 4, items[3].ID)
	}
}
