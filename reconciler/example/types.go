// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package main

import (
	"github.com/spf13/pflag"

	"github.com/cilium/statedb"
	"github.com/cilium/statedb/index"
	"github.com/cilium/statedb/reconciler"
)

// Config defines the command-line configuration for the memos
// example application.
type Config struct {
	Directory string // the directory in which memos are stored.
}

func (def Config) Flags(flags *pflag.FlagSet) {
	flags.String("directory", "memos", "Memo directory")
}

// Memo is a brief note stored in the memos directory. A memo
// can be created with the /memos API.
type Memo struct {
	Name    string            // filename of the memo. Stored in <directory>/<name>.
	Content string            // contents of the memo.
	Status  reconciler.Status // reconciliation status
}

// TableHeader implements statedb.TableWritable.
func (memo *Memo) TableHeader() []string {
	return []string{"Name", "Content", "Status"}
}

// TableRow implements statedb.TableWritable.
func (memo *Memo) TableRow() []string {
	return []string{memo.Name, memo.Content, memo.Status.String()}
}

var _ statedb.TableWritable = &Memo{}

// GetStatus returns the reconciliation status. Used to provide the
// reconciler access to it.
func (memo *Memo) GetStatus() reconciler.Status {
	return memo.Status
}

// SetStatus sets the reconciliation status.
// Used by the reconciler to update the reconciliation status of the memo.
func (memo *Memo) SetStatus(newStatus reconciler.Status) *Memo {
	memo.Status = newStatus
	return memo
}

// Clone returns a shallow copy of the memo.
func (memo *Memo) Clone() *Memo {
	m := *memo
	return &m
}

// MemoNameIndex allows looking up the memo by its name, e.g.
// memos.First(txn, MemoNameIndex.Query("my-memo"))
var MemoNameIndex = statedb.Index[*Memo, string]{
	Name: "name",
	FromObject: func(memo *Memo) index.KeySet {
		return index.NewKeySet(index.String(memo.Name))
	},
	FromKey: index.String,
	Unique:  true,
}

// MemoStatusIndex indexes memos by their reconciliation status.
// This is mainly used by the reconciler to implement WaitForReconciliation.
var MemoStatusIndex = reconciler.NewStatusIndex((*Memo).GetStatus)

// NewMemoTable creates and registers the memos table.
func NewMemoTable(db *statedb.DB) (statedb.RWTable[*Memo], statedb.Index[*Memo, reconciler.StatusKind], error) {
	tbl, err := statedb.NewTable(
		db,
		"memos",
		MemoNameIndex,
		MemoStatusIndex,
	)
	return tbl, MemoStatusIndex, err
}
