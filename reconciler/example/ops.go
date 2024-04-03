// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path"

	"github.com/cilium/hive/cell"
	"github.com/cilium/statedb"
	"github.com/cilium/statedb/reconciler"
)

// MemoOps writes [Memo]s to disk.
// Implements the Reconciler.Operations[*Memo] API.
type MemoOps struct {
	log       *slog.Logger
	directory string
}

// NewMemoOps creates the memo operations.
func NewMemoOps(lc cell.Lifecycle, log *slog.Logger, cfg Config) reconciler.Operations[*Memo] {
	ops := &MemoOps{directory: cfg.Directory, log: log}

	// Register the Start and Stop methods to be called when the application
	// starts and stops respectively. The start hook will create the
	// memo directory.
	lc.Append(ops)
	return ops
}

// Delete a memo.
func (ops *MemoOps) Delete(ctx context.Context, txn statedb.ReadTxn, memo *Memo) error {
	filename := path.Join(ops.directory, memo.Name)
	err := os.Remove(filename)
	ops.log.Info("Delete", "filename", filename, "error", err)
	return err
}

// Prune unexpected memos.
func (ops *MemoOps) Prune(ctx context.Context, txn statedb.ReadTxn, iter statedb.Iterator[*Memo]) error {
	expected := map[string]struct{}{}
	for memo, _, ok := iter.Next(); ok; memo, _, ok = iter.Next() {
		expected[memo.Name] = struct{}{}
	}

	// Find unexpected files
	unexpected := map[string]struct{}{}
	if entries, err := os.ReadDir(ops.directory); err != nil {
		return err
	} else {
		for _, entry := range entries {
			if _, ok := expected[entry.Name()]; !ok {
				unexpected[entry.Name()] = struct{}{}
			}
		}
	}

	// ... and remove them.
	var errs []error
	for name := range unexpected {
		filename := path.Join(ops.directory, name)
		err := os.Remove(filename)
		ops.log.Info("Prune", "filename", filename, "error", err)
		if err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Update a memo.
func (ops *MemoOps) Update(ctx context.Context, txn statedb.ReadTxn, memo *Memo, changed *bool) error {
	filename := path.Join(ops.directory, memo.Name)

	// Read the old file to figure out if it had changed.
	// The 'changed' boolean is used by full reconciliation to keep track of when the target
	// has gone out-of-sync (e.g. there has been some outside influence to it).
	old, err := os.ReadFile(filename)
	if err == nil && bytes.Equal(old, []byte(memo.Content)) {

		// Nothing to do.
		return nil
	}
	if changed != nil {
		*changed = true
	}
	err = os.WriteFile(filename, []byte(memo.Content), 0644)
	ops.log.Info("Update", "filename", filename, "error", err)
	return err
}

var _ reconciler.Operations[*Memo] = &MemoOps{}

func (ops *MemoOps) Start(cell.HookContext) error {
	return os.MkdirAll(ops.directory, 0755)
}

func (*MemoOps) Stop(cell.HookContext) error {
	return nil
}

var _ cell.HookInterface = &MemoOps{}
