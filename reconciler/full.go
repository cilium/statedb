// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package reconciler

import (
	"context"
	"fmt"
	"time"

	"github.com/cilium/statedb"
)

// full performs full reconciliation of all objects. First the Prune() operations is performed to clean up and then
// Update() is called for each object. Full reconciliation is used to recover from unexpected outside modifications.
func (r *reconciler[Obj]) full(ctx context.Context, txn statedb.ReadTxn, lastRev statedb.Revision) (statedb.Revision, error) {
	var errs []error
	outOfSync := false
	ops := r.Config.Operations

	// First perform pruning to make room in the target.
	iter, _ := r.Table.All(txn)
	start := time.Now()
	if err := ops.Prune(ctx, txn, iter); err != nil {
		outOfSync = true
		errs = append(errs, fmt.Errorf("pruning failed: %w", err))
	}
	r.metrics.FullReconciliationDuration(r.ModuleID, OpPrune, time.Since(start))

	// Call Update() for each desired object to validate that it is up-to-date.
	updateResults := make(map[Obj]opResult)
	iter, _ = r.Table.All(txn) // Grab a new iterator as Prune() may have consumed it.
	for obj, rev, ok := iter.Next(); ok; obj, rev, ok = iter.Next() {
		start := time.Now()
		var changed bool
		err := ops.Update(ctx, txn, obj, &changed)
		r.metrics.FullReconciliationDuration(r.ModuleID, OpUpdate, time.Since(start))

		outOfSync = outOfSync || changed
		if err == nil {
			updateResults[obj] = opResult{rev: rev, status: StatusDone()}
			r.retries.Clear(obj)
		} else {
			updateResults[obj] = opResult{rev: rev, status: StatusError(false, err)}
			errs = append(errs, err)
		}
	}

	// Increment the out-of-sync counter if full reconciliation catched any out-of-sync
	// objects.
	if outOfSync {
		r.metrics.FullReconciliationOutOfSync(r.ModuleID)
	}

	// Commit the new desired object status. This is performed separately in order
	// to not lock the table when performing long-running target operations.
	// If the desired object has been updated in the meanwhile the status update is dropped.
	if len(updateResults) > 0 {
		wtxn := r.DB.WriteTxn(r.Table)
		for obj, result := range updateResults {
			obj = r.Config.WithObjectStatus(obj, result.status)
			_, _, err := r.Table.CompareAndSwap(wtxn, result.rev, obj)
			if err == nil && result.status.Kind != StatusKindDone {
				// Object had not changed in the meantime, queue the retry.
				r.retries.Add(obj)
			}
		}
		wtxn.Commit()
	}

	r.metrics.FullReconciliationErrors(r.ModuleID, errs)
	if len(errs) > 0 {
		return r.Table.Revision(txn), fmt.Errorf("full: %w", joinErrors(errs))
	}

	// Sync succeeded up to latest revision. Continue incremental reconciliation from
	// this revision.
	return r.Table.Revision(txn), nil
}
