package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/cilium/statedb"
	"github.com/cilium/statedb/reconciler"
)

type podOps struct {
	log *slog.Logger
}

// Delete implements reconciler.Operations.
func (o *podOps) Delete(_ context.Context, _ statedb.ReadTxn, pod *Pod) error {
	o.log.Info("Pod deleted", "name", pod.Namespace+"/"+pod.Name)
	return nil
}

// Prune implements reconciler.Operations.
func (o *podOps) Prune(context.Context, statedb.ReadTxn, statedb.Iterator[*Pod]) error {
	return nil
}

// Update implements reconciler.Operations.
func (o *podOps) Update(ctx context.Context, txn statedb.ReadTxn, pod *Pod, changed *bool) error {
	o.log.Info("Pod updated", "name", pod.Namespace+"/"+pod.Name, "phase", pod.Phase)
	return nil
}

var _ reconciler.Operations[*Pod] = &podOps{}

func podReconcilerConfig(log *slog.Logger) reconciler.Config[*Pod] {
	return reconciler.Config[*Pod]{
		FullReconcilationInterval: time.Minute,
		RetryBackoffMinDuration:   100 * time.Millisecond,
		RetryBackoffMaxDuration:   time.Minute,
		IncrementalRoundSize:      1000,
		GetObjectStatus:           (*Pod).ReconciliationStatus,
		WithObjectStatus:          (*Pod).WithReconciliationStatus,
		Operations:                &podOps{log},
	}
}
