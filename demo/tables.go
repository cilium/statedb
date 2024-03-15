package main

import (
	v1 "k8s.io/api/core/v1"

	"github.com/cilium/statedb"
	"github.com/cilium/statedb/index"
	"github.com/cilium/statedb/reconciler"
)

type Pod struct {
	*v1.Pod

	reconciliationStatus reconciler.Status
}

func (p *Pod) ReconciliationStatus() reconciler.Status {
	return p.reconciliationStatus
}

func (p *Pod) WithReconciliationStatus(s reconciler.Status) *Pod {
	return &Pod{Pod: p.Pod, reconciliationStatus: s}
}

const PodTableName = "pods"

var (
	PodNameIndex = statedb.Index[*Pod, string]{
		Name: "name",
		FromObject: func(pod *Pod) index.KeySet {
			return index.NewKeySet(index.String(pod.Namespace + "/" + pod.Name))
		},
		FromKey: index.String,
		Unique:  true,
	}
	PodPhaseIndex = statedb.Index[*Pod, v1.PodPhase]{
		Name: "phase",
		FromObject: func(pod *Pod) index.KeySet {
			return index.NewKeySet(index.String(string(pod.Status.Phase)))
		},
		FromKey: func(key v1.PodPhase) index.Key {
			return index.String(string(key))
		},
		Unique: false,
	}
)

func NewPodTable(db *statedb.DB) (statedb.RWTable[*Pod], error) {
	return statedb.NewTable[*Pod](
		PodTableName,

		PodNameIndex,
		PodPhaseIndex,
	)
}
