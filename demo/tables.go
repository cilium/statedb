package main

import (
	"time"

	v1 "k8s.io/api/core/v1"

	"github.com/cilium/statedb"
	"github.com/cilium/statedb/index"
	"github.com/cilium/statedb/reconciler"
)

type Pod struct {
	Name, Namespace string
	Phase           v1.PodPhase
	StartTime       time.Time

	reconciliationStatus reconciler.Status
}

func fromV1Pod(p *v1.Pod) *Pod {
	var startTime time.Time
	if p.Status.StartTime != nil {
		startTime = p.Status.StartTime.Time
	}
	return &Pod{
		Name:                 p.Name,
		Namespace:            p.Namespace,
		Phase:                p.Status.Phase,
		StartTime:            startTime,
		reconciliationStatus: reconciler.StatusPending(),
	}
}

func (p *Pod) ReconciliationStatus() reconciler.Status {
	return p.reconciliationStatus
}

func (p *Pod) WithReconciliationStatus(s reconciler.Status) *Pod {
	p2 := *p
	p2.reconciliationStatus = s
	return &p2
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
			return index.NewKeySet(index.String(string(pod.Phase)))
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
