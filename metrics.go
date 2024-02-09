package statedb

import (
	"expvar"
	"fmt"
	"strings"
	"time"
)

type Metrics interface {
	WriteTxnTableAcquisition(tableName string, acquire time.Duration)
	WriteTxnTotalAcquisition(goPackage string, tables []string, acquire time.Duration)
	WriteTxnDuration(goPackage string, s []string, acquire time.Duration)

	GraveyardLowWatermark(tableName string, lowWatermark Revision)
	GraveyardCleaningDuration(tableName string, duration time.Duration)
	GraveyardObjectCount(tableName string, numDeletedObjects int)
	ObjectCount(tableName string, numObjects int)

	DeleteTrackerCount(tableName string, numTrackers int)
	Revision(tableName string, revision Revision)
}

// ExpVarMetrics is a simple implementation for the metrics.
type ExpVarMetrics struct {
	LockContentionVar            *expvar.Map
	GraveyardCleaningDurationVar *expvar.Map
	GraveyardLowWatermarkVar     *expvar.Map
	GraveyardObjectCountVar      *expvar.Map
	ObjectCountVar               *expvar.Map
	WriteTxnAcquisitionVar       *expvar.Map
	WriteTxnDurationVar          *expvar.Map
	DeleteTrackerCountVar        *expvar.Map
	RevisionVar                  *expvar.Map
}

func (m *ExpVarMetrics) String() (out string) {
	var b strings.Builder
	m.LockContentionVar.Do(func(kv expvar.KeyValue) {
		fmt.Fprintf(&b, "lock_contention[%s]: %s\n", kv.Key, kv.Value.String())
	})
	m.GraveyardCleaningDurationVar.Do(func(kv expvar.KeyValue) {
		fmt.Fprintf(&b, "graveyard_cleaning_duration[%s]: %s\n", kv.Key, kv.Value.String())
	})
	m.GraveyardLowWatermarkVar.Do(func(kv expvar.KeyValue) {
		fmt.Fprintf(&b, "graveyard_low_watermark[%s]: %s\n", kv.Key, kv.Value.String())
	})
	m.GraveyardObjectCountVar.Do(func(kv expvar.KeyValue) {
		fmt.Fprintf(&b, "graveyard_object_count[%s]: %s\n", kv.Key, kv.Value.String())
	})
	m.ObjectCountVar.Do(func(kv expvar.KeyValue) {
		fmt.Fprintf(&b, "object_count[%s]: %s\n", kv.Key, kv.Value.String())
	})
	m.WriteTxnAcquisitionVar.Do(func(kv expvar.KeyValue) {
		fmt.Fprintf(&b, "write_txn_acquisition[%s]: %s\n", kv.Key, kv.Value.String())
	})
	m.WriteTxnDurationVar.Do(func(kv expvar.KeyValue) {
		fmt.Fprintf(&b, "write_txn_duration[%s]: %s\n", kv.Key, kv.Value.String())
	})
	m.DeleteTrackerCountVar.Do(func(kv expvar.KeyValue) {
		fmt.Fprintf(&b, "delete_tracker_count[%s]: %s\n", kv.Key, kv.Value.String())
	})
	m.RevisionVar.Do(func(kv expvar.KeyValue) {
		fmt.Fprintf(&b, "revision[%s]: %s\n", kv.Key, kv.Value.String())
	})

	return b.String()
}

func NewExpVarMetrics(publish bool) *ExpVarMetrics {
	newMap := func(name string) *expvar.Map {
		if publish {
			return expvar.NewMap(name)
		}
		return new(expvar.Map).Init()
	}
	return &ExpVarMetrics{
		LockContentionVar:            newMap("lock_contention"),
		GraveyardCleaningDurationVar: newMap("graveyard_cleaning_duration"),
		GraveyardLowWatermarkVar:     newMap("graveyard_low_watermark"),
		GraveyardObjectCountVar:      newMap("graveyard_object_count"),
		ObjectCountVar:               newMap("object_count"),
		WriteTxnAcquisitionVar:       newMap("write_txn_acquisition"),
		WriteTxnDurationVar:          newMap("write_txn_duration"),
		DeleteTrackerCountVar:        newMap("delete_tracker_count"),
		RevisionVar:                  newMap("revision"),
	}
}

func (m *ExpVarMetrics) DeleteTrackerCount(name string, numTrackers int) {
	var intVar expvar.Int
	intVar.Set(int64(numTrackers))
	m.DeleteTrackerCountVar.Set(name, &intVar)
}

func (m *ExpVarMetrics) Revision(name string, revision uint64) {
	var intVar expvar.Int
	intVar.Set(int64(revision))
	m.RevisionVar.Set(name, &intVar)
}

func (m *ExpVarMetrics) GraveyardCleaningDuration(name string, duration time.Duration) {
	m.GraveyardCleaningDurationVar.AddFloat(name, duration.Seconds())
}

func (m *ExpVarMetrics) GraveyardLowWatermark(name string, lowWatermark Revision) {
	var intVar expvar.Int
	intVar.Set(int64(lowWatermark)) // unfortunately overflows at 2^63
	m.GraveyardLowWatermarkVar.Set(name, &intVar)
}

func (m *ExpVarMetrics) GraveyardObjectCount(name string, numDeletedObjects int) {
	var intVar expvar.Int
	intVar.Set(int64(numDeletedObjects))
	m.GraveyardObjectCountVar.Set(name, &intVar)
}

func (m *ExpVarMetrics) ObjectCount(name string, numObjects int) {
	var intVar expvar.Int
	intVar.Set(int64(numObjects))
	m.ObjectCountVar.Set(name, &intVar)
}

func (m *ExpVarMetrics) WriteTxnDuration(goPackage string, s []string, acquire time.Duration) {
	m.WriteTxnDurationVar.AddFloat(goPackage, acquire.Seconds())
}

func (m *ExpVarMetrics) WriteTxnTotalAcquisition(goPackage string, tables []string, acquire time.Duration) {
	m.WriteTxnAcquisitionVar.AddFloat(goPackage, acquire.Seconds())
}

func (m *ExpVarMetrics) WriteTxnTableAcquisition(name string, acquire time.Duration) {
	m.LockContentionVar.AddFloat(name, acquire.Seconds())
}

var _ Metrics = &ExpVarMetrics{}
