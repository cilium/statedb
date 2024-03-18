package main

import (
	"fmt"
	"net/http"
	"text/tabwriter"
	"time"

	"github.com/cilium/statedb"
	v1 "k8s.io/api/core/v1"
)

func registerStateDBHTTPHandler(mux *http.ServeMux, db *statedb.DB) {
	mux.Handle("/statedb", db)
}

func registerPodHTTPHandler(mux *http.ServeMux, db *statedb.DB, pods statedb.Table[*Pod]) {
	mux.HandleFunc("/pods/running", func(w http.ResponseWriter, req *http.Request) {
		txn := db.ReadTxn()
		iter, _ := pods.Get(txn, PodPhaseIndex.Query(v1.PodRunning))
		t := tabwriter.NewWriter(w, 10, 4, 2, ' ', 0)
		fmt.Fprintf(t, "NAME\tSTARTED\tPOD IP\tSTATUS\n")
		for pod, _, ok := iter.Next(); ok; pod, _, ok = iter.Next() {
			fmt.Fprintf(t, "%s/%s\t%s ago\t%s\t%s\n", pod.Namespace, pod.Name, time.Since(pod.Status.StartTime.Time), pod.Status.PodIP, pod.ReconciliationStatus())
		}
		t.Flush()
	})
}
