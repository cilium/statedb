// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package main

import (
	"expvar"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/time/rate"

	"github.com/cilium/hive"
	"github.com/cilium/hive/cell"
	"github.com/cilium/hive/job"
	"github.com/cilium/statedb"
	"github.com/cilium/statedb/reconciler"
)

// This is a simple example of the statedb reconciler. It implements an
// HTTP API for creating and deleting "memos" that are stored on the
// disk.
//
// To run the application:
//
//   $ go run .
//   (ctrl-c to stop)
//
// To create a memo:
//
//   $ curl -d 'hello world' http://localhost:8080/memos/greeting
//   $ cat memos/greeting
//
// To delete a memo:
//
//   $ curl -XDELETE http://localhost:8080/memos/greeting
//
// The application builds on top of the reconciler which retries any failed
// operations and also does periodic "full reconciliation" to prune unknown
// memos and check that the stored memos are up-to-date. To test the resilence
// you can try out the following:
//
//   # Create 'memos/greeting'
//   $ curl -d 'hello world' http://localhost:8080/memos/greeting
//
//   # Make the file read-only and try changing it:
//   $ chmod a-w memos/greeting
//   $ curl -d 'hei maailma' http://localhost:8080/memos/greeting
//   # (You should now see the update operation hitting permission denied)
//
//   # The reconciliation state can be observed in the Table[*Memo]:
//   $ curl -q http://localhost:8080/statedb | jq .
//
//   # Let's give write permissions back:
//   $ chmod u+w memos/greeting
//   # (The update operation should now succeed)
//   $ cat memos/greeting
//   $ curl -s http://localhost:8080/statedb | jq .
//
//   # The full reconciliation runs every 10 seconds. We can see it in
//   # action by modifying the contents of our greeting or by creating
//   # a file directly:
//   $ echo bogus > memos/bogus
//   $ echo mangled > memos/greeting
//   # (wait up to 10 seconds)
//   $ cat memos/bogus
//   $ cat memos/greeting
//

func main() {
	cmd := cobra.Command{
		Use: "example",
		Run: func(_ *cobra.Command, args []string) {
			if err := Hive.Run(slog.Default()); err != nil {
				fmt.Fprintf(os.Stderr, "Run: %s\n", err)
			}
		},
	}

	// Register command-line flags. Currently only
	// has --directory for specifying where to store
	// the memos.
	Hive.RegisterFlags(cmd.Flags())

	// Add the "hive" command for inspecting the object graph:
	//
	//  $ go run . hive
	//
	cmd.AddCommand(Hive.Command())

	cmd.Execute()
}

var Hive = hive.NewWithOptions(
	hive.Options{
		// Create a named DB handle for each module.
		ModuleDecorators: []cell.ModuleDecorator{
			func(db *statedb.DB, id cell.ModuleID) *statedb.DB {
				return db.NewHandle(string(id))
			},
		},
	},

	statedb.Cell,
	job.Cell,

	cell.SimpleHealthCell,

	cell.Provide(reconciler.NewExpVarMetrics),

	cell.Module(
		"example",
		"Reconciler example",

		cell.Config(Config{}),

		cell.Provide(
			// Create and register the RWTable[*Memo]
			NewMemoTable,

			// Provide the Operations[*Memo] for reconciling Memos.
			NewMemoOps,
		),

		// Create and register the reconciler for memos.
		// The reconciler watches Table[*Memo] for changes and
		// updates the memo files on disk accordingly.
		cell.Invoke(registerMemoReconciler),

		cell.Invoke(registerHTTPServer),
	),
)

func registerMemoReconciler(
	params reconciler.Params,
	ops reconciler.Operations[*Memo],
	tbl statedb.RWTable[*Memo],
	m *reconciler.ExpVarMetrics) error {

	// Create a new reconciler and register it to the lifecycle.
	// We ignore the returned Reconciler[*Memo] as we don't use it.
	_, err := reconciler.Register(
		params,
		tbl,
		(*Memo).Clone,
		(*Memo).SetStatus,
		(*Memo).GetStatus,
		ops,
		nil, // no batch operations support

		reconciler.WithMetrics(m),
		// Prune unexpected memos from disk once a minute.
		reconciler.WithPruning(time.Minute),
		// Refresh the memos once a minute.
		reconciler.WithRefreshing(time.Minute, rate.NewLimiter(100.0, 1)),
	)
	return err
}

func registerHTTPServer(
	lc cell.Lifecycle,
	log *slog.Logger,
	db *statedb.DB,
	memos statedb.RWTable[*Memo]) {

	mux := http.NewServeMux()

	// To dump the metrics:
	// curl -s http://localhost:8080/expvar
	mux.Handle("/expvar", expvar.Handler())

	// For dumping the database:
	// curl -s http://localhost:8080/statedb | jq .
	mux.HandleFunc("/statedb", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := db.ReadTxn().WriteJSON(w); err != nil {
			panic(err)
		}
	})

	// For creating and deleting memos:
	// curl -d 'foo' http://localhost:8080/memos/bar
	// curl -XDELETE http://localhost:8080/memos/bar
	mux.HandleFunc("/memos/", func(w http.ResponseWriter, r *http.Request) {
		name, ok := strings.CutPrefix(r.URL.Path, "/memos/")
		if !ok {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		txn := db.WriteTxn(memos)
		defer txn.Commit()

		switch r.Method {
		case "POST":
			content, err := io.ReadAll(r.Body)
			if err != nil {
				return
			}
			memos.Insert(
				txn,
				&Memo{
					Name:    name,
					Content: string(content),
					Status:  reconciler.StatusPending(),
				})
			log.Info("Inserted memo", "name", name)
			w.WriteHeader(http.StatusOK)

		case "DELETE":
			memo, _, ok := memos.Get(txn, MemoNameIndex.Query(name))
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			memos.Delete(txn, memo)
			log.Info("Deleted memo", "name", name)
			w.WriteHeader(http.StatusOK)
		}
	})

	server := http.Server{
		Addr:    "127.0.0.1:8080",
		Handler: mux,
	}

	lc.Append(cell.Hook{
		OnStart: func(cell.HookContext) error {
			log.Info("Serving API", "address", server.Addr)
			go server.ListenAndServe()
			return nil
		},
		OnStop: func(ctx cell.HookContext) error {
			return server.Shutdown(ctx)
		},
	})

}
