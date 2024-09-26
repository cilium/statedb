// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package testutils_test

import (
	"flag"
	"strings"
	"testing"

	"github.com/cilium/statedb"
	"github.com/cilium/statedb/index"
	"github.com/cilium/statedb/testutils"
	"github.com/rogpeppe/go-internal/testscript"
)

type object struct {
	Name string
	Tags []string
}

func (o object) TableHeader() []string {
	return []string{"Name", "Tags"}
}

func (o object) TableRow() []string {
	return []string{
		o.Name,
		strings.Join(o.Tags, ", "),
	}
}

var nameIdx = statedb.Index[object, string]{
	Name: "name",
	FromObject: func(obj object) index.KeySet {
		return index.NewKeySet(index.String(obj.Name))
	},
	FromKey: index.String,
	Unique:  true,
}

var update = flag.Bool("update", false, "update the txtar files")

func TestScriptCommands(t *testing.T) {
	testscript.Run(t, testscript.Params{
		Dir: "testdata",
		Setup: func(e *testscript.Env) error {
			db := statedb.New()
			tbl, err := statedb.NewTable("names", nameIdx)
			if err != nil {
				t.Fatalf("NewTable: %s", err)
			}
			if err := db.RegisterTable(tbl); err != nil {
				t.Fatalf("RegisterTable: %s", err)
			}
			testutils.Setup(e, db)
			return nil
		},
		Cmds:          testutils.Commands,
		UpdateScripts: *update,
	})
}
