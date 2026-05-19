// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package hive

import (
	"github.com/cilium/hive/cell"
	"github.com/cilium/statedb"
)

// Cell is a [cell.Module] that provides a [*statedb.DB] instance and
// registers the statedb script commands for use in hive script tests.
var Cell = cell.Module(
	"statedb",
	"In-memory transactional database",

	cell.Provide(
		newHiveDB,
		ScriptCommands,
	),
)

type params struct {
	cell.In

	Lifecycle cell.Lifecycle
	Metrics   statedb.Metrics `optional:"true"`
}

func newHiveDB(p params) *statedb.DB {
	db := statedb.New(statedb.WithMetrics(p.Metrics))
	p.Lifecycle.Append(
		cell.Hook{
			OnStart: func(cell.HookContext) error {
				return db.Start()
			},
			OnStop: func(cell.HookContext) error {
				return db.Stop()
			},
		})
	return db
}
