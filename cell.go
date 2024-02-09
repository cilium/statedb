// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package statedb

import (
	"github.com/cilium/hive/cell"
)

// This module provides an in-memory database built on top of immutable radix trees
// As the database is based on an immutable data structure, the objects inserted into
// the database MUST NOT be mutated, but rather copied first!
//
// For example use see pkg/statedb/example.
var Cell = cell.Module(
	"statedb",
	"In-memory transactional database",

	cell.Provide(
		newHiveDB,
	),
)

type params struct {
	cell.In

	Lifecycle cell.Lifecycle
	Metrics   Metrics `optional:"true"`
}

func newHiveDB(p params) (*DB, error) {
	if p.Metrics == nil {
		p.Metrics = NewExpVarMetrics(false)
	}
	db, err := NewDB(nil, p.Metrics)
	if err != nil {
		return nil, err
	}
	p.Lifecycle.Append(db)
	return db, nil
}
