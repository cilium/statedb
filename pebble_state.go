// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package statedb

import (
	"slices"
	"sync"

	"github.com/cockroachdb/pebble"
)

type pebbleState struct {
	path string
	opts *pebble.Options

	mu        sync.Mutex
	db        *pebble.DB
	snapshots map[*pebble.Snapshot]struct{}
}

type pebbleBatchHandle struct {
	batch *pebble.Batch
}

func newPebbleState(path string, opts *pebble.Options) *pebbleState {
	if path == "" {
		return &pebbleState{}
	}
	var cloned *pebble.Options
	if opts != nil {
		clone := *opts
		cloned = &clone
	}
	return &pebbleState{
		path:      path,
		opts:      cloned,
		snapshots: map[*pebble.Snapshot]struct{}{},
	}
}

func (p *pebbleState) ensureOpen() error {
	if p == nil {
		return ErrPebbleNotConfigured
	}
	if p.path == "" {
		return ErrPebbleNotConfigured
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.db != nil {
		return nil
	}
	db, err := pebble.Open(p.path, p.opts)
	if err != nil {
		return err
	}
	p.db = db
	return nil
}

func (p *pebbleState) get() *pebble.DB {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.db
}

func (p *pebbleState) close() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for snap := range p.snapshots {
		_ = snap.Close()
		delete(p.snapshots, snap)
	}
	if p.db == nil {
		return nil
	}
	err := p.db.Close()
	p.db = nil
	return err
}

func (p *pebbleState) addSnapshot(snapshot *pebble.Snapshot) {
	if p == nil || snapshot == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.snapshots == nil {
		p.snapshots = map[*pebble.Snapshot]struct{}{}
	}
	p.snapshots[snapshot] = struct{}{}
}

func (p *pebbleState) removeSnapshot(snapshot *pebble.Snapshot) {
	if p == nil || snapshot == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.snapshots, snapshot)
}

func namespacePrefix(tableName, indexName string) []byte {
	return append(append(append([]byte{}, tableName...), 0x00), append([]byte(indexName), 0x00)...)
}

func namespaceUpperBound(prefix []byte) []byte {
	upper := slices.Clone(prefix)
	upper[len(upper)-1]++
	return upper
}
