// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package reconciler

import (
	"context"
	"sync"

	"github.com/cilium/statedb"
)

// progressTracker tracks the highest revision observed as reconciled and
// allows callers to wait until a target revision is reached.
type progressTracker struct {
	mu       sync.Mutex
	revision statedb.Revision
	watch    chan struct{}
}

func newProgressTracker() *progressTracker {
	return &progressTracker{
		watch: make(chan struct{}),
	}
}

func (p *progressTracker) update(rev statedb.Revision) {
	p.mu.Lock()
	if rev > p.revision {
		p.revision = rev
		close(p.watch)
		p.watch = make(chan struct{})
	}
	p.mu.Unlock()
}

func (p *progressTracker) wait(ctx context.Context, rev statedb.Revision) (statedb.Revision, error) {
	for {
		p.mu.Lock()
		current := p.revision
		watch := p.watch
		p.mu.Unlock()

		if current >= rev {
			return current, nil
		}
		select {
		case <-ctx.Done():
			return current, ctx.Err()
		case <-watch:
		}
	}
}
