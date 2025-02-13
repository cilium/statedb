// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package statedb

import (
	"context"
	"maps"
	"slices"
	"sync"
)

const watchSetChunkSize = 16

type channelSet = map[<-chan struct{}]struct{}

// WatchSet is a set of watch channels that can be waited on.
type WatchSet struct {
	mu    sync.Mutex
	chans channelSet
}

func NewWatchSet() *WatchSet {
	return &WatchSet{
		chans: channelSet{},
	}
}

// Add channel(s) to the watch set
func (ws *WatchSet) Add(chans ...<-chan struct{}) {
	ws.mu.Lock()
	for _, ch := range chans {
		ws.chans[ch] = struct{}{}
	}
	ws.mu.Unlock()
}

// Clear the channels from the WatchSet
func (ws *WatchSet) Clear() {
	ws.mu.Lock()
	clear(ws.chans)
	ws.mu.Unlock()
}

// Has returns true if the WatchSet has the channel
func (ws *WatchSet) Has(ch <-chan struct{}) bool {
	ws.mu.Lock()
	_, found := ws.chans[ch]
	ws.mu.Unlock()
	return found
}

// Merge channels from another WatchSet
func (ws *WatchSet) Merge(other *WatchSet) {
	other.mu.Lock()
	defer other.mu.Unlock()
	ws.mu.Lock()
	defer ws.mu.Unlock()
	for ch := range other.chans {
		ws.chans[ch] = struct{}{}
	}
}

// Wait for any channel in the watch set to close. The
// watch set is cleared when this method returns.
func (ws *WatchSet) Wait(ctx context.Context) (<-chan struct{}, error) {
	ws.mu.Lock()
	defer func() {
		clear(ws.chans)
		ws.mu.Unlock()
	}()

	// No channels to watch? Just watch the context.
	if len(ws.chans) == 0 {
		<-ctx.Done()
		return nil, ctx.Err()
	}

	// Collect the channels into a slice. The slice length is rounded to a full
	// chunk size.
	chans := slices.Collect(maps.Keys(ws.chans))
	chunkSize := 16
	roundedSize := len(chans) + (chunkSize - len(chans)%chunkSize)
	chans = slices.Grow(chans, roundedSize)[:roundedSize]

	if len(ws.chans) <= chunkSize {
		ch := watch16(ctx.Done(), chans)
		return ch, ctx.Err()
	}

	// More than one chunk. Fork goroutines to watch each chunk. The first chunk
	// that completes will cancel the context and stop the other goroutines.
	innerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	closedChan := make(chan (<-chan struct{}), 1)
	defer close(closedChan)
	var wg sync.WaitGroup

	for chunk := range slices.Chunk(chans, chunkSize) {
		wg.Add(1)
		go func() {
			defer cancel()
			defer wg.Done()
			chunk = slices.Clone(chunk)
			if ch := watch16(innerCtx.Done(), chunk); ch != nil {
				select {
				case closedChan <- ch:
				default:
				}
			}
		}()
	}
	wg.Wait()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case ch := <-closedChan:
		return ch, nil
	}
}

func watch16(stop <-chan struct{}, chans []<-chan struct{}) <-chan struct{} {
	select {
	case <-stop:
		return nil
	case <-chans[0]:
		return chans[0]
	case <-chans[1]:
		return chans[1]
	case <-chans[2]:
		return chans[2]
	case <-chans[3]:
		return chans[3]
	case <-chans[4]:
		return chans[4]
	case <-chans[5]:
		return chans[5]
	case <-chans[6]:
		return chans[6]
	case <-chans[7]:
		return chans[7]
	case <-chans[8]:
		return chans[8]
	case <-chans[9]:
		return chans[9]
	case <-chans[10]:
		return chans[10]
	case <-chans[11]:
		return chans[11]
	case <-chans[12]:
		return chans[12]
	case <-chans[13]:
		return chans[13]
	case <-chans[14]:
		return chans[14]
	case <-chans[15]:
		return chans[15]
	}
}
