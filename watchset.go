// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package statedb

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"
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

// HasAny returns true if the WatchSet has any of the given channels
func (ws *WatchSet) HasAny(chans []<-chan struct{}) bool {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	for _, ch := range chans {
		if _, found := ws.chans[ch]; found {
			return true
		}
	}
	return false
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

// Wait for channels in the watch set to close until context is cancelled or timeout reached.
// Returns the closed channels and removes them from the set.
func (ws *WatchSet) Wait(ctx context.Context, timeout time.Duration) ([]<-chan struct{}, error) {
	if timeout <= 0 {
		return nil, fmt.Errorf("bad timeout %d, must be >0", timeout)
	}
	innerCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ws.mu.Lock()
	defer ws.mu.Unlock()

	closedChannels := &closedChannelsSlice{}

	// No channels to watch? Just watch the context.
	if len(ws.chans) == 0 {
		<-innerCtx.Done()
		return nil, ctx.Err()
	}

	// Collect the channels into a slice. The slice length is rounded to a full
	// chunk size.
	chans := slices.Collect(maps.Keys(ws.chans))
	chunkSize := 16
	roundedSize := len(chans) + (chunkSize - len(chans)%chunkSize)
	chans = slices.Grow(chans, roundedSize)[:roundedSize]

	if len(ws.chans) <= chunkSize {
		watch16(closedChannels, innerCtx.Done(), chans)
		return closedChannels.chans, ctx.Err()
	}

	var wg sync.WaitGroup
	for chunk := range slices.Chunk(chans, chunkSize) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			watch16(closedChannels, innerCtx.Done(), chunk)
		}()
	}
	wg.Wait()

	for _, ch := range closedChannels.chans {
		delete(ws.chans, ch)
	}

	return closedChannels.chans, ctx.Err()
}

func watch16(closedChannels *closedChannelsSlice, stop <-chan struct{}, chans []<-chan struct{}) {
	for {
		closedIndex := -1
		select {
		case <-stop:
			return
		case <-chans[0]:
			closedIndex = 0
		case <-chans[1]:
			closedIndex = 1
		case <-chans[2]:
			closedIndex = 2
		case <-chans[3]:
			closedIndex = 3
		case <-chans[4]:
			closedIndex = 4
		case <-chans[5]:
			closedIndex = 5
		case <-chans[6]:
			closedIndex = 6
		case <-chans[7]:
			closedIndex = 7
		case <-chans[8]:
			closedIndex = 8
		case <-chans[9]:
			closedIndex = 9
		case <-chans[10]:
			closedIndex = 10
		case <-chans[11]:
			closedIndex = 11
		case <-chans[12]:
			closedIndex = 12
		case <-chans[13]:
			closedIndex = 13
		case <-chans[14]:
			closedIndex = 14
		case <-chans[15]:
			closedIndex = 15
		}
		closedChannels.append(chans[closedIndex])
		chans[closedIndex] = nil
	}
}

type closedChannelsSlice struct {
	mu    sync.Mutex
	chans []<-chan struct{}
}

func (ccs *closedChannelsSlice) append(ch <-chan struct{}) {
	ccs.mu.Lock()
	ccs.chans = append(ccs.chans, ch)
	ccs.mu.Unlock()
}
