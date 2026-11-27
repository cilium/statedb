// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package statedb

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"reflect"
	"slices"
	"sync"
	"time"
)

// ErrWatchSetTooLarge indicates that Wait cannot select from all distinct
// watch channels plus its context channel within reflect.Select's limit.
var ErrWatchSetTooLarge = errors.New("watch set exceeds reflect.Select case limit")

// reflect.Select permits 65536 cases; one is reserved for the context.
const watchSetMaxChannels = (1 << 16) - 1

// WatchSet for collecting a set of channels and waiting to receive from them.
//
// Example usage:
//
//	ws := statedb.NewWatchSet()
//	timer := time.NewTimer(time.Second)
//	defer timer.Stop()
//	ws.Add(timer.C)
//
//	x, _, xWatch, _ := xTable.GetWatch(db.ReadTxn(), query)
//	reconcileX(x)
//	ws.Add(xWatch)
//
//	for {
//		if err := ws.Wait(ctx, 100*time.Millisecond); err != nil {
//			return err
//		}
//		if ws.Received(xWatch) {
//			x, _, nextWatch, _ := xTable.GetWatch(db.ReadTxn(), query)
//			reconcileX(x)
//			xWatch = nextWatch
//			ws.Add(xWatch)
//		}
//		if firedAt, ok := ws.Get(timer.C); ok {
//			processTimer(firedAt)
//			timer.Reset(time.Second)
//			ws.Add(timer.C)
//		}
//	}
type WatchSet struct {
	// mu protects [channels]. The mutex is needed to support e.g. reconciliation
	// loop that reconciles the results in parallel.
	mu sync.Mutex
	// Keys are channels converted to their unnamed receive-only type, so Add,
	// Received and Get use the same identity for named and bidirectional channels.
	// An invalid value marks a pending channel; a valid value is its last receive.
	channels map[any]reflect.Value
}

func NewWatchSet() *WatchSet { return &WatchSet{} }

// Add adds channels to be included in the next [Wait], clearing any previous
// receive results for those channels.
// A channel is removed from the set after [Wait] has received a value on it
// and [Wait] is called again.
func (ws *WatchSet) Add[T any](chans ...<-chan T) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if len(chans) > 0 && ws.channels == nil {
		ws.channels = make(map[any]reflect.Value, len(chans))
	}
	for _, ch := range chans {
		ws.channels[ch] = reflect.Value{}
	}
}

// Delete removes a channel from the set.
func (ws *WatchSet) Delete[T any, Ch someChannel[T]](ch Ch) (found bool) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	_, found = ws.channels[(<-chan T)(ch)]
	if found {
		delete(ws.channels, (<-chan T)(ch))
	}
	return
}

// Has returns true if the channel is in the set.
// The return value only tells that the channel is in the set, but not
// whether a value has been received.
//
// Has returns true for a channel that had a value received on it after [Wait]
// and the channel is only removed by the subsequent [Wait] unless the channel
// was added back with [Add].
func (ws *WatchSet) Has[T any, Ch someChannel[T]](ch Ch) (found bool) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	_, found = ws.channels[(<-chan T)(ch)]
	return
}

// Clear removes all channels and recorded receives.
func (ws *WatchSet) Clear() {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	clear(ws.channels)
}

type someChannel[T any] interface{ ~chan T | ~<-chan T }

// Get returns the stored value received from ch by the last [Wait]. It returns
// false if no result is available or its stored value has the wrong type.
// Receives from closed channels record the channel element's zero value.
// The value remains available until the next Wait or Clear, or until ch is re-added.
func (ws *WatchSet) Get[T any, Ch someChannel[T]](ch Ch) (value T, found bool) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	received := ws.channels[(<-chan T)(ch)]
	if !received.IsValid() {
		return value, false
	}
	value, found = reflect.TypeAssert[T](received)
	if !found && received.Kind() == reflect.Interface && received.IsNil() && received.Type().AssignableTo(reflect.TypeFor[T]()) {
		// A nil interface has no dynamic type to assert, but is still a received
		// value of a compatible interface type.
		return value, true
	}
	return value, found
}

// Received reports whether the given channel(s) have received a value or closed after [Wait].
func (ws *WatchSet) Received[T any, Ch someChannel[T]](chans ...Ch) bool {
	return ws.ReceivedSeq(slices.Values(chans))
}

// ReceivedSeq reports whether any channels in [chans] have received a
// value or closed after [Wait].
// The supplied iterator must not call methods in [ws] as the iteration
// is done with the [WatchSet] locked.
func (ws *WatchSet) ReceivedSeq[T any, Ch someChannel[T]](chans iter.Seq[Ch]) bool {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	for ch := range chans {
		if ws.channels[(<-chan T)(ch)].IsValid() {
			return true
		}
	}
	return false
}

// ReceivedSet reports whether any channels in [other] have received a
// value or closed after [Wait].
// If the given set is the same set this method returns true.
func (ws *WatchSet) ReceivedSet(other *WatchSet) bool {
	if ws == other {
		return true
	}
	ws.mu.Lock()
	defer ws.mu.Unlock()
	other.mu.Lock()
	defer other.mu.Unlock()
	for ch := range other.channels {
		if ws.channels[ch].IsValid() {
			return true
		}
	}
	return false
}

// Merge adds pending channels from the other set.
func (ws *WatchSet) Merge(other *WatchSet) {
	if ws == other {
		return
	}
	ws.mu.Lock()
	defer ws.mu.Unlock()
	other.mu.Lock()
	defer other.mu.Unlock()
	if ws.channels == nil {
		ws.channels = make(map[any]reflect.Value, len(other.channels))
	}
	for ch, received := range other.channels {
		if !received.IsValid() {
			ws.channels[ch] = reflect.Value{}
		}
	}
}

// Wait receives from a channel in the set or waits for ctx to be cancelled.
//
// If [settleTime] is >=0 [Wait] waits for [settleTime] after the first
// received value to receive additional values from the remaining channels.
// A non-zero [settleTime] is recommended to reduce the overhead of waiting.
//
// The received values are retained for [Get] and [Received] until [Wait] is
// called again or until the entry discarded by [Add], [Clear] or [Delete].
// If a channel was closed the zero value will be returned by [Get].
//
// Channels that received a value or were closed are removed from the set by [Wait]
// unless they were added back with [Add].
//
// If [ctx] is cancelled [Wait] returns the context error.
// If there are more than 65535 channels in the set the [ErrWatchSetTooLarge]
// is returned.
func (ws *WatchSet) Wait(ctx context.Context, settleTime time.Duration) error {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if len(ws.channels) == 0 {
		<-ctx.Done()
		return ctx.Err()
	}
	// Keep small sets on the stack; larger sets allocate only for this Wait.
	var stackCases [32]reflect.SelectCase
	cases := slices.Grow(stackCases[:1], len(ws.channels))
	cases[0] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())}
	for ch, received := range ws.channels {
		if received.IsValid() {
			delete(ws.channels, ch)
			continue
		}
		cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)})
	}
	if count := len(cases) - 1; count > watchSetMaxChannels {
		return fmt.Errorf("%w: %d watch channels, maximum %d", ErrWatchSetTooLarge, count, watchSetMaxChannels)
	}
	if len(cases) == 1 {
		<-ctx.Done()
		return ctx.Err()
	}
	settling := false
	for {
		chosen, received, _ := reflect.Select(cases)
		if chosen == 0 {
			return ctx.Err()
		}
		ch := cases[chosen].Chan.Interface()
		ws.channels[ch] = received
		last := len(cases) - 1
		cases[chosen] = cases[last]
		cases[last] = reflect.SelectCase{}
		cases = cases[:last]
		if settleTime <= 0 {
			return ctx.Err()
		}
		if !settling {
			settleCtx, cancel := context.WithTimeout(ctx, settleTime)
			defer cancel()
			cases[0].Chan = reflect.ValueOf(settleCtx.Done())
			settling = true
		}
	}
}
