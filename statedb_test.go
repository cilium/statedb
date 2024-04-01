package statedb

import (
	"cmp"
	"slices"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
)

func assertEventually(t testing.TB, condition func() bool, waitFor time.Duration, tick time.Duration, msgAndArgs ...interface{}) bool {
	t.Helper()

	ch := make(chan bool, 1)

	timer := time.NewTimer(waitFor)
	defer timer.Stop()

	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	for tick := ticker.C; ; {
		select {
		case <-timer.C:
			// FIXME fix use of msgAndArgs
			// see https://github.com/stretchr/testify/blob/v1.9.0/assert/assertions.go#L342
			assert.Zero(t, "Condition never satisfied", msgAndArgs...)
		case <-tick:
			tick = nil
			go func() { ch <- condition() }()
		case v := <-ch:
			if v {
				return true
			}
			tick = ticker.C
		}
	}
}

func sorted[T cmp.Ordered](s []T) []T {
	slices.Sort(s)
	return s
}
