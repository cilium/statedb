// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package reconciler

import (
	"errors"
	"fmt"
	"slices"
	"time"
)

var closedWatchChannel = func() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

const (
	// maxJoinedErrors limits the number of errors to join and return from
	// failed reconciliation. This avoids constructing a massive error for
	// health status when many operations fail at once.
	maxJoinedErrors = 10
)

func omittedError(n int) error {
	return fmt.Errorf("%d further errors omitted", n)
}

func prettySince(t time.Time) string {
	return prettyDuration(time.Since(t))
}

func prettyDuration(d time.Duration) string {
	ago := float64(d) / float64(time.Microsecond)
	if ago < 1000.0 {
		return fmt.Sprintf("%.1fus", ago)
	}
	ago /= 1000.0
	if ago < 1000.0 {
		return fmt.Sprintf("%.1fms", ago)
	}
	ago /= 1000.0
	if ago < 60.0 {
		return fmt.Sprintf("%.1fs", ago)
	}
	ago /= 60.0
	if ago < 60.0 {
		return fmt.Sprintf("%.1fm", ago)
	}
	ago /= 60.0
	return fmt.Sprintf("%.1fh", ago)
}

func joinErrors(errs []error) error {
	if len(errs) > maxJoinedErrors {
		errs = append(slices.Clone(errs)[:maxJoinedErrors], omittedError(len(errs)-maxJoinedErrors))
	}
	return errors.Join(errs...)
}
