// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package reconciler

import (
	"reflect"
	"time"

	"golang.org/x/time/rate"
)

// mergeWithDefaults fills in any zero fields from the default configuration.
func mergeWithDefaults[Obj any](cfg Config[Obj]) Config[Obj] {
	def := defaultConfig[Obj]()
	cfgV := reflect.ValueOf(&cfg).Elem()
	defV := reflect.ValueOf(def)
	for i := 0; i < cfgV.NumField(); i++ {
		f := cfgV.Field(i)
		if f.IsZero() {
			f.Set(defV.Field(i))
		}
	}
	return cfg
}

func defaultConfig[Obj any]() Config[Obj] {
	return Config[Obj]{
		// Refreshing is disabled by default.
		RefreshInterval:    0,
		RefreshRateLimiter: defaultRefreshRateLimiter(),

		// Pruning is disabled by default.
		PruneInterval: 0,

		// Retry failed operations with exponential backoff from 100ms to 1min.
		RetryBackoffMinDuration: time.Millisecond * 100,
		RetryBackoffMaxDuration: time.Minute,

		// Reconcile 100 rounds per second * 1000 yielding maximum rate of
		// 100k objects per second.
		IncrementalRoundSize: 1000,
		RateLimiter:          defaultRoundRateLimiter(),

		// Table, GetObjectStatus, SetObjectStatus, CloneObject, Operations,
		// BatchOperations must be specified by the user.
		// Metrics either specified by user or the default metrics are used.
	}
}

func defaultRoundRateLimiter() *rate.Limiter {
	// By default limit the rate of reconciliation rounds to 100 times per second.
	// This enables the reconciler to operate on batches of objects at a time, which
	// enables efficient use of the batch operations and amortizes the cost of WriteTxn.
	return rate.NewLimiter(100.0, 1)
}

func defaultRefreshRateLimiter() *rate.Limiter {
	// By default limit the object refresh rate to 100 objects per second. This avoids a
	// stampade of refreshes that could delay normal updates.
	return rate.NewLimiter(100.0, 1)
}
