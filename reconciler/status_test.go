// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package reconciler

import (
	"encoding/json"
	"errors"
	"maps"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func errp(s string) *string {
	return &s
}

func TestStatusString(t *testing.T) {
	now := time.Now()

	s := StatusPending()
	assert.Regexp(t, `Pending \([0-9]+\.[0-9]+.+s ago\)`, s.String())
	s.UpdatedAt = now.Add(-time.Hour)
	assert.Regexp(t, `Pending \([0-9]+\.[0-9]+h ago\)`, s.String())

	s = Status{
		Kind:      StatusKindDone,
		UpdatedAt: now,
		Error:     nil,
	}
	assert.Regexp(t, `Done \([0-9]+\.[0-9]+.+s ago\)`, s.String())

	s = StatusError(errors.New("hey I'm an error"))
	assert.Regexp(t, `Error: hey I'm an error \([0-9]+\.[0-9]+.+s ago\)`, s.String())

	s = StatusError(nil)
	assert.Regexp(t, `Error: <nil> \([0-9]+\.[0-9]+.+s ago\)`, s.String())
}

func TestStatusJSON(t *testing.T) {
	testCases := []struct {
		s        Status
		expected string
	}{
		{
			Status{
				Kind:      StatusKindDone,
				UpdatedAt: time.Unix(1, 0).UTC(),
				Error:     nil,
			},
			`{"updated-at":"1970-01-01T00:00:01Z","kind":"Done"}`,
		},
		{
			Status{
				Kind:      StatusKindPending,
				UpdatedAt: time.Unix(2, 0).UTC(),
				Error:     nil,
			},
			`{"updated-at":"1970-01-01T00:00:02Z","kind":"Pending"}`,
		},
		{
			Status{
				Kind:      StatusKindError,
				UpdatedAt: time.Unix(3, 0).UTC(),
				Error:     errp("some-error"),
			},
			`{"updated-at":"1970-01-01T00:00:03Z","error":"some-error","kind":"Error"}`,
		},
		{
			Status{
				Kind:      StatusKindRefreshing,
				UpdatedAt: time.Unix(4, 0).UTC(),
				Error:     nil,
			},
			`{"updated-at":"1970-01-01T00:00:04Z","kind":"Refreshing"}`,
		},
	}

	for _, tc := range testCases {
		b, err := json.Marshal(tc.s)
		assert.NoError(t, err, "Marshal")
		assert.Equal(t, tc.expected, string(b))

		var s Status
		assert.NoError(t, json.Unmarshal(b, &s), "Unmarshal")
		assert.Equal(t, tc.s, s)
	}

}

func sanitizeAgo(s string) string {
	r := regexp.MustCompile(`\(.* ago\)`)
	return string(r.ReplaceAll([]byte(s), []byte("(??? ago)")))
}

func assertStatusSetJSONRoundtrip(t *testing.T, s StatusSet) {
	t.Helper()

	data, err := json.Marshal(s)
	assert.NoError(t, err, "Marshal")
	var s2 StatusSet
	err = json.Unmarshal(data, &s2)
	assert.NoError(t, err, "Unmarshal")
	assert.Equal(t, sanitizeAgo(s.String()), sanitizeAgo(s2.String()))
}

func TestStatusSet(t *testing.T) {
	set := NewStatusSet()
	assert.Equal(t, "Pending", set.String())
	assertStatusSetJSONRoundtrip(t, set)

	s := set.Get("foo")
	assert.Equal(t, s.Kind, StatusKindPending)
	assert.NotZero(t, s.ID)
	assert.False(t, set.IsPendingOrRefreshing())
	assert.True(t, set.IsDone())

	set = set.Set("foo", StatusDone())
	assert.False(t, set.IsPendingOrRefreshing())
	assert.True(t, set.IsDone())

	set = set.Set("bar", StatusError(errors.New("fail")))
	assertStatusSetJSONRoundtrip(t, set)
	assert.False(t, set.IsPendingOrRefreshing())
	assert.False(t, set.IsDone())

	assert.Equal(t, set.Get("foo").Kind, StatusKindDone)
	assert.Equal(t, set.Get("bar").Kind, StatusKindError)
	assert.Regexp(t, "^Errored: bar \\(fail\\), Done: foo \\(.* ago\\)", set.String())

	previous := set
	set = set.Set("foo", StatusRefreshing())
	assert.Equal(t, StatusKindDone, previous.Get("foo").Kind)
	assert.Equal(t, StatusKindRefreshing, set.Get("foo").Kind)
	assert.True(t, set.IsPendingOrRefreshing())
	assert.False(t, set.IsDone())
	assert.Len(t, set.statuses, 2)

	set = set.Pending()
	assert.NotZero(t, set.Get("foo").ID)
	assert.Equal(t, set.Get("foo").Kind, StatusKindPending)
	assert.Equal(t, set.Get("bar").Kind, StatusKindPending)
	assert.Equal(t, set.Get("baz").Kind, StatusKindPending)
	assert.True(t, set.IsPendingOrRefreshing())
	assert.False(t, set.IsDone())
	assert.Regexp(t, "^Pending: bar foo \\(.* ago\\)", set.String())
	assertStatusSetJSONRoundtrip(t, set)
}

func TestStatusSetPendingWithReconcilers(t *testing.T) {
	set := NewStatusSet().
		Set("existing", StatusError(errors.New("previous failure"))).
		Set("done", StatusDone())

	existing := set.Get("existing")
	done := set.Get("done")
	names := []string{"new-b", "existing", "new-a", "new-b"}

	pending := set.Pending(names...)

	// Pending has value semantics and does not modify either the original set or
	// the caller's list of default reconcilers.
	assert.Equal(t, StatusKindError, set.Get("existing").Kind)
	assert.Equal(t, existing.ID, set.Get("existing").ID)
	assert.Equal(t, StatusKindDone, set.Get("done").Kind)
	assert.Equal(t, done.ID, set.Get("done").ID)
	assert.Equal(t, []string{"new-b", "existing", "new-a", "new-b"}, names)

	assert.Len(t, pending.statuses, 4)
	all := maps.Collect(pending.All())
	assert.Len(t, all, 4)
	for _, name := range []string{"done", "existing", "new-a", "new-b"} {
		status, found := all[name]
		assert.True(t, found, name)
		assert.Equal(t, StatusKindPending, status.Kind, name)
		assert.Nil(t, status.Error, name)
		assert.Equal(t, pending.id, status.ID, name)
		assert.Equal(t, pending.updatedAt, status.UpdatedAt, name)
	}
	assert.NotEqual(t, set.id, pending.id)
	assert.Regexp(t, `^Pending: done existing new-a new-b \(.* ago\)$`, pending.String())
	assertStatusSetJSONRoundtrip(t, pending)
}

func TestStatusSetDelete(t *testing.T) {
	set := NewStatusSet().
		Set("first", StatusDone()).
		Set("middle", StatusError(errors.New("failed"))).
		Set("last", StatusRefreshing())
	original := maps.Collect(set.All())

	// Deleting a missing name is a no-op.
	assert.Equal(t, set, set.Delete("missing"))

	deleted := set.Delete("middle")
	assert.Equal(t, original, maps.Collect(set.All()))
	assert.Equal(t, map[string]Status{
		"first": original["first"],
		"last":  original["last"],
	}, maps.Collect(deleted.All()))

	// Pending retains only names that remain in the set.
	pending := deleted.Pending()
	assert.Len(t, pending.statuses, 2)
	assert.NotContains(t, maps.Collect(pending.All()), "middle")
	assert.Equal(t, StatusKindPending, pending.Get("first").Kind)
	assert.Equal(t, StatusKindPending, pending.Get("last").Kind)
	readded := deleted.Pending("middle")
	assert.Contains(t, maps.Collect(readded.All()), "middle")
	assert.Equal(t, StatusKindPending, readded.Get("middle").Kind)

	// Deleting the boundary entries leaves an empty set.
	empty := deleted.Delete("first").Delete("last")
	assert.Empty(t, empty.statuses)
	assert.Empty(t, maps.Collect(empty.All()))
	assert.Equal(t, "Pending", empty.String())
}
