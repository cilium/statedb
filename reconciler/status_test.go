// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package reconciler

import (
	"encoding/json"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStatusString(t *testing.T) {
	now := time.Now()

	s := Status{
		Kind:      StatusKindPending,
		UpdatedAt: now,
		Error:     "",
	}
	assert.Regexp(t, `Pending \([0-9]+\.[0-9]+.+s ago\)`, s.String())
	s.UpdatedAt = now.Add(-time.Hour)
	assert.Regexp(t, `Pending \([0-9]+\.[0-9]+h ago\)`, s.String())

	s = Status{
		Kind:      StatusKindDone,
		UpdatedAt: now,
		Error:     "",
	}
	assert.Regexp(t, `Done \([0-9]+\.[0-9]+.+s ago\)`, s.String())

	s = Status{
		Kind:      StatusKindError,
		UpdatedAt: now,
		Error:     "hey I'm an error",
	}
	assert.Regexp(t, `Error: hey I'm an error \([0-9]+\.[0-9]+.+s ago\)`, s.String())
}

func sanitizeAgo(s string) string {
	r := regexp.MustCompile(`\(.* ago\)`)
	return string(r.ReplaceAll([]byte(s), []byte("(??? ago)")))
}

func TestStatusSet(t *testing.T) {
	assertJSONRoundtrip := func(s StatusSet) {
		data, err := json.Marshal(s)
		assert.NoError(t, err, "Marshal")
		var s2 StatusSet
		err = json.Unmarshal(data, &s2)
		assert.NoError(t, err, "Unmarshal")
		assert.Equal(t, sanitizeAgo(s.String()), sanitizeAgo(s2.String()))
	}

	set := NewStatusSet()
	assert.Equal(t, "Pending", set.String())
	assertJSONRoundtrip(set)

	s := set.Get("foo")
	assert.Equal(t, s.Kind, StatusKindPending)
	assert.NotZero(t, s.id)

	set = set.Set("foo", StatusDone())
	set = set.Set("bar", StatusError(errors.New("fail")))
	assertJSONRoundtrip(set)

	assert.Equal(t, set.Get("foo").Kind, StatusKindDone)
	assert.Equal(t, set.Get("bar").Kind, StatusKindError)
	assert.Regexp(t, "^Errored: bar \\(fail\\), Done: foo \\(.* ago\\)", set.String())

	set = set.Pending()
	assert.NotZero(t, set.Get("foo").id)
	assert.Equal(t, set.Get("foo").Kind, StatusKindPending)
	assert.Equal(t, set.Get("bar").Kind, StatusKindPending)
	assert.Equal(t, set.Get("baz").Kind, StatusKindPending)
	assert.Regexp(t, "^Pending: bar foo \\(.* ago\\)", set.String())
	assertJSONRoundtrip(set)
}
