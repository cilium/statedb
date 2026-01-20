// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package tui

import (
	"strings"
	"testing"

	"github.com/liggitt/tabwriter"
)

func TestParseTable(t *testing.T) {
	var b strings.Builder
	tw := tabwriter.NewWriter(&b, 5, 4, 3, ' ', tabwriter.RememberWidths)
	_, _ = tw.Write([]byte("Name\tObject count\tZombie objects\n"))
	_, _ = tw.Write([]byte("foo\t1\t0\n"))
	_, _ = tw.Write([]byte("bar\t2\t0\n"))
	tw.Flush()

	table, err := parseTable(b.String())
	if err != nil {
		t.Fatalf("parseTable: %v", err)
	}
	t.Logf("header: %#v", table.Header)

	if got, want := table.Header, []string{"Name", "Object count", "Zombie objects"}; !slicesEqual(got, want) {
		t.Fatalf("header mismatch: got %v want %v", got, want)
	}
	if len(table.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(table.Rows))
	}
	if table.Rows[0][0] != "foo" || table.Rows[1][0] != "bar" {
		t.Fatalf("unexpected rows: %v", table.Rows)
	}
}

func slicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
