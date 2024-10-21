package statedb

import (
	"context"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/cilium/hive"
	"github.com/cilium/hive/cell"
	"github.com/cilium/hive/hivetest"
	"github.com/cilium/hive/script"
	"github.com/cilium/hive/script/scripttest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScript(t *testing.T) {
	log := hivetest.Logger(t)
	h := hive.New(
		Cell, // DB
		cell.Invoke(func(db *DB) {
			t1 := newTestObjectTable(t, "test1", tagsIndex)
			require.NoError(t, db.RegisterTable(t1), "RegisterTable")
			t2 := newTestObjectTable(t, "test2", tagsIndex)
			require.NoError(t, db.RegisterTable(t2), "RegisterTable")
		}),
	)
	t.Cleanup(func() {
		assert.NoError(t, h.Stop(log, context.TODO()))
	})
	cmds, err := h.ScriptCommands(log)
	require.NoError(t, err, "ScriptCommands")
	maps.Insert(cmds, maps.All(script.DefaultCmds()))
	engine := &script.Engine{
		Cmds: cmds,
	}
	scripttest.Test(t,
		context.Background(), func() *script.Engine {
			return engine
		}, []string{}, "testdata/*.txtar")
}

func TestHeaderLine(t *testing.T) {
	type retrieval struct {
		header string
		idxs   []int
	}
	testCases := []struct {
		line  string
		names []string
		pos   []int
		get   []retrieval
	}{
		{
			"Foo   Bar   ",
			[]string{"Foo", "Bar"},
			[]int{0, 6},
			[]retrieval{
				{"Foo", []int{0}},
				{"Bar", []int{1}},
				{"Bar Foo Bar", []int{1, 0, 1}},
			},
		},
		{
			"Foo Bar   Quux",
			[]string{"Foo", "Bar", "Quux"},
			[]int{0, 4, 10},
			[]retrieval{
				{"Foo", []int{0}},
				{"Bar", []int{1}},
				{"Bar  Foo", []int{1, 0}},
				{"Quux", []int{2}},
				{"Quux   Foo", []int{2, 0}},
			},
		},
	}

	for _, tc := range testCases {
		// Parse header line into names and positions
		names, pos := splitHeaderLine(tc.line)
		require.Equal(t, tc.names, names)
		require.Equal(t, tc.pos, pos)

		// Split the header line with the parsed positions.
		header := splitByPositions(tc.line, pos)
		require.Equal(t, tc.names, header)

		// Join the headers with the positions.
		line := joinByPositions(header, pos)
		require.Equal(t, strings.TrimRight(tc.line, " \t"), line)

		// Test retrievals
		for _, r := range tc.get {
			names, pos = splitHeaderLine(r.header)
			idxs, err := getColumnIndexes(names, header)
			require.NoError(t, err)
			require.Equal(t, r.idxs, idxs)

			row := slices.Clone(header)
			cols := takeColumns(row, idxs)
			line := joinByPositions(cols, pos)
			require.Equal(t, line, r.header)
		}
	}
}
