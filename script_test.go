package statedb

import (
	"context"
	"maps"
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
