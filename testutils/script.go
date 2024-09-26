// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package testutils

import (
	"bytes"
	"cmp"
	"encoding/json"
	"flag"
	"fmt"
	"iter"
	"maps"
	"os"
	"regexp"
	"slices"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/cilium/statedb"
	"github.com/rogpeppe/go-internal/testscript"
	"gopkg.in/yaml.v3"
)

type Cmd = func(ts *testscript.TestScript, neg bool, args []string)

const tsDBKey = "statedb"

func Setup(e *testscript.Env, db *statedb.DB) {
	e.Values[tsDBKey] = db
}

func getDB(ts *testscript.TestScript) *statedb.DB {
	v := ts.Value(tsDBKey)
	if v == nil {
		ts.Fatalf("%q not set, call testutils.Setup()", tsDBKey)
	}
	return v.(*statedb.DB)
}

func getTable(ts *testscript.TestScript, tableName string) (*statedb.DB, statedb.ReadTxn, statedb.AnyTable) {
	db := getDB(ts)
	txn := db.ReadTxn()
	meta := db.GetTable(txn, tableName)
	if meta == nil {
		ts.Fatalf("table %q not found", tableName)
	}
	tbl := statedb.AnyTable{Meta: meta}
	return db, txn, tbl
}

var (
	Commands = map[string]Cmd{
		"db": DBCmd,
	}
	SubCommands = map[string]Cmd{
		"tables":     TablesCmd,
		"show":       ShowTableCmd,
		"write":      WriteTableCmd,
		"cmp":        CompareTableCmd,
		"insert":     InsertCmd,
		"delete":     DeleteCmd,
		"prefix":     PrefixCmd,
		"lowerbound": LowerBoundCmd,
	}
)

func DBCmd(ts *testscript.TestScript, neg bool, args []string) {
	if len(args) < 1 {
		ts.Fatalf("usage: db <command> args...\n<command> is one of %v", maps.Keys(SubCommands))
	}
	if cmd, ok := SubCommands[args[0]]; ok {
		cmd(ts, neg, args[1:])
	} else {
		ts.Fatalf("unknown db command %q, should be one of %v", args[0], maps.Keys(SubCommands))
	}
}

func TablesCmd(ts *testscript.TestScript, neg bool, args []string) {
	db := getDB(ts)
	txn := db.ReadTxn()
	tbls := db.GetTables(txn)
	var buf bytes.Buffer
	w := tabwriter.NewWriter(&buf, 5, 4, 3, ' ', 0)
	fmt.Fprintf(w, "Name\tObject count\tIndexes\n")
	for _, tbl := range tbls {
		idxs := strings.Join(tbl.Indexes(), ", ")
		fmt.Fprintf(w, "%s\t%d\t%s\n", tbl.Name(), tbl.NumObjects(txn), idxs)
	}
	w.Flush()
	ts.Logf("%s", buf.String())
}

func ShowTableCmd(ts *testscript.TestScript, neg bool, args []string) {
	if len(args) != 1 {
		ts.Fatalf("usage: show_table <table>")
	}
	ts.Logf("%s", showTable(ts, args[0]).String())
}

func WriteTableCmd(ts *testscript.TestScript, neg bool, args []string) {
	if len(args) < 1 || len(args) > 5 {
		ts.Fatalf("usage: write_table <table> (-to=<file>) (-columns=<col1,col2,...>) (-format={table*,yaml})")
	}
	var flags flag.FlagSet
	file := flags.String("to", "", "File to write to instead of stdout")
	columns := flags.String("columns", "", "Comma-separated list of columns to write")
	format := flags.String("format", "table", "Format to write in")

	// Sort the args to allow the table name at any position.
	slices.SortFunc(args, func(a, b string) int {
		switch {
		case a[0] == '-':
			return 1
		case b[0] == '-':
			return -1
		default:
			return cmp.Compare(a, b)
		}
	})

	if err := flags.Parse(args[1:]); err != nil {
		ts.Fatalf("bad args: %s", err)
	}
	tableName := args[0]

	switch *format {
	case "yaml", "json":
		if len(*columns) > 0 {
			ts.Fatalf("-columns not supported with -format=yaml/json")
		}

		_, txn, tbl := getTable(ts, tableName)
		var buf bytes.Buffer
		count := tbl.Meta.NumObjects(txn)
		for obj := range tbl.All(txn) {
			if *format == "yaml" {
				out, err := yaml.Marshal(obj)
				if err != nil {
					ts.Fatalf("yaml.Marshal: %s", err)
				}
				buf.Write(out)
				if count > 1 {
					buf.WriteString("---\n")
				}
			} else {
				out, err := json.Marshal(obj)
				if err != nil {
					ts.Fatalf("json.Marshal: %s", err)
				}
				buf.Write(out)
				buf.WriteByte('\n')
			}
			count--
		}
		if *file == "" {
			ts.Logf("%s", buf.String())
		} else if err := os.WriteFile(ts.MkAbs(*file), buf.Bytes(), 0644); err != nil {
			ts.Fatalf("WriteFile(%s): %s", *file, err)
		}
	default:
		var cols []string
		if len(*columns) > 0 {
			cols = strings.Split(*columns, ",")
		}
		buf := showTable(ts, tableName, cols...)
		if *file == "" {
			ts.Logf("%s", buf.String())
		} else if err := os.WriteFile(ts.MkAbs(*file), buf.Bytes(), 0644); err != nil {
			ts.Fatalf("WriteFile(%s): %s", *file, err)
		}
	}
}

func CompareTableCmd(ts *testscript.TestScript, neg bool, args []string) {
	var flags flag.FlagSet
	timeout := flags.Duration("timeout", time.Second, "Maximum amount of time to wait for the table contents to match")
	grep := flags.String("grep", "", "Grep the result rows and only compare matching ones")

	err := flags.Parse(args)
	args = args[len(args)-flags.NArg():]
	if err != nil || len(args) != 2 {
		ts.Fatalf("usage: cmp (-timeout=<duration>) (-grep=<pattern>) <table> <file>")
	}

	var grepRe *regexp.Regexp
	if *grep != "" {
		grepRe, err = regexp.Compile(*grep)
		if err != nil {
			ts.Fatalf("bad grep: %s", err)
		}
	}

	tableName := args[0]
	db, _, tbl := getTable(ts, tableName)
	header := tbl.TableHeader()

	data := ts.ReadFile(args[1])
	lines := strings.Split(data, "\n")
	lines = slices.DeleteFunc(lines, func(line string) bool {
		return strings.TrimSpace(line) == ""
	})
	if len(lines) < 1 {
		ts.Fatalf("%q missing header line, e.g. %q", args[1], strings.Join(header, " "))
	}

	columnNames, columnPositions := splitHeaderLine(lines[0])
	columnIndexes, err := getColumnIndexes(columnNames, header)
	if err != nil {
		ts.Fatalf("%s", err)
	}
	lines = lines[1:]
	origLines := lines
	tryUntil := time.Now().Add(*timeout)

	for {
		lines = origLines

		// Create the diff between 'lines' and the rows in the table.
		equal := true
		var diff bytes.Buffer
		w := tabwriter.NewWriter(&diff, 5, 4, 3, ' ', 0)
		fmt.Fprintf(w, "  %s\n", joinByPositions(columnNames, columnPositions))

		for obj := range tbl.All(db.ReadTxn()) {
			rowRaw := takeColumns(obj.(statedb.TableWritable).TableRow(), columnIndexes)
			row := joinByPositions(rowRaw, columnPositions)
			if grepRe != nil && !grepRe.Match([]byte(row)) {
				continue
			}

			if len(lines) == 0 {
				equal = false
				fmt.Fprintf(w, "- %s\n", row)
				continue
			}
			line := lines[0]
			splitLine := splitByPositions(line, columnPositions)

			if slices.Equal(rowRaw, splitLine) {
				fmt.Fprintf(w, "  %s\n", row)
			} else {
				fmt.Fprintf(w, "- %s\n", row)
				fmt.Fprintf(w, "+ %s\n", line)
				equal = false
			}
			lines = lines[1:]
		}
		for _, line := range lines {
			fmt.Fprintf(w, "+ %s\n", line)
			equal = false
		}
		if equal {
			return
		}
		w.Flush()

		if time.Now().After(tryUntil) {
			ts.Fatalf("table mismatch:\n%s", diff.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func InsertCmd(ts *testscript.TestScript, neg bool, args []string) {
	insertOrDeleteCmd(ts, true, args)
}

func DeleteCmd(ts *testscript.TestScript, neg bool, args []string) {
	insertOrDeleteCmd(ts, false, args)
}

func insertOrDeleteCmd(ts *testscript.TestScript, insert bool, args []string) {
	if len(args) < 2 {
		if insert {
			ts.Fatalf("usage: insert <table> path...")
		} else {
			ts.Fatalf("usage: delete <table> path...")
		}
	}

	db, _, tbl := getTable(ts, args[0])
	wtxn := db.WriteTxn(tbl.Meta)
	defer wtxn.Commit()

	for _, arg := range args[1:] {
		data := ts.ReadFile(arg)
		parts := strings.Split(data, "---")
		for _, part := range parts {
			obj, err := tbl.UnmarshalYAML([]byte(part))
			if err != nil {
				ts.Fatalf("Unmarshal(%s): %s", arg, err)
			}
			if insert {
				_, _, err = tbl.Insert(wtxn, obj)
				if err != nil {
					ts.Fatalf("Insert(%s): %s", arg, err)
				}
			} else {
				_, _, err = tbl.Delete(wtxn, obj)
				if err != nil {
					ts.Fatalf("Delete(%s): %s", arg, err)
				}

			}
		}
	}
}

func PrefixCmd(ts *testscript.TestScript, neg bool, args []string) {
	prefixOrLowerboundCmd(ts, false, args)
}

func LowerBoundCmd(ts *testscript.TestScript, neg bool, args []string) {
	prefixOrLowerboundCmd(ts, true, args)
}

func prefixOrLowerboundCmd(ts *testscript.TestScript, lowerbound bool, args []string) {
	db := getDB(ts)
	if len(args) < 2 {
		if lowerbound {
			ts.Fatalf("usage: lowerbound <table> <key> (-to=<file>)")
		} else {
			ts.Fatalf("usage: prefix <table> <key> (-to=<file>)")
		}
	}

	var flags flag.FlagSet
	file := flags.String("to", "", "File to write to instead of stdout")
	if err := flags.Parse(args[2:]); err != nil {
		ts.Fatalf("bad args: %s", err)
	}

	txn := db.ReadTxn()
	meta := db.GetTable(txn, args[0])
	if meta == nil {
		ts.Fatalf("table %q not found", args[0])
	}
	tbl := statedb.AnyTable{Meta: meta}
	var buf bytes.Buffer
	w := tabwriter.NewWriter(&buf, 5, 4, 3, ' ', 0)
	header := tbl.TableHeader()
	fmt.Fprintf(w, "%s\n", strings.Join(header, "\t"))

	var it iter.Seq2[any, uint64]
	if lowerbound {
		it = tbl.LowerBound(txn, args[1])
	} else {
		it = tbl.Prefix(txn, args[1])
	}

	for obj := range it {
		row := obj.(statedb.TableWritable).TableRow()
		fmt.Fprintf(w, "%s\n", strings.Join(row, "\t"))
	}
	w.Flush()
	if *file == "" {
		ts.Logf("%s", buf.String())
	} else if err := os.WriteFile(ts.MkAbs(*file), buf.Bytes(), 0644); err != nil {
		ts.Fatalf("WriteFile(%s): %s", *file, err)
	}
}

// splitHeaderLine takes a header of column names separated by any
// number of whitespaces and returns the names and their starting positions.
// e.g. "Foo  Bar Baz" would result in ([Foo,Bar,Baz],[0,5,9]).
// With this information we can take a row in the database and format it
// the same way as our test data.
func splitHeaderLine(line string) (names []string, pos []int) {
	start := 0
	skip := true
	for i, r := range line {
		switch r {
		case ' ', '\t':
			if !skip {
				names = append(names, line[start:i])
				pos = append(pos, start)
				start = -1
			}
			skip = true
		default:
			skip = false
			if start == -1 {
				start = i
			}
		}
	}
	if start >= 0 && start < len(line) {
		names = append(names, line[start:])
		pos = append(pos, start)
	}
	return
}

// splitByPositions takes a "row" line and the positions of the header columns
// and extracts the values.
// e.g. if we have the positions [0,5,9] (from header "Foo  Bar Baz") and
// line is "1    a   b", then we'd extract [1,a,b].
// The whitespace on the right of the start position (e.g. "1  \t") is trimmed.
// This of course requires that the table is properly formatted in a way that the
// header columns are indented to fit the data exactly.
func splitByPositions(line string, positions []int) []string {
	out := make([]string, 0, len(positions))
	start := 0
	for _, pos := range positions[1:] {
		if start >= len(line) {
			out = append(out, "")
			start = len(line)
			continue
		}
		out = append(out, strings.TrimRight(line[start:min(pos, len(line))], " \t"))
		start = pos
	}
	out = append(out, strings.TrimRight(line[min(start, len(line)):], " \t"))
	return out
}

// joinByPositions is the reverse of splitByPositions, it takes the columns of a
// row and the starting positions of each and joins into a single line.
// e.g. [1,a,b] and positions [0,5,9] expands to "1    a   b".
// NOTE: This does not deal well with mixing tabs and spaces. The test input
// data should preferably just use spaces.
func joinByPositions(row []string, positions []int) string {
	var w strings.Builder
	prev := 0
	for i, pos := range positions {
		for pad := pos - prev; pad > 0; pad-- {
			w.WriteByte(' ')
		}
		w.WriteString(row[i])
		prev = pos + len(row[i])
	}
	return w.String()
}

func showTable(ts *testscript.TestScript, tableName string, columns ...string) *bytes.Buffer {
	db := getDB(ts)
	txn := db.ReadTxn()
	meta := db.GetTable(txn, tableName)
	if meta == nil {
		ts.Fatalf("table %q not found", tableName)
	}
	tbl := statedb.AnyTable{Meta: meta}

	header := tbl.TableHeader()
	if header == nil {
		ts.Fatalf("objects in table %q not TableWritable", meta.Name())
	}
	var idxs []int
	var err error
	if len(columns) > 0 {
		idxs, err = getColumnIndexes(columns, header)
		header = columns
	} else {
		idxs, err = getColumnIndexes(header, header)
	}
	if err != nil {
		ts.Fatalf("%s", err)
	}

	var buf bytes.Buffer
	w := tabwriter.NewWriter(&buf, 5, 4, 3, ' ', 0)
	fmt.Fprintf(w, "%s\n", strings.Join(header, "\t"))
	for obj := range tbl.All(db.ReadTxn()) {
		row := takeColumns(obj.(statedb.TableWritable).TableRow(), idxs)
		fmt.Fprintf(w, "%s\n", strings.Join(row, "\t"))
	}
	w.Flush()
	return &buf
}

func takeColumns[T any](xs []T, idxs []int) []T {
	// Invariant: idxs is sorted so can set in-place.
	for i, idx := range idxs {
		xs[i] = xs[idx]
	}
	return xs[:len(idxs)]
}

func getColumnIndexes(names []string, header []string) ([]int, error) {
	columnIndexes := make([]int, 0, len(header))
loop:
	for _, name := range names {
		for i, name2 := range header {
			if strings.EqualFold(name, name2) {
				columnIndexes = append(columnIndexes, i)
				continue loop
			}
		}
		return nil, fmt.Errorf("column %q not part of %v", name, header)
	}
	return columnIndexes, nil
}
