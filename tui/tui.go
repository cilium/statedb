// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

// Package tui provides a small tview-based UI on top of the Hive script shell.
// It drives the existing "db" and "db/show" commands instead of talking to the
// database directly so that the output matches the regular hive shell.
package tui

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
	"syscall"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

const hiveShellEndMarker = "<<end>>"

// Run launches a terminal UI backed by a command runner.
// The runner function is called with commands like "db" or "db/show <table>"
// and should return the stdout (optionally with the hive/shell "<<end>>" marker).
func Run(ctx context.Context, run func(string) (string, error), log *slog.Logger) error {
	if log == nil {
		log = slog.New(slog.NewTextHandler(os.Stdout, nil))
	}

	client := newShellClient(run, log)

	app := newView(client, log)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Stop the UI when the context is cancelled or on SIGINT/SIGTERM.
	go func() {
		select {
		case <-ctx.Done():
		case <-interruptChan():
		}
		app.Stop()
	}()

	return app.Run()
}

type shellClient struct {
	runFunc func(string) (string, error)
	mu      sync.Mutex
	log     *slog.Logger
}

func newShellClient(run func(string) (string, error), log *slog.Logger) *shellClient {
	return &shellClient{
		runFunc: run,
		log:     log,
	}
}

// run executes a single hive shell command and returns stdout until the next prompt.
func (c *shellClient) run(cmd string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	out, err := c.runFunc(cmd)
	if err != nil {
		return out, err
	}
	return strings.TrimSpace(stripEndMarker(out)), nil
}

type view struct {
	app         *tview.Application
	client      *shellClient
	log         *slog.Logger
	filterInput *tview.InputField
	statusInfo  *tview.TextView
	statusMsg   *tview.TextView
	viewFlex    *tview.Flex
	focusOrder  []tview.Primitive
	tableNames  []string
	filterText  string
	views       []*tableView
	filterFocus bool
}

type tableView struct {
	root       *tview.Flex
	tableDrop  *tview.DropDown
	sortDrop   *tview.DropDown
	filterDrop *tview.DropDown
	dataTable  *tview.Table
	table      string
	header     []string
	rows       [][]string
	filterCol  int
	sortCol    int
	sortAsc    bool
	lastRow    int
}

func newView(client *shellClient, log *slog.Logger) *view {
	v := &view{
		app:    tview.NewApplication(),
		client: client,
		log:    log,
	}
	v.app.EnableMouse(true)

	v.viewFlex = tview.NewFlex().SetDirection(tview.FlexRow)

	v.filterInput = tview.NewInputField().
		SetLabel("Search: ").
		SetPlaceholder("<regexp>").
		SetChangedFunc(func(text string) {
			v.filterText = text
			v.applyFilterAndSortAll()
		})
	v.filterInput.SetFieldWidth(40)
	v.filterInput.SetFocusFunc(func() { v.filterFocus = true })
	v.filterInput.SetBlurFunc(func() { v.filterFocus = false })

	v.statusInfo = tview.NewTextView().
		SetDynamicColors(true).
		SetText("[yellow]Tab: focus  N: new pane  n/p: next/prev table  r: reload  R: refresh schema  q: close pane  Ctrl+Q: quit")
	v.statusMsg = tview.NewTextView().
		SetDynamicColors(true).
		SetTextAlign(tview.AlignRight)

	v.app.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		switch event.Key() {
		case tcell.KeyTAB:
			v.focusNext(false)
			return nil
		case tcell.KeyBacktab:
			v.focusNext(true)
			return nil
		case tcell.KeyCtrlQ:
			v.app.Stop()
			return nil
		}

		if v.filterFocus {
			return event
		}

		switch event.Rune() {
		case 'q':
			v.closeFocusedView()
			return nil
		case 'r':
			v.reloadAll()
			return nil
		case 'R':
			v.refreshTables()
			return nil
		case 'N':
			v.addTableView("")
			return nil
		case 'n':
			v.selectNextTable(+1)
			return nil
		case 'p':
			v.selectNextTable(-1)
			return nil
		}
		return event
	})

	controlBar := tview.NewFlex().
		AddItem(v.filterInput, 0, 1, true)

	root := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(v.viewFlex, 0, 1, true).
		AddItem(controlBar, 1, 0, false).
		AddItem(tview.NewFlex().
			AddItem(v.statusInfo, 0, 3, false).
			AddItem(v.statusMsg, 0, 2, false), 1, 0, false)

	v.app.SetRoot(root, true)

	return v
}

func (v *view) Run() error {
	if err := v.refreshTables(); err != nil {
		return err
	}
	if len(v.views) == 0 {
		v.addTableView("")
	}
	return v.app.Run()
}

func (v *view) Stop() {
	v.app.Stop()
}

func (v *view) refreshTables() error {
	out, err := v.client.run("db")
	if err != nil {
		v.setStatus(fmt.Sprintf("[red]db: %v", err))
		return err
	}

	table, err := parseTable(out)
	if err != nil {
		v.setStatus(fmt.Sprintf("[red]parse db: %v", err))
		return err
	}

	nameIdx := findColumn(table.Header, "Name")
	v.tableNames = nil
	for _, row := range table.Rows {
		name := valueAt(row, nameIdx)
		if name != "" {
			v.tableNames = append(v.tableNames, name)
		}
	}
	if len(v.views) == 0 && len(v.tableNames) > 0 {
		v.addTableView(v.tableNames[0])
	} else {
		for _, tv := range v.views {
			v.updateTableOptions(tv)
			if tv.table == "" && len(v.tableNames) > 0 {
				tv.table = v.tableNames[0]
				tv.tableDrop.SetCurrentOption(0)
				v.loadTable(tv, tv.table)
			}
		}
	}

	v.setStatus("[green]Schema refreshed")
	return nil
}

func (v *view) applyFilterAndSortAll() {
	for _, tv := range v.views {
		tv.applyFilter(v.filterText)
	}
}

func (v *view) setStatus(msg string) {
	v.statusMsg.SetText(msg)
}

func (v *view) focusNext(reverse bool) {
	if len(v.focusOrder) == 0 {
		return
	}
	current := v.app.GetFocus()
	idx := -1
	for i, p := range v.focusOrder {
		if p == current {
			idx = i
			break
		}
	}
	if idx == -1 {
		idx = 0
	}
	if reverse {
		idx = (idx - 1 + len(v.focusOrder)) % len(v.focusOrder)
	} else {
		idx = (idx + 1) % len(v.focusOrder)
	}
	v.app.SetFocus(v.focusOrder[idx])
}

func (v *view) closeFocusedView() {
	focus := v.app.GetFocus()
	for i, tv := range v.views {
		if focus == tv.dataTable || focus == tv.tableDrop || focus == tv.sortDrop || focus == tv.filterDrop {
			v.viewFlex.RemoveItem(tv.root)
			v.views = append(v.views[:i], v.views[i+1:]...)
			v.rebuildFocusOrder()
			if len(v.views) == 0 && len(v.tableNames) > 0 {
				v.addTableView(v.tableNames[0])
			}
			return
		}
	}
}

func (v *view) selectNextTable(delta int) {
	if len(v.tableNames) == 0 || len(v.views) == 0 {
		return
	}
	// Use focused view if possible, else first.
	var tv *tableView
	focus := v.app.GetFocus()
	for _, t := range v.views {
		if focus == t.dataTable || focus == t.tableDrop || focus == t.sortDrop || focus == t.filterDrop {
			tv = t
			break
		}
	}
	if tv == nil {
		tv = v.views[0]
	}
	// Find current index in tableNames.
	curIdx := 0
	for i, name := range v.tableNames {
		if name == tv.table {
			curIdx = i
			break
		}
	}
	nextIdx := (curIdx + delta + len(v.tableNames)) % len(v.tableNames)
	tv.tableDrop.SetCurrentOption(nextIdx)
	tv.table = v.tableNames[nextIdx]
	tv.dataTable.Clear()
	v.loadTable(tv, tv.table)
}

func (v *view) addTableView(initial string) {
	if len(v.tableNames) == 0 {
		v.setStatus("[red]No tables available")
		return
	}
	if initial == "" {
		initial = v.tableNames[0]
	}
	tv := &tableView{
		table:     initial,
		sortAsc:   true,
		sortCol:   0,
		filterCol: 0,
		lastRow:   1,
	}

	tv.tableDrop = tview.NewDropDown()
	tv.tableDrop.SetLabel("Table: ")
	tv.tableDrop.SetFieldWidth(32)
	tv.tableDrop.SetBackgroundColor(tcell.ColorGray)
	tv.sortDrop = tview.NewDropDown()
	tv.sortDrop.SetLabel("Sort by: ")
	tv.sortDrop.SetFieldWidth(32)
	tv.sortDrop.SetBackgroundColor(tcell.ColorGray)
	tv.filterDrop = tview.NewDropDown()
	tv.filterDrop.SetLabel("Search by: ")
	tv.filterDrop.SetFieldWidth(32)
	tv.filterDrop.SetBorder(false)
	tv.filterDrop.SetBackgroundColor(tcell.ColorGray)
	tv.dataTable = tview.NewTable().
		SetBorders(false).
		SetFixed(1, 1).
		SetSelectable(true, true).
		SetSeparator(tview.Borders.Vertical)

	tv.dataTable.SetSelectionChangedFunc(func(row, column int) {
		if row != 0 || column < 0 || column >= len(tv.header) {
			return
		}
		if column == tv.sortCol {
			tv.sortAsc = !tv.sortAsc
		} else {
			tv.sortCol = column
			tv.sortAsc = true
		}
		tv.sortDrop.SetCurrentOption(tv.sortCol)
		tv.applyFilter(v.filterText)
		// Move selection back to data row to avoid keeping header selected.
		targetRow := tv.lastRow
		if targetRow == 0 && len(tv.rows) > 0 {
			targetRow = 1
		}
		tv.dataTable.Select(targetRow, column)
	})

	// Wire callbacks
	tv.sortDrop.SetSelectedFunc(func(_ string, idx int) {
		if idx == tv.sortCol {
			tv.sortAsc = !tv.sortAsc
		} else {
			tv.sortCol = idx
			tv.sortAsc = true
		}
		v.applyFilterAndSortAll()
	})
	tv.filterDrop.SetSelectedFunc(func(_ string, idx int) {
		tv.filterCol = idx
		v.applyFilterAndSortAll()
	})

	spacer := tview.NewBox().SetBackgroundColor(tcell.ColorGray)
	control := tview.NewFlex().
		AddItem(tv.tableDrop, 32, 0, false). // fixed width to keep controls left
		AddItem(tv.sortDrop, 32, 0, false).
		AddItem(tv.filterDrop, 32, 0, false).
		AddItem(spacer, 0, 1, false) // spacer consumes remaining width

	tv.root = tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(control, 1, 0, false).
		AddItem(tv.dataTable, 0, 1, true)
	tv.root.SetBorder(true)

	v.updateTableOptions(tv)
	v.views = append(v.views, tv)
	v.viewFlex.AddItem(tv.root, 0, 1, false)
	v.rebuildFocusOrder()

	v.loadTable(tv, initial)
}

type parsedTable struct {
	Header []string
	Rows   [][]string
}

func parseTable(out string) (parsedTable, error) {
	out = strings.ReplaceAll(out, "\r", "")
	lines := slices.DeleteFunc(strings.Split(out, "\n"), func(s string) bool {
		return strings.TrimSpace(s) == ""
	})
	if len(lines) == 0 {
		return parsedTable{}, fmt.Errorf("empty table output")
	}

	// Pick the first non-empty line as header.
	headerLine := ""
	var rest []string
	for i, line := range lines {
		line = strings.TrimSpace(line)
		if line == "[stdout]" || line == "" {
			continue
		}
		headerLine = stripMagic(line)
		rest = lines[i+1:]
		break
	}
	if headerLine == "" {
		return parsedTable{}, fmt.Errorf("no header line detected")
	}

	header, positions := splitHeaderLine(headerLine)
	splitByTabs := strings.ContainsRune(headerLine, '\t')
	usePositions := len(header) > 0
	if !usePositions {
		header = strings.Fields(headerLine)
	}
	if len(header) == 0 {
		return parsedTable{}, fmt.Errorf("no header columns detected")
	}

	rows := make([][]string, 0, len(rest))
	for _, line := range rest {
		line = stripMagic(line)
		if strings.TrimSpace(line) == "" {
			continue
		}
		switch {
		case usePositions:
			rows = append(rows, splitByPositions(line, positions, splitByTabs))
		case splitByTabs:
			rows = append(rows, strings.Split(line, "\t"))
		default:
			rows = append(rows, strings.Fields(line))
		}
	}
	return parsedTable{Header: header, Rows: rows}, nil
}

func stripMagic(line string) string {
	return strings.Map(func(r rune) rune {
		if r == 0xfe {
			return -1
		}
		return r
	}, line)
}

func stripEndMarker(out string) string {
	out = strings.ReplaceAll(out, "\r", "")
	if idx := strings.LastIndex(out, hiveShellEndMarker); idx >= 0 {
		out = out[:idx]
	}
	return out
}

func findColumn(header []string, name string) int {
	for i, col := range header {
		if strings.EqualFold(col, name) {
			return i
		}
	}
	return -1
}

func valueAt(row []string, idx int) string {
	if idx >= 0 && idx < len(row) {
		return row[idx]
	}
	return ""
}

func tableHeaderCell(text string) *tview.TableCell {
	return tview.NewTableCell(text).
		SetAlign(tview.AlignLeft).
		SetAttributes(tcell.AttrBold | tcell.AttrUnderline).
		SetSelectable(false)

}

// splitHeaderLine mirrors the helper used by the script commands to extract
// column names and their start offsets from a formatted header line.
func splitHeaderLine(line string) (names []string, pos []int) {
	start := 0
	for i := 0; i < len(line); {
		if line[i] == ' ' || line[i] == '\t' {
			runStart := i
			hasTab := false
			for i < len(line) && (line[i] == ' ' || line[i] == '\t') {
				if line[i] == '\t' {
					hasTab = true
				}
				i++
			}
			if hasTab || i-runStart >= 2 {
				if start < runStart {
					names = append(names, strings.TrimSpace(line[start:runStart]))
					pos = append(pos, start)
				}
				start = i
			}
			continue
		}
		i++
	}
	if start < len(line) {
		names = append(names, strings.TrimSpace(line[start:]))
		pos = append(pos, start)
	}
	return
}

// splitByPositions mirrors the script helper used to parse rows formatted by a
// tabwriter. The start positions are derived from splitHeaderLine.
func splitByPositions(line string, positions []int, splitByTabs bool) []string {
	out := make([]string, 0, len(positions))
	start := 0
	for i, pos := range positions[1:] {
		if start >= len(line) {
			out = append(out, "")
			start = len(line)
			continue
		}
		if splitByTabs {
			s := strings.Split(line[start:min(pos, len(line))], "\t")[i]
			out = append(out, s)
			start += len(s) + i
		} else {
			out = append(out, strings.TrimRight(line[start:min(pos, len(line))], " \t"))
			start = pos
		}
	}
	if splitByTabs {
		out = append(out, strings.Split(line[min(start, len(line)):], "\t")...)
	} else {
		out = append(out, strings.TrimRight(line[min(start, len(line)):], " \t"))
	}
	return out
}

func interruptChan() <-chan os.Signal {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	return ch
}
func (v *view) updateTableOptions(tv *tableView) {
	tv.tableDrop.SetOptions(v.tableNames, func(name string, _ int) {
		tv.table = name
		tv.dataTable.Clear()
		v.loadTable(tv, name)
	})
	for i, name := range v.tableNames {
		if name == tv.table {
			tv.tableDrop.SetCurrentOption(i)
			break
		}
	}
}

func (v *view) loadTable(tv *tableView, name string) {
	if name == "" {
		return
	}
	target := name
	v.setStatus(fmt.Sprintf("[yellow]Loading %s...", name))

	go func() {
		out, err := v.client.run("db/show " + name)
		if err != nil {
			v.app.QueueUpdateDraw(func() {
				v.setStatus(fmt.Sprintf("[red]db/show %s: %v", name, err))
			})
			return
		}

		table, err := parseTable(out)
		if err != nil {
			v.app.QueueUpdateDraw(func() {
				v.setStatus(fmt.Sprintf("[red]parse %s: %v", name, err))
			})
			return
		}

		v.app.QueueUpdateDraw(func() {
			if tv.table != target {
				return // stale response after table changed
			}
			tv.table = target
			tv.header = table.Header
			tv.rows = table.Rows
			tv.installColumns(v.filterText, v.applyFilterAndSortAll)
			if len(tv.rows) == 0 {
				tv.lastRow = 0
			} else {
				tv.lastRow = 1
			}
			tv.applyFilter(v.filterText)
			v.setStatus(fmt.Sprintf("[green]%s loaded (%d rows)", name, len(table.Rows)))
		})
	}()
}

func (v *view) reloadAll() {
	for _, tv := range v.views {
		v.loadTable(tv, tv.table)
	}
}

func (v *view) rebuildFocusOrder() {
	var order []tview.Primitive
	order = append(order, v.filterInput)
	for _, tv := range v.views {
		order = append(order, tv.tableDrop, tv.sortDrop, tv.filterDrop, tv.dataTable)
	}
	v.focusOrder = order
	if v.app.GetFocus() == nil && len(order) > 0 {
		v.app.SetFocus(order[0])
	}
}

func (tv *tableView) installColumns(filterText string, apply func()) {
	cols := tv.header
	tv.sortDrop.SetOptions(cols, func(_ string, idx int) {
		if idx == tv.sortCol {
			tv.sortAsc = !tv.sortAsc
		} else {
			tv.sortCol = idx
			tv.sortAsc = true
		}
		apply()
	})
	tv.filterDrop.SetOptions(cols, func(_ string, idx int) {
		tv.filterCol = idx
		apply()
	})
	if tv.sortCol >= len(cols) {
		tv.sortCol = 0
	}
	if tv.filterCol >= len(cols) {
		tv.filterCol = 0
	}
	tv.sortDrop.SetCurrentOption(tv.sortCol)
	tv.filterDrop.SetCurrentOption(tv.filterCol)
	apply()
}

func (tv *tableView) applyFilter(filterText string) {
	if len(tv.header) == 0 {
		return
	}
	rows := tv.rows

	if filterText != "" && tv.filterCol < len(tv.header) {
		re, err := regexp.Compile(filterText)
		if err != nil {
			return
		}
		filtered := make([][]string, 0, len(rows))
		for _, row := range rows {
			if tv.filterCol < len(row) && re.MatchString(row[tv.filterCol]) {
				filtered = append(filtered, row)
			}
		}
		rows = filtered
	}

	if tv.sortCol < len(tv.header) {
		sort.SliceStable(rows, func(i, j int) bool {
			a := valueAt(rows[i], tv.sortCol)
			b := valueAt(rows[j], tv.sortCol)
			if tv.sortAsc {
				return a < b
			}
			return a > b
		})
	}

	tv.renderRows(rows)
}

func (tv *tableView) renderRows(rows [][]string) {
	tv.dataTable.Clear()
	tv.dataTable.SetFixed(1, 0).SetOffset(0, 0)
	for i, col := range tv.header {
		icon := ""
		if i == tv.sortCol {
			if tv.sortAsc {
				icon += "▲"
			} else {
				icon += "▼"
			}
		}
		if i == tv.filterCol {
			icon += "🔍"
		}
		label := col
		if icon != "" {
			label = fmt.Sprintf("%s %s", icon, col)
		}
		tv.dataTable.SetCell(0, i, tableHeaderCell(label).SetExpansion(1).SetMaxWidth(0).SetText(fmt.Sprintf(" %s ", label)))
	}
	for r, row := range rows {
		for c, val := range row {
			tv.dataTable.SetCell(r+1, c, tview.NewTableCell(fmt.Sprintf(" %s ", val)).SetExpansion(1).SetMaxWidth(0))
		}
	}
}
