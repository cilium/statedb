// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package txlint

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"strings"

	"golang.org/x/tools/go/analysis"
)

const statedbPkgPath = "github.com/cilium/statedb"
const StrictSingleLiveTransactionFlag = "strict-single-live-transaction"

var Analyzer = NewAnalyzer()

func NewAnalyzer() *analysis.Analyzer {
	var strictSingleLiveTxn bool
	analyzer := &analysis.Analyzer{
		Name: "statedbtxn",
		Doc:  "reports suspicious StateDB transaction misuse",
		Run: func(pass *analysis.Pass) (any, error) {
			return run(pass, strictSingleLiveTxn)
		},
	}
	analyzer.Flags.BoolVar(
		&strictSingleLiveTxn,
		StrictSingleLiveTransactionFlag,
		false,
		"report more than one live StateDB transaction in a function",
	)
	return analyzer
}

type readTxnSource uint8

const (
	readTxnUnknown readTxnSource = iota
	readTxnFromDB
	readTxnFromCommit
)

var writeTxnMethods = map[string]struct{}{
	"Changes":             {},
	"CompareAndDelete":    {},
	"CompareAndSwap":      {},
	"Delete":              {},
	"DeleteAll":           {},
	"Insert":              {},
	"InsertWatch":         {},
	"Modify":              {},
	"RegisterInitializer": {},
}

var readTxnMethods = map[string]struct{}{
	"All":                 {},
	"AllWatch":            {},
	"Get":                 {},
	"GetWatch":            {},
	"Initialized":         {},
	"List":                {},
	"ListWatch":           {},
	"LowerBound":          {},
	"LowerBoundWatch":     {},
	"NumObjects":          {},
	"PendingInitializers": {},
	"Prefix":              {},
	"PrefixWatch":         {},
	"Revision":            {},
}

type writeTxnInfo struct {
	name          string
	lockedTables  map[string]struct{}
	dynamicTable  bool
	closed        bool
	closedBy      string
	deferredClose bool
	openedAt      token.Pos
}

func (info *writeTxnInfo) clone() *writeTxnInfo {
	if info == nil {
		return nil
	}
	clone := &writeTxnInfo{
		name:          info.name,
		dynamicTable:  info.dynamicTable,
		closed:        info.closed,
		closedBy:      info.closedBy,
		deferredClose: info.deferredClose,
		openedAt:      info.openedAt,
	}
	if len(info.lockedTables) > 0 {
		clone.lockedTables = make(map[string]struct{}, len(info.lockedTables))
		for table := range info.lockedTables {
			clone.lockedTables[table] = struct{}{}
		}
	}
	return clone
}

func (info *writeTxnInfo) locksTable(table string) bool {
	if info == nil || info.dynamicTable {
		return false
	}
	_, ok := info.lockedTables[table]
	if ok {
		return true
	}
	for locked := range info.lockedTables {
		if sameTableExpr(locked, table) {
			return true
		}
	}
	return false
}

func (info *writeTxnInfo) cleanedOnFunctionExit() bool {
	return info != nil && (info.closed || info.deferredClose)
}

type readTxnInfo struct {
	name      string
	source    readTxnSource
	createdAt token.Pos
}

func (info readTxnInfo) clone() readTxnInfo {
	return readTxnInfo{name: info.name, source: info.source, createdAt: info.createdAt}
}

type changeIterInfo struct {
	name          string
	table         string
	closed        bool
	deferredClose bool
	createdAt     token.Pos
}

func (info changeIterInfo) clone() changeIterInfo {
	return changeIterInfo{
		name:          info.name,
		table:         info.table,
		closed:        info.closed,
		deferredClose: info.deferredClose,
		createdAt:     info.createdAt,
	}
}

func (info changeIterInfo) cleanedOnFunctionExit() bool {
	return info.closed || info.deferredClose
}

type immutableObjInfo struct {
	name string
}

func (info immutableObjInfo) clone() immutableObjInfo {
	return immutableObjInfo{name: info.name}
}

type changeVarInfo struct {
	name string
}

func (info changeVarInfo) clone() changeVarInfo {
	return changeVarInfo{name: info.name}
}

type seqKind uint8

const (
	seqReadObject seqKind = iota + 1
	seqChange
)

type seqInfo struct {
	name string
	kind seqKind
}

func (info seqInfo) clone() seqInfo {
	return seqInfo{name: info.name, kind: info.kind}
}

type functionState struct {
	writeTxns     map[types.Object]*writeTxnInfo
	readTxns      map[types.Object]readTxnInfo
	changeIters   map[types.Object]changeIterInfo
	immutableObjs map[types.Object]immutableObjInfo
	changeVars    map[types.Object]changeVarInfo
	seqs          map[types.Object]seqInfo
}

func newFunctionState() *functionState {
	return &functionState{
		writeTxns:     map[types.Object]*writeTxnInfo{},
		readTxns:      map[types.Object]readTxnInfo{},
		changeIters:   map[types.Object]changeIterInfo{},
		immutableObjs: map[types.Object]immutableObjInfo{},
		changeVars:    map[types.Object]changeVarInfo{},
		seqs:          map[types.Object]seqInfo{},
	}
}

func (s *functionState) clone() *functionState {
	clone := newFunctionState()
	seen := map[*writeTxnInfo]*writeTxnInfo{}
	for obj, info := range s.writeTxns {
		if info == nil {
			clone.writeTxns[obj] = nil
			continue
		}
		if copied, ok := seen[info]; ok {
			clone.writeTxns[obj] = copied
			continue
		}
		copied := info.clone()
		seen[info] = copied
		clone.writeTxns[obj] = copied
	}
	for obj, info := range s.readTxns {
		clone.readTxns[obj] = info.clone()
	}
	for obj, info := range s.changeIters {
		clone.changeIters[obj] = info.clone()
	}
	for obj, info := range s.immutableObjs {
		clone.immutableObjs[obj] = info.clone()
	}
	for obj, info := range s.changeVars {
		clone.changeVars[obj] = info.clone()
	}
	for obj, info := range s.seqs {
		clone.seqs[obj] = info.clone()
	}
	return clone
}

func (s *functionState) copyFrom(other *functionState) {
	clone := other.clone()
	s.writeTxns = clone.writeTxns
	s.readTxns = clone.readTxns
	s.changeIters = clone.changeIters
	s.immutableObjs = clone.immutableObjs
	s.changeVars = clone.changeVars
	s.seqs = clone.seqs
}

func (s *functionState) merge(a, b *functionState) {
	writeTxns := map[types.Object]*writeTxnInfo{}
	for obj, infoA := range a.writeTxns {
		infoB, ok := b.writeTxns[obj]
		if !ok || infoA == nil || infoB == nil {
			continue
		}
		merged := infoA.clone()
		merged.closed = infoA.closed && infoB.closed
		switch {
		case infoA.closedBy == infoB.closedBy:
			merged.closedBy = infoA.closedBy
		case merged.closed:
			merged.closedBy = "Commit/Abort"
		default:
			merged.closedBy = ""
		}
		if infoA.cleanedOnFunctionExit() && infoB.cleanedOnFunctionExit() {
			merged.deferredClose = !merged.closed
		} else {
			merged.closed = false
			merged.closedBy = ""
			merged.deferredClose = false
		}
		if infoA.dynamicTable || infoB.dynamicTable || !sameStringSet(infoA.lockedTables, infoB.lockedTables) {
			merged.dynamicTable = true
			merged.lockedTables = nil
		}
		writeTxns[obj] = merged
	}
	readTxns := map[types.Object]readTxnInfo{}
	for obj, infoA := range a.readTxns {
		infoB, ok := b.readTxns[obj]
		if !ok {
			continue
		}
		if infoA.source == infoB.source {
			readTxns[obj] = infoA
			continue
		}
		readTxns[obj] = readTxnInfo{name: infoA.name, source: readTxnUnknown}
	}
	changeIters := map[types.Object]changeIterInfo{}
	for obj, infoA := range a.changeIters {
		infoB, ok := b.changeIters[obj]
		if !ok || infoA.table != infoB.table {
			continue
		}
		merged := infoA.clone()
		merged.closed = infoA.closed && infoB.closed
		if infoA.cleanedOnFunctionExit() && infoB.cleanedOnFunctionExit() {
			merged.deferredClose = !merged.closed
		} else {
			merged.closed = false
			merged.deferredClose = false
		}
		changeIters[obj] = merged
	}
	immutableObjs := map[types.Object]immutableObjInfo{}
	for obj, infoA := range a.immutableObjs {
		if _, ok := b.immutableObjs[obj]; ok {
			immutableObjs[obj] = infoA
		}
	}
	changeVars := map[types.Object]changeVarInfo{}
	for obj, infoA := range a.changeVars {
		if _, ok := b.changeVars[obj]; ok {
			changeVars[obj] = infoA
		}
	}
	seqs := map[types.Object]seqInfo{}
	for obj, infoA := range a.seqs {
		infoB, ok := b.seqs[obj]
		if ok && infoA.kind == infoB.kind {
			seqs[obj] = infoA
		}
	}
	s.writeTxns = writeTxns
	s.readTxns = readTxns
	s.changeIters = changeIters
	s.immutableObjs = immutableObjs
	s.changeVars = changeVars
	s.seqs = seqs
}

func (s *functionState) openWriteTxnForTable(table string) *writeTxnInfo {
	for _, info := range s.writeTxns {
		if info == nil || info.closed || info.dynamicTable {
			continue
		}
		if info.locksTable(table) {
			return info
		}
	}
	return nil
}

func (s *functionState) firstLiveTxnUsedAfter(after token.Pos, lastUses map[types.Object]token.Pos, ignored map[types.Object]struct{}) string {
	var (
		name string
		pos  token.Pos
	)
	consider := func(obj types.Object, createdAt token.Pos, txnName string) {
		if ignored != nil {
			if _, ok := ignored[obj]; ok {
				return
			}
		}
		if lastUse, ok := lastUses[obj]; ok && lastUse <= after {
			return
		}
		if name == "" || createdAt < pos {
			name = txnName
			pos = createdAt
		}
	}
	for obj, info := range s.writeTxns {
		if info == nil || info.closed {
			continue
		}
		consider(obj, info.openedAt, resourceName(info.name, obj.Name()))
	}
	for obj, info := range s.readTxns {
		consider(obj, info.createdAt, resourceName(info.name, obj.Name()))
	}
	return name
}

func sameStringSet(a, b map[string]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if _, ok := b[k]; !ok {
			return false
		}
	}
	return true
}

type functionAnalyzer struct {
	pass                *analysis.Pass
	strictSingleLiveTxn bool
	txnLastUses         map[types.Object]token.Pos
	ignoredTxnObjs      map[types.Object]struct{}
}

func run(pass *analysis.Pass, strictSingleLiveTxn bool) (any, error) {
	if !packageMayUseStateDB(pass) {
		return nil, nil
	}
	analyzer := &functionAnalyzer{pass: pass, strictSingleLiveTxn: strictSingleLiveTxn}
	for _, file := range pass.Files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			analyzer.txnLastUses = collectTxnLastUses(pass, fn.Body)
			analyzer.ignoredTxnObjs = nil
			state := newFunctionState()
			terminated := analyzer.walkBlock(fn.Body, state)
			if !terminated {
				analyzer.reportFunctionExitLeaks(fn.Body.Rbrace, state, nil)
			}
		}
	}
	return nil, nil
}

func packageMayUseStateDB(pass *analysis.Pass) bool {
	for _, imported := range pass.Pkg.Imports() {
		if imported.Path() == statedbPkgPath {
			return true
		}
	}
	for _, obj := range pass.TypesInfo.Uses {
		if objectFromPackage(obj, statedbPkgPath) {
			return true
		}
	}
	for _, selection := range pass.TypesInfo.Selections {
		if selection != nil && objectFromPackage(selection.Obj(), statedbPkgPath) {
			return true
		}
	}
	return false
}

func collectTxnLastUses(pass *analysis.Pass, body *ast.BlockStmt) map[types.Object]token.Pos {
	lastUses := map[types.Object]token.Pos{}
	ast.Inspect(body, func(node ast.Node) bool {
		ident, ok := node.(*ast.Ident)
		if !ok {
			return true
		}
		obj := pass.TypesInfo.ObjectOf(ident)
		if obj == nil || obj.Name() == "_" || !isStateDBTxnType(obj.Type()) {
			return true
		}
		if ident.Pos() > lastUses[obj] {
			lastUses[obj] = ident.Pos()
		}
		return true
	})
	return lastUses
}

func txnObjects(exprs []ast.Expr, pass *analysis.Pass) map[types.Object]struct{} {
	var objs map[types.Object]struct{}
	for _, expr := range exprs {
		ident, ok := unparen(expr).(*ast.Ident)
		if !ok {
			continue
		}
		obj := pass.TypesInfo.ObjectOf(ident)
		if obj == nil || obj.Name() == "_" || !isStateDBTxnType(obj.Type()) {
			continue
		}
		if objs == nil {
			objs = map[types.Object]struct{}{}
		}
		objs[obj] = struct{}{}
	}
	return objs
}

func (a *functionAnalyzer) walkBlock(block *ast.BlockStmt, state *functionState) bool {
	if block == nil {
		return false
	}
	for _, stmt := range block.List {
		if a.walkStmt(stmt, state) {
			return true
		}
	}
	return false
}

func (a *functionAnalyzer) walkStmt(stmt ast.Stmt, state *functionState) bool {
	switch stmt := stmt.(type) {
	case *ast.BlockStmt:
		return a.walkBlock(stmt, state)
	case *ast.AssignStmt:
		a.withIgnoredTxnObjects(txnObjects(stmt.Lhs, a.pass), func() {
			for _, rhs := range stmt.Rhs {
				a.inspectExpr(rhs, state, true)
			}
		})
		a.checkImmutableMutations(stmt.Lhs, state)
		a.applyAssignments(stmt.Lhs, stmt.Rhs, state)
		return false
	case *ast.DeclStmt:
		a.walkDecl(stmt.Decl, state)
		return false
	case *ast.ExprStmt:
		a.inspectExpr(stmt.X, state, true)
		return false
	case *ast.GoStmt:
		a.checkGoStmt(stmt, state)
		return false
	case *ast.DeferStmt:
		a.inspectExpr(stmt.Call, state, false)
		a.checkDeferredCleanup(stmt.Call, state)
		return false
	case *ast.IfStmt:
		if stmt.Init != nil {
			a.walkStmt(stmt.Init, state)
		}
		a.inspectExpr(stmt.Cond, state, false)
		thenState := state.clone()
		thenTerminated := a.walkBlock(stmt.Body, thenState)
		elseState := state.clone()
		elseTerminated := false
		if stmt.Else != nil {
			elseTerminated = a.walkStmt(stmt.Else, elseState)
		}
		switch {
		case thenTerminated && stmt.Else != nil && elseTerminated:
			return true
		case thenTerminated:
			state.copyFrom(elseState)
		case stmt.Else != nil && elseTerminated:
			state.copyFrom(thenState)
		default:
			state.merge(thenState, elseState)
		}
		return false
	case *ast.ForStmt:
		if stmt.Init != nil {
			a.walkStmt(stmt.Init, state)
		}
		a.inspectExpr(stmt.Cond, state, false)
		beforeLoop := state.clone()
		loopState := state.clone()
		a.walkBlock(stmt.Body, loopState)
		if stmt.Post != nil {
			a.walkStmt(stmt.Post, loopState)
		}
		state.merge(beforeLoop, loopState)
		return false
	case *ast.RangeStmt:
		a.inspectExpr(stmt.X, state, false)
		beforeRange := state.clone()
		rangeState := state.clone()
		a.applyRangeBindings(stmt, rangeState)
		a.walkBlock(stmt.Body, rangeState)
		state.merge(beforeRange, rangeState)
		return false
	case *ast.ReturnStmt:
		for _, result := range stmt.Results {
			a.inspectExpr(result, state, false)
		}
		a.reportFunctionExitLeaks(stmt.Return, state, stmt.Results)
		return true
	case *ast.SwitchStmt:
		if stmt.Init != nil {
			a.walkStmt(stmt.Init, state)
		}
		a.inspectExpr(stmt.Tag, state, false)
		a.walkCaseClauses(stmt.Body, state)
		return false
	case *ast.TypeSwitchStmt:
		if stmt.Init != nil {
			a.walkStmt(stmt.Init, state)
		}
		a.walkStmt(stmt.Assign, state)
		a.walkCaseClauses(stmt.Body, state)
		return false
	case *ast.SelectStmt:
		a.walkCommClauses(stmt.Body, state)
		return false
	case *ast.LabeledStmt:
		return a.walkStmt(stmt.Stmt, state)
	case *ast.SendStmt:
		a.inspectExpr(stmt.Chan, state, false)
		a.inspectExpr(stmt.Value, state, false)
		return false
	case *ast.IncDecStmt:
		a.inspectExpr(stmt.X, state, false)
		a.checkImmutableMutation(stmt.X, state)
		return false
	}
	return false
}

func (a *functionAnalyzer) walkCaseClauses(body *ast.BlockStmt, state *functionState) {
	if body == nil {
		return
	}
	var states []*functionState
	for _, stmt := range body.List {
		clause, ok := stmt.(*ast.CaseClause)
		if !ok {
			continue
		}
		caseState := state.clone()
		for _, expr := range clause.List {
			a.inspectExpr(expr, caseState, false)
		}
		for _, bodyStmt := range clause.Body {
			a.walkStmt(bodyStmt, caseState)
		}
		states = append(states, caseState)
	}
	if len(states) == 0 {
		return
	}
	merged := states[0]
	for _, next := range states[1:] {
		tmp := merged.clone()
		tmp.merge(merged, next)
		merged = tmp
	}
	state.merge(state.clone(), merged)
}

func (a *functionAnalyzer) walkCommClauses(body *ast.BlockStmt, state *functionState) {
	if body == nil {
		return
	}
	var states []*functionState
	for _, stmt := range body.List {
		clause, ok := stmt.(*ast.CommClause)
		if !ok {
			continue
		}
		clauseState := state.clone()
		if clause.Comm != nil {
			a.walkStmt(clause.Comm, clauseState)
		}
		for _, bodyStmt := range clause.Body {
			a.walkStmt(bodyStmt, clauseState)
		}
		states = append(states, clauseState)
	}
	if len(states) == 0 {
		return
	}
	merged := states[0]
	for _, next := range states[1:] {
		tmp := merged.clone()
		tmp.merge(merged, next)
		merged = tmp
	}
	state.merge(state.clone(), merged)
}

func (a *functionAnalyzer) walkDecl(decl ast.Decl, state *functionState) {
	gen, ok := decl.(*ast.GenDecl)
	if !ok {
		return
	}
	for _, spec := range gen.Specs {
		valueSpec, ok := spec.(*ast.ValueSpec)
		if !ok {
			continue
		}
		lhs := make([]ast.Expr, 0, len(valueSpec.Names))
		for _, name := range valueSpec.Names {
			lhs = append(lhs, name)
		}
		a.withIgnoredTxnObjects(txnObjects(lhs, a.pass), func() {
			for _, value := range valueSpec.Values {
				a.inspectExpr(value, state, true)
			}
		})
		a.applyAssignments(lhs, valueSpec.Values, state)
	}
}

func (a *functionAnalyzer) withIgnoredTxnObjects(ignored map[types.Object]struct{}, fn func()) {
	prev := a.ignoredTxnObjs
	a.ignoredTxnObjs = ignored
	defer func() {
		a.ignoredTxnObjs = prev
	}()
	fn()
}

func (a *functionAnalyzer) inspectExpr(expr ast.Expr, state *functionState, closeEffects bool) {
	if expr == nil {
		return
	}
	ast.Inspect(expr, func(node ast.Node) bool {
		switch node := node.(type) {
		case *ast.FuncLit:
			return false
		case *ast.CallExpr:
			a.checkCall(node, state, closeEffects)
		}
		return true
	})
}

func (a *functionAnalyzer) checkCall(call *ast.CallExpr, state *functionState, closeEffects bool) {
	recvExpr, method, ok := a.statedbMethod(call)
	if ok {
		if method != "Abort" && method != "Commit" && method != "Close" {
			if obj := a.exprObject(recvExpr); obj != nil {
				if info := state.writeTxns[obj]; info != nil && info.closed {
					a.reportClosedTxn(recvExpr.Pos(), info)
				}
			}
		}
	}

	for _, arg := range call.Args {
		obj := a.exprObject(arg)
		if obj == nil {
			continue
		}
		if info := state.writeTxns[obj]; info != nil && info.closed {
			a.reportClosedTxn(arg.Pos(), info)
		}
	}

	if ok {
		switch method {
		case "ReadTxn", "WriteTxn":
			if a.strictSingleLiveTxn {
				if existing := state.firstLiveTxnUsedAfter(call.Lparen, a.txnLastUses, a.ignoredTxnObjs); existing != "" {
					a.pass.Reportf(call.Lparen, "opening %s while transaction %q is still live; strict-single-live-transaction mode allows only one live transaction at a time", method, existing)
				}
			}
		case "Close":
			obj := a.exprObject(recvExpr)
			info, exists := state.changeIters[obj]
			if exists {
				info.closed = true
				state.changeIters[obj] = info
			}
		}
	}

	if ok && len(call.Args) > 0 {
		switch {
		case isWriteTxnMethod(method):
			table := exprString(recvExpr)
			if table == "" {
				break
			}
			if info, ok := a.writeTxnInfoOfExpr(call.Args[0], state); ok && info != nil && !info.closed && !info.dynamicTable && !info.locksTable(table) {
				a.reportWrongTableLock(call.Lparen, info, table)
			}
		case isReadTxnMethod(method):
			table := exprString(recvExpr)
			if table == "" {
				break
			}
			if readInfo, ok := a.readTxnInfoOfExpr(call.Args[0], state); ok && readInfo.source == readTxnFromDB {
				if info := state.openWriteTxnForTable(table); info != nil {
					a.reportMixedSnapshot(call.Lparen, table, info)
				}
			}
		case method == "Next":
			iterObj := a.exprObject(recvExpr)
			iterInfo, ok := state.changeIters[iterObj]
			if !ok {
				break
			}
			if info, ok := a.writeTxnInfoOfExpr(call.Args[0], state); ok && info != nil && !info.closed && info.locksTable(iterInfo.table) {
				a.pass.Reportf(call.Lparen, "ChangeIterator.Next should not be called with a WriteTxn that locks the iterator target table; pass db.ReadTxn() or the ReadTxn returned by Commit() instead")
			}
		}
	}

	if !closeEffects || !ok {
		return
	}
	if method != "Abort" && method != "Commit" {
		return
	}
	obj := a.exprObject(recvExpr)
	if obj == nil {
		return
	}
	info := state.writeTxns[obj]
	if info == nil {
		return
	}
	info.closed = true
	info.closedBy = method
}

func (a *functionAnalyzer) checkDeferredCleanup(call *ast.CallExpr, state *functionState) {
	recvExpr, method, ok := a.statedbMethod(call)
	if !ok {
		return
	}
	obj := a.exprObject(recvExpr)
	if obj == nil {
		return
	}
	switch method {
	case "Abort", "Commit":
		info := state.writeTxns[obj]
		if info == nil {
			return
		}
		info.deferredClose = true
	case "Close":
		info, exists := state.changeIters[obj]
		if !exists {
			return
		}
		info.deferredClose = true
		state.changeIters[obj] = info
	}
}

func (a *functionAnalyzer) checkGoStmt(stmt *ast.GoStmt, state *functionState) {
	if stmt == nil {
		return
	}

	a.inspectExpr(stmt.Call, state, false)

	reportTxn := func(expr ast.Expr) {
		obj := a.exprObject(expr)
		if obj == nil {
			return
		}
		if info := state.writeTxns[obj]; info != nil {
			a.pass.Reportf(expr.Pos(), "transaction %q passed to or captured by a goroutine; StateDB transactions are not thread-safe", info.name)
			return
		}
		if _, ok := state.readTxns[obj]; ok {
			a.pass.Reportf(expr.Pos(), "transaction %q passed to or captured by a goroutine; StateDB transactions are not thread-safe", obj.Name())
		}
	}

	ast.Inspect(stmt.Call, func(node ast.Node) bool {
		switch node := node.(type) {
		case *ast.FuncLit:
			ast.Inspect(node.Body, func(inner ast.Node) bool {
				ident, ok := inner.(*ast.Ident)
				if !ok {
					return true
				}
				reportTxn(ident)
				return true
			})
			return false
		case *ast.Ident:
			reportTxn(node)
		}
		return true
	})
}

func (a *functionAnalyzer) applyAssignments(lhs, rhs []ast.Expr, state *functionState) {
	if len(lhs) == 0 || len(rhs) == 0 {
		return
	}
	if len(rhs) == 1 && len(lhs) != len(rhs) {
		a.applyMultiResultCallAssignments(lhs, rhs[0], state)
		return
	}
	if len(lhs) != len(rhs) {
		return
	}
	for i, lhsExpr := range lhs {
		obj := a.exprObject(lhsExpr)
		if obj == nil || obj.Name() == "_" {
			continue
		}
		rhsExpr := rhs[i]
		a.assignExpr(lhsExpr.Pos(), obj, rhsExpr, state)
	}
}

func (a *functionAnalyzer) applyMultiResultCallAssignments(lhs []ast.Expr, rhs ast.Expr, state *functionState) {
	call, ok := unparen(rhs).(*ast.CallExpr)
	if !ok {
		for _, lhsExpr := range lhs {
			obj := a.exprObject(lhsExpr)
			if obj == nil || obj.Name() == "_" {
				continue
			}
			a.clearObjectState(lhsExpr.Pos(), obj, state)
		}
		return
	}
	_, method, ok := a.statedbMethod(call)
	if !ok {
		for _, lhsExpr := range lhs {
			obj := a.exprObject(lhsExpr)
			if obj == nil || obj.Name() == "_" {
				continue
			}
			a.clearObjectState(lhsExpr.Pos(), obj, state)
		}
		return
	}

	for _, lhsExpr := range lhs {
		obj := a.exprObject(lhsExpr)
		if obj == nil || obj.Name() == "_" {
			continue
		}
		a.clearObjectState(lhsExpr.Pos(), obj, state)
	}
	if len(lhs) == 0 {
		return
	}

	obj := a.exprObject(lhs[0])
	if obj == nil || obj.Name() == "_" {
		return
	}
	switch method {
	case "Changes":
		info, ok := a.changeIterInfoOfExpr(rhs)
		if !ok {
			return
		}
		a.assignChangeIter(lhs[0].Pos(), obj, state, info)
	case "Get", "GetWatch":
		if !isPointerType(obj.Type()) {
			return
		}
		state.immutableObjs[obj] = immutableObjInfo{name: obj.Name()}
	case "AllWatch", "ListWatch", "LowerBoundWatch", "PrefixWatch", "All", "List", "LowerBound", "Prefix":
		state.seqs[obj] = seqInfo{name: obj.Name(), kind: seqReadObject}
	case "Next":
		state.seqs[obj] = seqInfo{name: obj.Name(), kind: seqChange}
	}
}

func (a *functionAnalyzer) assignExpr(pos token.Pos, obj types.Object, rhs ast.Expr, state *functionState) {
	if info, ok := a.writeTxnInfoOfExpr(rhs, state); ok {
		a.assignWriteTxn(pos, obj, state, info, true)
	} else if isStatedbWriteTxnType(a.pass.TypesInfo.TypeOf(rhs)) {
		a.assignWriteTxn(pos, obj, state, nil, true)
	} else {
		a.assignWriteTxn(pos, obj, state, nil, false)
	}

	if info, ok := a.readTxnInfoOfExpr(rhs, state); ok {
		info.name = obj.Name()
		info.createdAt = rhs.Pos()
		state.readTxns[obj] = info
	} else if isStatedbReadTxnType(a.pass.TypesInfo.TypeOf(rhs)) {
		state.readTxns[obj] = readTxnInfo{name: obj.Name(), source: readTxnUnknown, createdAt: rhs.Pos()}
	} else {
		delete(state.readTxns, obj)
	}

	if info, ok := a.changeIterInfoOfExpr(rhs); ok {
		a.assignChangeIter(pos, obj, state, info)
	} else {
		a.assignChangeIter(pos, obj, state, changeIterInfo{})
	}

	if info, ok := a.immutableObjInfoOfExpr(rhs, state); ok && isPointerType(obj.Type()) {
		info.name = obj.Name()
		state.immutableObjs[obj] = info
	} else {
		delete(state.immutableObjs, obj)
	}

	if info, ok := a.changeVarInfoOfExpr(rhs, state); ok {
		info.name = obj.Name()
		state.changeVars[obj] = info
	} else {
		delete(state.changeVars, obj)
	}

	if info, ok := a.seqInfoOfExpr(rhs, state); ok {
		info.name = obj.Name()
		state.seqs[obj] = info
	} else {
		delete(state.seqs, obj)
	}
}

func (a *functionAnalyzer) assignWriteTxn(pos token.Pos, obj types.Object, state *functionState, info *writeTxnInfo, keep bool) {
	a.checkWriteTxnOverwrite(pos, obj, state, info)
	if info != nil && info.name == "" {
		info = info.clone()
		info.name = obj.Name()
	}
	if keep {
		state.writeTxns[obj] = info
		return
	}
	delete(state.writeTxns, obj)
}

func (a *functionAnalyzer) assignChangeIter(pos token.Pos, obj types.Object, state *functionState, info changeIterInfo) {
	a.checkChangeIterOverwrite(pos, obj, state, info)
	if info.table != "" {
		info.name = obj.Name()
		state.changeIters[obj] = info
		return
	}
	delete(state.changeIters, obj)
}

func (a *functionAnalyzer) clearObjectState(pos token.Pos, obj types.Object, state *functionState) {
	a.assignWriteTxn(pos, obj, state, nil, false)
	delete(state.readTxns, obj)
	a.assignChangeIter(pos, obj, state, changeIterInfo{})
	delete(state.immutableObjs, obj)
	delete(state.changeVars, obj)
	delete(state.seqs, obj)
}

func (a *functionAnalyzer) applyRangeBindings(stmt *ast.RangeStmt, state *functionState) {
	if stmt == nil {
		return
	}
	keyObj := a.exprObject(stmt.Key)
	valueObj := a.exprObject(stmt.Value)
	if keyObj != nil && keyObj.Name() != "_" {
		a.clearObjectState(stmt.Key.Pos(), keyObj, state)
	}
	if valueObj != nil && valueObj.Name() != "_" {
		a.clearObjectState(stmt.Value.Pos(), valueObj, state)
	}

	info, ok := a.seqInfoOfExpr(stmt.X, state)
	if !ok || keyObj == nil || keyObj.Name() == "_" {
		return
	}
	switch info.kind {
	case seqReadObject:
		if isPointerType(keyObj.Type()) {
			state.immutableObjs[keyObj] = immutableObjInfo{name: keyObj.Name()}
		}
	case seqChange:
		if changeVarHasPointerObjectField(keyObj.Type()) {
			state.changeVars[keyObj] = changeVarInfo{name: keyObj.Name()}
		}
	}
}

func (a *functionAnalyzer) checkImmutableMutations(lhs []ast.Expr, state *functionState) {
	for _, expr := range lhs {
		a.checkImmutableMutation(expr, state)
	}
}

func (a *functionAnalyzer) checkImmutableMutation(expr ast.Expr, state *functionState) {
	info, ok := a.mutatedImmutableRoot(expr, state)
	if !ok {
		return
	}
	a.pass.Reportf(expr.Pos(), "immutable pointer object %q returned from StateDB is mutated; clone before modifying", info.name)
}

func (a *functionAnalyzer) mutatedImmutableRoot(expr ast.Expr, state *functionState) (immutableObjInfo, bool) {
	expr = unparen(expr)
	switch expr := expr.(type) {
	case *ast.ParenExpr:
		return a.mutatedImmutableRoot(expr.X, state)
	case *ast.SelectorExpr:
		if info, ok := a.immutableObjInfoOfExpr(expr.X, state); ok {
			return info, true
		}
		return a.mutatedImmutableRoot(expr.X, state)
	case *ast.IndexExpr:
		if info, ok := a.immutableObjInfoOfExpr(expr.X, state); ok {
			return info, true
		}
		return a.mutatedImmutableRoot(expr.X, state)
	case *ast.StarExpr:
		return a.immutableObjInfoOfExpr(expr.X, state)
	default:
		return immutableObjInfo{}, false
	}
}

func (a *functionAnalyzer) checkWriteTxnOverwrite(pos token.Pos, obj types.Object, state *functionState, next *writeTxnInfo) {
	current := state.writeTxns[obj]
	if current == nil || current.cleanedOnFunctionExit() || current == next {
		return
	}
	a.pass.Reportf(pos, "write transaction %q is overwritten without Commit() or Abort()", resourceName(current.name, obj.Name()))
}

func (a *functionAnalyzer) checkChangeIterOverwrite(pos token.Pos, obj types.Object, state *functionState, next changeIterInfo) {
	current, ok := state.changeIters[obj]
	if !ok || current.cleanedOnFunctionExit() {
		return
	}
	if next.table != "" && current.table == next.table && current.createdAt == next.createdAt {
		return
	}
	a.pass.Reportf(pos, "change iterator %q is overwritten without Close()", resourceName(current.name, obj.Name()))
}

func (a *functionAnalyzer) reportFunctionExitLeaks(pos token.Pos, state *functionState, results []ast.Expr) {
	for obj, info := range state.writeTxns {
		if info == nil || info.cleanedOnFunctionExit() || a.returnedObject(results, obj) {
			continue
		}
		a.pass.Reportf(pos, "write transaction %q is not closed on all paths; call Commit(), Abort(), or defer one of them", resourceName(info.name, obj.Name()))
	}
	for obj, info := range state.changeIters {
		if info.cleanedOnFunctionExit() || a.returnedObject(results, obj) {
			continue
		}
		a.pass.Reportf(pos, "change iterator %q is not closed on all paths; call Close() or defer %s.Close()", resourceName(info.name, obj.Name()), resourceName(info.name, obj.Name()))
	}
}

func (a *functionAnalyzer) returnedObject(results []ast.Expr, obj types.Object) bool {
	for _, result := range results {
		if resultObj := a.returnedExprObject(result); resultObj == obj {
			return true
		}
	}
	return false
}

func (a *functionAnalyzer) returnedExprObject(expr ast.Expr) types.Object {
	switch expr := unparen(expr).(type) {
	case *ast.TypeAssertExpr:
		return a.returnedExprObject(expr.X)
	default:
		return a.exprObject(expr)
	}
}

func (a *functionAnalyzer) writeTxnInfoOfExpr(expr ast.Expr, state *functionState) (*writeTxnInfo, bool) {
	if obj := a.exprObject(expr); obj != nil {
		info, ok := state.writeTxns[obj]
		return info, ok
	}

	call, ok := unparen(expr).(*ast.CallExpr)
	if !ok {
		return nil, false
	}
	_, method, ok := a.statedbMethod(call)
	if !ok || method != "WriteTxn" {
		return nil, false
	}
	info := &writeTxnInfo{
		lockedTables: map[string]struct{}{},
		openedAt:     call.Pos(),
	}
	if call.Ellipsis != token.NoPos {
		info.dynamicTable = true
		return info, true
	}
	for _, arg := range call.Args {
		table := exprString(arg)
		if table == "" {
			info.dynamicTable = true
			info.lockedTables = nil
			return info, true
		}
		info.lockedTables[table] = struct{}{}
	}
	return info, true
}

func (a *functionAnalyzer) readTxnSourceOfExpr(expr ast.Expr, state *functionState) (readTxnSource, bool) {
	info, ok := a.readTxnInfoOfExpr(expr, state)
	if !ok {
		return readTxnUnknown, false
	}
	return info.source, true
}

func (a *functionAnalyzer) readTxnInfoOfExpr(expr ast.Expr, state *functionState) (readTxnInfo, bool) {
	if obj := a.exprObject(expr); obj != nil {
		info, ok := state.readTxns[obj]
		return info, ok
	}
	call, ok := unparen(expr).(*ast.CallExpr)
	if !ok {
		return readTxnInfo{}, false
	}
	_, method, ok := a.statedbMethod(call)
	if !ok {
		return readTxnInfo{}, false
	}
	switch method {
	case "ReadTxn":
		return readTxnInfo{source: readTxnFromDB, createdAt: call.Pos()}, true
	case "Commit":
		return readTxnInfo{source: readTxnFromCommit, createdAt: call.Pos()}, true
	default:
		return readTxnInfo{}, false
	}
}

func (a *functionAnalyzer) changeIterInfoOfExpr(expr ast.Expr) (changeIterInfo, bool) {
	call, ok := unparen(expr).(*ast.CallExpr)
	if !ok {
		return changeIterInfo{}, false
	}
	recvExpr, method, ok := a.statedbMethod(call)
	if !ok || method != "Changes" {
		return changeIterInfo{}, false
	}
	return changeIterInfo{table: exprString(recvExpr), createdAt: call.Pos()}, true
}

func (a *functionAnalyzer) immutableObjInfoOfExpr(expr ast.Expr, state *functionState) (immutableObjInfo, bool) {
	if obj := a.exprObject(expr); obj != nil {
		info, ok := state.immutableObjs[obj]
		return info, ok
	}
	sel, ok := unparen(expr).(*ast.SelectorExpr)
	if !ok || sel.Sel == nil || sel.Sel.Name != "Object" {
		return immutableObjInfo{}, false
	}
	if !isPointerType(a.pass.TypesInfo.TypeOf(sel)) {
		return immutableObjInfo{}, false
	}
	base := a.exprObject(sel.X)
	if base == nil {
		return immutableObjInfo{}, false
	}
	if _, ok := state.changeVars[base]; !ok {
		return immutableObjInfo{}, false
	}
	return immutableObjInfo{name: exprString(sel)}, true
}

func (a *functionAnalyzer) changeVarInfoOfExpr(expr ast.Expr, state *functionState) (changeVarInfo, bool) {
	obj := a.exprObject(expr)
	if obj == nil {
		return changeVarInfo{}, false
	}
	info, ok := state.changeVars[obj]
	return info, ok
}

func (a *functionAnalyzer) seqInfoOfExpr(expr ast.Expr, state *functionState) (seqInfo, bool) {
	if obj := a.exprObject(expr); obj != nil {
		info, ok := state.seqs[obj]
		return info, ok
	}
	call, ok := unparen(expr).(*ast.CallExpr)
	if !ok {
		return seqInfo{}, false
	}
	_, method, ok := a.statedbMethod(call)
	if !ok {
		return seqInfo{}, false
	}
	switch method {
	case "All", "List", "LowerBound", "Prefix":
		return seqInfo{name: exprString(expr), kind: seqReadObject}, true
	default:
		return seqInfo{}, false
	}
}

func (a *functionAnalyzer) statedbMethod(call *ast.CallExpr) (ast.Expr, string, bool) {
	sel, ok := unparen(call.Fun).(*ast.SelectorExpr)
	if !ok {
		return nil, "", false
	}
	selection := a.pass.TypesInfo.Selections[sel]
	if selection == nil {
		return nil, "", false
	}
	obj := selection.Obj()
	if obj == nil || obj.Pkg() == nil || obj.Pkg().Path() != statedbPkgPath {
		return nil, "", false
	}
	return sel.X, obj.Name(), true
}

func (a *functionAnalyzer) exprObject(expr ast.Expr) types.Object {
	ident, ok := unparen(expr).(*ast.Ident)
	if !ok {
		return nil
	}
	return a.pass.TypesInfo.ObjectOf(ident)
}

func (a *functionAnalyzer) reportClosedTxn(pos token.Pos, info *writeTxnInfo) {
	if info == nil {
		return
	}
	verb := info.closedBy
	if verb == "" {
		verb = "Commit/Abort"
	}
	a.pass.Reportf(pos, "transaction %q used after %s()", info.name, verb)
}

func (a *functionAnalyzer) reportWrongTableLock(pos token.Pos, info *writeTxnInfo, table string) {
	if info == nil || info.name == "" {
		a.pass.Reportf(pos, "write transaction does not lock table %s", table)
		return
	}
	a.pass.Reportf(pos, "write transaction %q does not lock table %s", info.name, table)
}

func (a *functionAnalyzer) reportMixedSnapshot(pos token.Pos, table string, info *writeTxnInfo) {
	if info == nil || info.name == "" {
		a.pass.Reportf(pos, "read from table %s using db.ReadTxn() while a WriteTxn for the same table is still open", table)
		return
	}
	a.pass.Reportf(pos, "read from table %s using db.ReadTxn() while write transaction %q for the same table is still open", table, info.name)
}

func resourceName(name, fallback string) string {
	if name != "" {
		return name
	}
	return fallback
}

func isWriteTxnMethod(name string) bool {
	_, ok := writeTxnMethods[name]
	return ok
}

func isReadTxnMethod(name string) bool {
	_, ok := readTxnMethods[name]
	return ok
}

func isStatedbWriteTxnType(typ types.Type) bool {
	return isNamedType(typ, statedbPkgPath, "WriteTxn")
}

func isStatedbReadTxnType(typ types.Type) bool {
	return isNamedType(typ, statedbPkgPath, "ReadTxn")
}

func isStateDBTxnType(typ types.Type) bool {
	return isStatedbWriteTxnType(typ) || isStatedbReadTxnType(typ)
}

func isPointerType(typ types.Type) bool {
	_, ok := typ.(*types.Pointer)
	return ok
}

func changeVarHasPointerObjectField(typ types.Type) bool {
	typ = types.Unalias(typ)
	named, ok := typ.(*types.Named)
	if ok {
		typ = named.Underlying()
	}
	strct, ok := typ.(*types.Struct)
	if !ok {
		return false
	}
	for i := 0; i < strct.NumFields(); i++ {
		field := strct.Field(i)
		if field.Name() == "Object" && isPointerType(field.Type()) {
			return true
		}
	}
	return false
}

func isNamedType(typ types.Type, pkgPath, name string) bool {
	for {
		switch t := typ.(type) {
		case *types.Named:
			obj := t.Obj()
			return obj != nil && obj.Pkg() != nil && obj.Pkg().Path() == pkgPath && obj.Name() == name
		case *types.Pointer:
			typ = t.Elem()
		default:
			return false
		}
	}
}

func objectFromPackage(obj types.Object, pkgPath string) bool {
	return obj != nil && obj.Pkg() != nil && obj.Pkg().Path() == pkgPath
}

func exprString(expr ast.Expr) string {
	expr = unparen(expr)
	if expr == nil {
		return ""
	}
	return types.ExprString(expr)
}

func sameTableExpr(a, b string) bool {
	if a == b {
		return true
	}
	if strings.TrimSuffix(a, ".Meta") == b {
		return true
	}
	if strings.TrimSuffix(b, ".Meta") == a {
		return true
	}
	return false
}

func unparen(expr ast.Expr) ast.Expr {
	for {
		paren, ok := expr.(*ast.ParenExpr)
		if !ok {
			return expr
		}
		expr = paren.X
	}
}

func (info *writeTxnInfo) String() string {
	if info == nil {
		return ""
	}
	if info.name != "" {
		return info.name
	}
	return fmt.Sprintf("%v", info.lockedTables)
}
