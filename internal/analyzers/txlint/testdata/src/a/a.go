package a

import "github.com/cilium/statedb"

type ptrObj struct {
	Value int
}

type valueObj struct {
	Value int
}

func wrongTable(db *statedb.DB, foo, bar statedb.RWTable[int]) {
	wtxn := db.WriteTxn(foo)
	defer wtxn.Abort()
	bar.Insert(wtxn, 1) // want `write transaction "wtxn" does not lock table bar`
}

func useAfterCommit(db *statedb.DB, foo statedb.RWTable[int]) {
	wtxn := db.WriteTxn(foo)
	wtxn.Commit()
	foo.Insert(wtxn, 1) // want `transaction "wtxn" used after Commit\(\)`
}

func mixedSnapshotsDirect(db *statedb.DB, foo statedb.RWTable[int]) {
	wtxn := db.WriteTxn(foo)
	defer wtxn.Abort()
	foo.Get(db.ReadTxn(), statedb.Query[int]{}) // want `read from table foo using db.ReadTxn\(\) while write transaction "wtxn" for the same table is still open`
	_ = wtxn
}

func mixedSnapshotsViaVar(db *statedb.DB, foo statedb.RWTable[int]) {
	wtxn := db.WriteTxn(foo)
	defer wtxn.Abort()
	rtxn := db.ReadTxn()
	foo.NumObjects(rtxn) // want `read from table foo using db.ReadTxn\(\) while write transaction "wtxn" for the same table is still open`
	_ = wtxn
}

func goroutineCapture(db *statedb.DB, foo statedb.RWTable[int]) {
	wtxn := db.WriteTxn(foo)
	defer wtxn.Abort()
	go func() {
		foo.Insert(wtxn, 1) // want `transaction "wtxn" passed to or captured by a goroutine; StateDB transactions are not thread-safe`
	}()
}

func nextWithWriteTxn(db *statedb.DB, foo statedb.RWTable[int]) {
	wtxn := db.WriteTxn(foo)
	defer wtxn.Abort()
	iter, _ := foo.Changes(wtxn)
	defer iter.Close()
	iter.Next(wtxn) // want `ChangeIterator.Next should not be called with a WriteTxn that locks the iterator target table; pass db.ReadTxn\(\) or the ReadTxn returned by Commit\(\) instead`
}

func okCommittedSnapshot(db *statedb.DB, foo statedb.RWTable[int]) {
	wtxn := db.WriteTxn(foo)
	rtxn := wtxn.Commit()
	foo.Get(rtxn, statedb.Query[int]{})
}

func mixedSnapshotsOlderRead(db *statedb.DB, foo statedb.RWTable[int]) {
	rtxn := db.ReadTxn()
	wtxn := db.WriteTxn(foo)
	defer wtxn.Abort()
	foo.NumObjects(rtxn) // want `read from table foo using db.ReadTxn\(\) while write transaction "wtxn" for the same table is still open`
	_ = wtxn
}

func leakedWriteTxn(db *statedb.DB, foo statedb.RWTable[int]) {
	wtxn := db.WriteTxn(foo)
	foo.Insert(wtxn, 1)
} // want `write transaction "wtxn" is not closed on all paths; call Commit\(\), Abort\(\), or defer one of them`

func leakedWriteTxnOnReturn(db *statedb.DB, foo statedb.RWTable[int]) error {
	wtxn := db.WriteTxn(foo)
	foo.Insert(wtxn, 1)
	return nil // want `write transaction "wtxn" is not closed on all paths; call Commit\(\), Abort\(\), or defer one of them`
}

func overwrittenWriteTxn(db *statedb.DB, foo statedb.RWTable[int]) {
	wtxn := db.WriteTxn(foo)
	wtxn = db.WriteTxn(foo) // want `write transaction "wtxn" is overwritten without Commit\(\) or Abort\(\)`
	wtxn.Abort()
}

func leakedChangeIterator(db *statedb.DB, foo statedb.RWTable[int]) {
	wtxn := db.WriteTxn(foo)
	iter, _ := foo.Changes(wtxn)
	wtxn.Commit()
	_ = iter
} // want `change iterator "iter" is not closed on all paths; call Close\(\) or defer iter.Close\(\)`

func leakedChangeIteratorOnReturn(db *statedb.DB, foo statedb.RWTable[int]) error {
	wtxn := db.WriteTxn(foo)
	iter, _ := foo.Changes(wtxn)
	wtxn.Commit()
	_ = iter
	return nil // want `change iterator "iter" is not closed on all paths; call Close\(\) or defer iter.Close\(\)`
}

func overwrittenChangeIterator(db *statedb.DB, foo statedb.RWTable[int]) {
	wtxn := db.WriteTxn(foo)
	iter, _ := foo.Changes(wtxn)
	wtxn.Commit()

	wtxn = db.WriteTxn(foo)
	iter, _ = foo.Changes(wtxn) // want `change iterator "iter" is overwritten without Close\(\)`
	wtxn.Commit()
	iter.Close()
}

func okDeferIteratorClose(db *statedb.DB, foo statedb.RWTable[int]) {
	wtxn := db.WriteTxn(foo)
	iter, _ := foo.Changes(wtxn)
	wtxn.Commit()
	defer iter.Close()
}

func okDeferAbort(db *statedb.DB, foo statedb.RWTable[int]) {
	wtxn := db.WriteTxn(foo)
	defer wtxn.Abort()
	foo.Insert(wtxn, 1)
}

func immutablePointerGet(db *statedb.DB, foo statedb.Table[*ptrObj]) {
	txn := db.ReadTxn()
	obj, _, _ := foo.Get(txn, statedb.Query[*ptrObj]{})
	obj.Value = 1 // want `immutable pointer object "obj" returned from StateDB is mutated; clone before modifying`
}

func immutablePointerAlias(db *statedb.DB, foo statedb.Table[*ptrObj]) {
	txn := db.ReadTxn()
	obj, _, _ := foo.Get(txn, statedb.Query[*ptrObj]{})
	alias := obj
	alias.Value++ // want `immutable pointer object "alias" returned from StateDB is mutated; clone before modifying`
}

func immutablePointerRange(db *statedb.DB, foo statedb.Table[*ptrObj]) {
	txn := db.ReadTxn()
	for obj := range foo.All(txn) {
		obj.Value = 1 // want `immutable pointer object "obj" returned from StateDB is mutated; clone before modifying`
	}
}

func immutablePointerChangeObject(db *statedb.DB, foo statedb.Table[*ptrObj]) {
	wtxn := db.WriteTxn(foo)
	iter, _ := foo.Changes(wtxn)
	defer iter.Close()
	txn := wtxn.Commit()
	changes, _ := iter.Next(txn)
	for change := range changes {
		change.Object.Value = 1 // want `immutable pointer object "change\.Object" returned from StateDB is mutated; clone before modifying`
	}
}

func immutablePointerChangeAlias(db *statedb.DB, foo statedb.Table[*ptrObj]) {
	wtxn := db.WriteTxn(foo)
	iter, _ := foo.Changes(wtxn)
	defer iter.Close()
	txn := wtxn.Commit()
	changes, _ := iter.Next(txn)
	for change := range changes {
		obj := change.Object
		obj.Value = 1 // want `immutable pointer object "obj" returned from StateDB is mutated; clone before modifying`
	}
}

func okImmutableClone(db *statedb.DB, foo statedb.Table[*ptrObj]) {
	txn := db.ReadTxn()
	obj, _, _ := foo.Get(txn, statedb.Query[*ptrObj]{})
	clone := *obj
	clone.Value = 1
}

func okValueObject(db *statedb.DB, foo statedb.Table[valueObj]) {
	txn := db.ReadTxn()
	obj, _, _ := foo.Get(txn, statedb.Query[valueObj]{})
	obj.Value = 1
}
