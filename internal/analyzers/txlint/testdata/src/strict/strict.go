package strict

import "github.com/cilium/statedb"

func readAfterRead(db *statedb.DB) {
	rtxn1 := db.ReadTxn()
	rtxn2 := db.ReadTxn() // want `opening ReadTxn while transaction "rtxn1" is still live; strict-single-live-transaction mode allows only one live transaction at a time`
	_, _ = rtxn1, rtxn2
}

func readAfterWrite(db *statedb.DB, foo statedb.Table[int]) {
	wtxn := db.WriteTxn(foo)
	defer wtxn.Abort()
	rtxn := db.ReadTxn() // want `opening ReadTxn while transaction "wtxn" is still live; strict-single-live-transaction mode allows only one live transaction at a time`
	_, _ = wtxn, rtxn
}

func writeAfterRead(db *statedb.DB, foo statedb.Table[int]) {
	rtxn := db.ReadTxn()
	wtxn := db.WriteTxn(foo) // want `opening WriteTxn while transaction "rtxn" is still live; strict-single-live-transaction mode allows only one live transaction at a time`
	defer wtxn.Abort()
	_, _ = rtxn, wtxn
}

func okWriteAfterDeadRead(db *statedb.DB, foo statedb.Table[int]) {
	rtxn := db.ReadTxn()
	_ = rtxn
	wtxn := db.WriteTxn(foo)
	wtxn.Commit()
}

func okRefreshReadTxn(db *statedb.DB) {
	txn := db.ReadTxn()
	_ = txn
	txn = db.ReadTxn()
	_ = txn
}

func okAfterCommit(db *statedb.DB, foo statedb.Table[int]) {
	wtxn := db.WriteTxn(foo)
	wtxn.Commit()
	rtxn := db.ReadTxn()
	_ = rtxn
}
