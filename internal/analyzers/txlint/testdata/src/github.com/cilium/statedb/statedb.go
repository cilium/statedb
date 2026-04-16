package statedb

import "iter"

type Revision uint64

type DB struct{}

func (db *DB) ReadTxn() ReadTxn {
	return nil
}

func (db *DB) WriteTxn(tables ...TableMeta) WriteTxn {
	return nil
}

type ReadTxn interface{}

type WriteTxn interface {
	ReadTxn
	Abort()
	Commit() ReadTxn
}

type Query[Obj any] struct{}

type Change[Obj any] struct {
	Object   Obj
	Revision Revision
	Deleted  bool
}

type ChangeIterator[Obj any] interface {
	Next(ReadTxn) (iter.Seq2[Change[Obj], Revision], <-chan struct{})
	Close()
}

type TableMeta interface {
	Name() string
}

type Table[Obj any] interface {
	TableMeta
	All(ReadTxn) iter.Seq2[Obj, Revision]
	Get(ReadTxn, Query[Obj]) (Obj, Revision, bool)
	GetWatch(ReadTxn, Query[Obj]) (Obj, Revision, <-chan struct{}, bool)
	List(ReadTxn, Query[Obj]) iter.Seq2[Obj, Revision]
	Changes(WriteTxn) (ChangeIterator[Obj], error)
	NumObjects(ReadTxn) int
	Revision(ReadTxn) Revision
}

type RWTable[Obj any] interface {
	Table[Obj]
	CompareAndSwap(WriteTxn, Revision, Obj) (Obj, bool, error)
	Delete(WriteTxn, Obj)
	Insert(WriteTxn, Obj)
}
