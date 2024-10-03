package statedb

import (
	"iter"
)

// AnyTable allows any-typed access to a StateDB table. This is intended
// for building generic tooling for accessing the table and should be
// avoided if possible.
type AnyTable struct {
	Meta TableMeta
}

func (t AnyTable) All(txn ReadTxn) iter.Seq2[any, Revision] {
	indexTxn := txn.getTxn().mustIndexReadTxn(t.Meta, PrimaryIndexPos)
	return partSeq[any](indexTxn.Iterator())
}

func (t AnyTable) UnmarshalYAML(data []byte) (any, error) {
	return t.Meta.unmarshalYAML(data)
}

func (t AnyTable) Insert(txn WriteTxn, obj any) (old any, hadOld bool, err error) {
	var iobj object
	iobj, hadOld, err = txn.getTxn().insert(t.Meta, Revision(0), obj)
	if hadOld {
		old = iobj.data
	}
	return
}

func (t AnyTable) Delete(txn WriteTxn, obj any) (old any, hadOld bool, err error) {
	var iobj object
	iobj, hadOld, err = txn.getTxn().delete(t.Meta, Revision(0), obj)
	if hadOld {
		old = iobj.data
	}
	return
}

func (t AnyTable) Prefix(txn ReadTxn, key string) iter.Seq2[any, Revision] {
	indexTxn := txn.getTxn().mustIndexReadTxn(t.Meta, PrimaryIndexPos)
	iter, _ := indexTxn.Prefix([]byte(key))
	if indexTxn.unique {
		return partSeq[any](iter)
	}
	return nonUniqueSeq[any](iter, true, []byte(key))
}

func (t AnyTable) LowerBound(txn ReadTxn, key string) iter.Seq2[any, Revision] {
	indexTxn := txn.getTxn().mustIndexReadTxn(t.Meta, PrimaryIndexPos)
	iter := indexTxn.LowerBound([]byte(key))
	if indexTxn.unique {
		return partSeq[any](iter)
	}
	return nonUniqueLowerBoundSeq[any](iter, []byte(key))
}

func (t AnyTable) TableHeader() []string {
	zero := t.Meta.proto()
	if tw, ok := zero.(TableWritable); ok {
		return tw.TableHeader()
	}
	return nil
}

func (t AnyTable) Proto() any {
	return t.Meta.proto()
}
