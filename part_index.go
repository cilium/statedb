package statedb

import (
	"iter"

	"github.com/cilium/statedb/index"
	"github.com/cilium/statedb/part"
)

// Index implements the indexing of objects (FromObjects) and querying of objects from the index (FromKey)
type Index[Obj any, Key any] struct {
	// Name of the index
	Name string

	// FromObject extracts key(s) from the object. The key set
	// can contain 0, 1 or more keys. Must contain exactly one
	// key for primary indices.
	FromObject func(obj Obj) index.KeySet

	// FromKey converts the index key into a raw key.
	// With this we can perform Query() against this index with
	// the [Key] type.
	FromKey func(key Key) index.Key

	// FromString is an optional conversion from string to a raw key.
	// If implemented allows script commands to query with this index.
	FromString func(key string) (index.Key, error)

	// Unique marks the index as unique. Primary index must always be
	// unique. A secondary index may be non-unique in which case a single
	// key may map to multiple objects.
	Unique bool
}

func (i Index[Obj, Key]) isIndexerOf(Obj) {
	panic("isIndexerOf")
}

// fromString implements Indexer.
func (i Index[Obj, Key]) fromString(s string) (indexKey, error) {
	return i.FromString(s)
}

var _ Indexer[struct{}] = &Index[struct{}, bool]{}

// The nolint:unused below are needed due to linter not seeing
// the use-sites due to generics.

//nolint:unused
func (i Index[Key, Obj]) indexName() string {
	return i.Name
}

/*
//nolint:unused
func (i Index[Obj, Key]) fromObject(obj Obj) index.KeySet {
	return i.FromObject(obj)
}

var errFromStringNil = errors.New("FromString not defined")

//nolint:unused
func (i Index[Obj, Key]) fromString(s string) (index.Key, error) {
	if i.FromString == nil {
		return index.Key{}, errFromStringNil
	}
	k, err := i.FromString(s)
	k = i.encodeKey(k)
	return k, err
}

//nolint:unused
func (i Index[Obj, Key]) isUnique() bool {
	return i.Unique
}
*/

func (r partIndex) encodeKey(ikey indexKey) []byte {
	key := ikey.(index.Key)
	if !r.unique {
		return encodeNonUniqueBytes(key)
	}
	return key
}

// Query constructs a query against this index from a key.
func (i Index[Obj, Key]) Query(key Key) Query[Obj] {
	return Query[Obj]{
		index: i.Name,
		key:   i.FromKey(key),
	}
}

func (i Index[Obj, Key]) QueryFromObject(obj Obj) Query[Obj] {
	return Query[Obj]{
		index: i.Name,
		key:   i.FromObject(obj).First(),
	}
}

// QueryFromKey constructs a query against the index using the given
// user-supplied key. Be careful when using this and prefer [Index.Query]
// over this if possible.
func (i Index[Obj, Key]) QueryFromKey(key index.Key) Query[Obj] {
	return Query[Obj]{
		index: i.Name,
		key:   key,
	}
}

func (i Index[Obj, Key]) ObjectToKey(obj Obj) index.Key {
	return i.FromObject(obj).First()
}

func (i Index[Obj, Key]) newTableIndex() tableIndex {
	return partIndex{
		tree: part.New[object](),
		objectToKeys: func(obj object) index.KeySet {
			return i.FromObject(obj.data.(Obj))
		},
		unique: i.Unique,
	}
}

type partIndex struct {
	tree         part.Ops[object]
	objectToKeys func(object) index.KeySet
	unique       bool
}

// list implements tableIndex.
func (r partIndex) list(ikey indexKey) (iter.Seq[object], <-chan struct{}) {
	key := ikey.(index.Key)

	if r.unique {
		// Unique index means that there can be only a single matching object.
		// Doing a Get() is more efficient than constructing an iterator.
		obj, watch, ok := r.tree.Get(key)
		seq := func(yield func(object) bool) {
			if ok {
				yield(obj)
			}
		}
		return seq, watch
	}

	key = encodeNonUniqueBytes(key)

	// For a non-unique index we do a prefix search. The keys are of
	// form <secondary key><primary key><secondary key length>, and thus the
	// iteration will continue until key length mismatches, e.g. we hit a
	// longer key sharing the same prefix.
	iter, watch := r.tree.Prefix(key)
	return nonUniquePartSeq(iter, false, key), watch
}

// rootWatch implements tableIndex.
func (r partIndex) rootWatch() <-chan struct{} {
	return r.tree.RootWatch()
}

func (r partIndex) objectToKey(obj object) indexKey {
	return r.objectToKeys(obj).First()
}

// get implements tableIndex.
func (r partIndex) get(ikey indexKey) (iobj object, watch <-chan struct{}, found bool) {
	searchKey := ikey.(index.Key)
	if r.unique {
		// On a unique index we can do a direct get rather than a prefix search.
		return r.tree.Get(searchKey)
	}

	searchKey = encodeNonUniqueBytes(searchKey)

	// For a non-unique index we need to do a prefix search.
	iter, watch := r.tree.Prefix(searchKey)
	for {
		var key []byte
		key, iobj, found = iter.Next()
		if !found {
			break
		}

		// Check that we have a full match on the key
		if nonUniqueKey(key).secondaryLen() == len(searchKey) {
			break
		}
	}
	return iobj, watch, found
}

// len implements tableIndex.
func (r partIndex) len() int {
	return r.tree.Len()
}

func (r partIndex) all() (iter.Seq2[indexKey, object], <-chan struct{}) {
	return func(yield func(indexKey, object) bool) {
		iter := r.tree.Iterator()
		for {
			key, iobj, ok := iter.Next()
			if !ok {
				break
			}
			if !yield(key, iobj) {
				break
			}
		}
	}, r.rootWatch()
}

// prefix implements tableIndex.
func (r partIndex) prefix(ikey indexKey) (iter.Seq[object], <-chan struct{}) {
	key := ikey.(index.Key)
	if !r.unique {
		key = encodeNonUniqueBytes(key)
	}
	iter, watch := r.tree.Prefix(key)
	if r.unique {
		return partSeq(iter), watch
	}
	return nonUniquePartSeq(iter, true, key), watch
}

// lowerBound implements tableIndexTxn.
func (r partIndex) lowerBound(ikey indexKey) (iter.Seq[object], <-chan struct{}) {
	key := ikey.(index.Key)
	if !r.unique {
		key = encodeNonUniqueBytes(key)
	}

	// Since LowerBound query may be invalidated by changes in another branch
	// of the tree, we cannot just simply watch the node we seeked to. Instead
	// we watch the whole table for changes.
	watch := r.rootWatch()
	iter := r.tree.LowerBound(key)
	if r.unique {
		return partSeq(iter), watch
	}
	return nonUniqueLowerBoundPartSeq(iter, key), watch
}

// txn implements tableIndex.
func (r partIndex) txn() tableIndexTxn {
	return partIndexTxn{index: r, txn: r.tree.Txn()}
}

var _ tableIndex = partIndex{}

type partIndexTxn struct {
	index partIndex
	txn   *part.Txn[object]
}

// deleteByKey implements tableIndexTxn.
func (r partIndexTxn) deleteByKey(ikey indexKey) (old object, hadOld bool) {
	// FIXME figure out why type info is lost
	switch ikey := ikey.(type) {
	case []byte:
		return r.txn.Delete(ikey)
	case index.Key:
		return r.txn.Delete(ikey)
	default:
		panic("what")
	}
}

// all implements tableIndexTxn.
func (r partIndexTxn) all() (iter.Seq2[indexKey, object], <-chan struct{}) {
	return partIndex{
		tree:         r.txn.Clone(),
		objectToKeys: r.index.objectToKeys,
		unique:       r.index.unique,
	}.all()
}

// list implements tableIndexTxn.
func (r partIndexTxn) list(ikey indexKey) (iter.Seq[object], <-chan struct{}) {
	return partIndex{
		tree:         r.txn.Clone(),
		objectToKeys: r.index.objectToKeys,
		unique:       r.index.unique,
	}.list(ikey)
}

// lowerBound implements tableIndexTxn.
func (r partIndexTxn) lowerBound(ikey indexKey) (iter.Seq[object], <-chan struct{}) {
	// TODO: how much does this cost?
	return partIndex{
		tree:         r.txn.Clone(),
		objectToKeys: r.index.objectToKeys,
		unique:       r.index.unique,
	}.lowerBound(ikey)
}

// rootWatch implements tableIndexTxn.
func (r partIndexTxn) rootWatch() <-chan struct{} {
	panic("unimplemented")
}

// abort implements tableIndexTxn.
func (r partIndexTxn) abort() {
	panic("unimplemented")
}

// commit implements tableIndexTxn.
func (r partIndexTxn) commit() tableIndex {
	tree := r.txn.Commit()
	return partIndex{
		tree:         tree,
		objectToKeys: r.index.objectToKeys,
		unique:       r.index.unique,
	}
}

// delete implements tableIndexTxn.
func (r partIndexTxn) delete(obj object) (old object, hadOld bool) {
	return r.txn.Delete(r.index.encodeKey(r.index.objectToKeys(obj).First()))
}

// get implements tableIndexTxn.
func (r partIndexTxn) get(key indexKey) (iobj object, watch <-chan struct{}, ok bool) {
	return partIndex{
		tree:         r.txn,
		objectToKeys: r.index.objectToKeys,
		unique:       r.index.unique,
	}.get(key)
}

// insert implements tableIndexTxn.
func (r partIndexTxn) insert(obj object) (old object, hadOld bool, watch <-chan struct{}) {
	return r.txn.InsertWatch(
		r.index.encodeKey(r.index.objectToKeys(obj).First()),
		obj)
}

// len implements tableIndexTxn.
func (r partIndexTxn) len() int {
	return r.txn.Len()
}

// modify implements tableIndexTxn.
func (r partIndexTxn) modify(obj object, mod func(old object) object) (old object, hadOld bool, watch <-chan struct{}) {
	return r.txn.ModifyWatch(
		r.index.encodeKey(r.index.objectToKeys(obj).First()),
		mod)
}

// notify implements tableIndexTxn.
func (r partIndexTxn) notify() {
	r.txn.Notify()
}

// prefix implements tableIndexTxn.
func (r partIndexTxn) prefix(ikey indexKey) (iter.Seq[object], <-chan struct{}) {
	return partIndex{
		tree:         r.txn.Clone(),
		objectToKeys: r.index.objectToKeys,
		unique:       r.index.unique,
	}.prefix(ikey)
}

func (r partIndexTxn) objectToKey(obj object) indexKey {
	return r.index.objectToKeys(obj).First()
}

// reindex implements tableIndexTxn.
func (r partIndexTxn) reindex(primaryKey indexKey, old *object, new *object) {
	idKey := primaryKey.(index.Key)
	unique := r.index.unique
	var newKeys index.KeySet
	if new != nil {
		newKeys = r.index.objectToKeys(*new)
	}

	if old != nil {
		// The old object existed, remove any obsolete keys
		r.index.objectToKeys(*old).Foreach(
			func(oldKey index.Key) {
				if !newKeys.Exists(oldKey) {
					if !unique {
						oldKey = encodeNonUniqueKey(idKey, oldKey)
					}
					_, hadOld := r.txn.Delete(oldKey)
					if !hadOld {
						panic("BUG: delete did not find old object")
					}
				}
			},
		)
	}

	if new != nil {
		newKeys.Foreach(func(newKey index.Key) {
			// Non-unique secondary indexes are formed by concatenating them
			// with the primary key.
			if !unique {
				newKey = encodeNonUniqueKey(idKey, newKey)
			}
			r.txn.Insert(newKey, *new)
		})
	}
}

// snapshot implements tableIndexTxn.
func (r partIndexTxn) snapshot() tableIndexReader {
	return partIndex{
		tree:         r.txn.Clone(),
		objectToKeys: r.index.objectToKeys,
		unique:       r.index.unique,
	}
}

var _ tableIndexTxn = partIndexTxn{}
