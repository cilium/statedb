package statedb

import (
	"github.com/cilium/statedb/index"
	"github.com/cilium/statedb/part"
)

func newRevisionIndex() tableIndex {
	return partIndex{
		tree: part.New[object](part.RootOnlyWatch),
		objectToKeys: func(obj object) index.KeySet {
			return index.NewKeySet(index.Uint64(obj.revision))
		},
		unique: true,
	}
}

func newGraveyardIndex(primaryIndex tableIndex) tableIndex {
	return partIndex{
		tree: part.New[object](part.RootOnlyWatch),
		objectToKeys: func(obj object) index.KeySet {
			// FIXME need to find a more efficient way to do this to avoid double-creating
			// the key.
			return index.NewKeySet(primaryIndex.objectToKey(obj).(index.Key))
		},
		unique: true,
	}
}
