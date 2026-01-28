// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package statedb

func collectTableIndexObjs[Obj any](iter tableIndexIterator) []Obj {
	var objs []Obj
	iter.All(func(_ []byte, obj object) bool {
		objs = append(objs, obj.data.(Obj))
		return true
	})
	return objs
}
