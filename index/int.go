// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package index

import (
	"encoding/binary"
	"strconv"
)

// The indexing functions on integers should use big-endian encoding.
// This allows prefix searching on integers as the most significant
// byte is first.
// For example to find 16-bit key larger than 260 (0x0104) from 3 (0x0003)
// and 270 (0x0109)
//   00 (3) < 01 (260) => skip,
//   01 (270) >= 01 (260) => 09 > 04 => found!

func Int(n int) Key {
	return Int32(int32(n))
}

func IntString(s string) (Key, error) {
	return Int32String(s)
}

func Int64(n int64) Key {
	return Uint64(uint64(n))
}

func Int64String(s string) (Key, error) {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return Key{}, err
	}
	return Uint64(uint64(n)), nil
}

func Int32(n int32) Key {
	return Uint32(uint32(n))
}

func Int32String(s string) (Key, error) {
	n, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return Key{}, err
	}
	return Uint32(uint32(n)), nil
}

func Int16(n int16) Key {
	return Uint16(uint16(n))
}

func Int16String(s string) (Key, error) {
	n, err := strconv.ParseInt(s, 10, 16)
	if err != nil {
		return Key{}, err
	}
	return Uint16(uint16(n)), nil
}

func Uint64(n uint64) Key {
	return binary.BigEndian.AppendUint64(nil, n)
}

func Uint64String(s string) (Key, error) {
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return Key{}, err
	}
	return Uint64(n), nil
}

func Uint32(n uint32) Key {
	return binary.BigEndian.AppendUint32(nil, n)
}

func Uint32String(s string) (Key, error) {
	n, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		return Key{}, err
	}
	return Uint32(uint32(n)), nil
}

func Uint16(n uint16) Key {
	return binary.BigEndian.AppendUint16(nil, n)
}

func Uint16String(s string) (Key, error) {
	n, err := strconv.ParseUint(s, 10, 16)
	if err != nil {
		return Key{}, err
	}
	return Uint16(uint16(n)), nil
}
