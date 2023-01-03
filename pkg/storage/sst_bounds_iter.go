// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import "github.com/cockroachdb/cockroach/pkg/roachpb"

type rangeKeyBoundType int

const (
	boundTypeStart rangeKeyBoundType = iota
	boundTypeEnd
)

type rangeKeyBound struct {
	key       roachpb.Key
	rangeKey  MVCCRangeKeyStack
	boundType rangeKeyBoundType
}

type rangeKeyBoundIterator struct {
	iter MVCCIterator
	pos  int
}

func (r *rangeKeyBoundIterator) SeekGE(key MVCCKey) {
	r.iter.SeekLT(key)
	if ok, _ := r.iter.Valid(); ok {
		r.iter.RangeKeys()
	}
}

func (r *rangeKeyBoundIterator) Next() {
	switch r.pos {
	case 0:
		r.pos++
	case 1:
		r.iter.Next()
		r.pos = 0
	default:
		panic("unreachable")
	}
}

func (r *rangeKeyBoundIterator) Key() rangeKeyBound {
	if ok, _ := r.Valid(); !ok {
		return rangeKeyBound{}
	}

	rk := r.iter.RangeKeys()
	switch r.pos {
	case 0:
		return rangeKeyBound{
			key:       rk.Bounds.Key,
			rangeKey:  rk,
			boundType: boundTypeStart,
		}
	case 1:
		return rangeKeyBound{
			key:       rk.Bounds.EndKey,
			rangeKey:  rk,
			boundType: boundTypeEnd,
		}
	default:
		panic("unreachable")
	}
}

func (r *rangeKeyBoundIterator) RangeKeys() MVCCRangeKeyStack {
	return r.iter.RangeKeys()
}

func (r *rangeKeyBoundIterator) Valid() (bool, error) {
	return r.iter.Valid()
}

func (r *rangeKeyBoundIterator) Close() {
	r.iter.Close()
}
