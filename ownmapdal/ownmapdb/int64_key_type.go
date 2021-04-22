package ownmapdb

import (
	"encoding/binary"
	reflect "reflect"

	"github.com/jamesrr39/goutil/errorsx"
	"golang.org/x/exp/errors/fmt"
)

type int64ItemType int64

func (t *int64ItemType) MarshalKey() []byte {
	bb := make([]byte, 8)
	binary.LittleEndian.PutUint64(bb, uint64(*t))

	return bb
}

func (t *int64ItemType) UnmarshalKey(data []byte) errorsx.Error {
	if reflect.ValueOf(t).Kind() != reflect.Ptr {
		return errorsx.Errorf("type must be a pointer")
	}
	num := binary.LittleEndian.Uint64(data)
	v := int64ItemType(num)
	*t = v

	return nil
}

func (t *int64ItemType) Compare(other KeyType) ComparisonResult {
	thisInt64 := *t
	otherNumPtr := other.(*int64ItemType)
	otherNum := *otherNumPtr

	if thisInt64 == otherNum {
		return ComparisonResultEqual
	}

	if thisInt64 > otherNum {
		return ComparisonResultAGreaterThanB
	}

	return ComparisonResultALessThanB
}

func (t *int64ItemType) String() string {
	return fmt.Sprintf("%d", *t)
}

func (t *int64ItemType) LowerThanLowestValidValue() KeyType {
	return new(int64ItemType)
}
