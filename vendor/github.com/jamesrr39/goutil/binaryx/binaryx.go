package binaryx

import (
	"encoding/binary"
)

func LittleEndianPutUint64(val uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, val)
	return b
}
