package binaryx

import (
	"encoding/binary"
)

func LittleEndianPutUint64(val uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, val)
	return b
}

func BigEndianPutUint64(val uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, val)
	return b
}

func FlipEndiannessInPlace(s []byte) {
	sLen := len(s)
	iterations := sLen / 2 // round down automatically with int if old number
	for i := 0; i < iterations; i++ {
		highIdx := sLen - (i + 1)
		s[i], s[highIdx] = s[highIdx], s[i]
	}
}

func FlipEndiannessInNewSlice(old []byte) []byte {
	oldLen := len(old)
	newSlice := make([]byte, oldLen)
	for i := 0; i < oldLen; i++ {
		newSlice[i] = old[oldLen-(i+1)]
	}
	return newSlice
}
