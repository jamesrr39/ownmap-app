package testmocks

import (
	"github.com/paulmach/osm"
	"github.com/paulmach/osm/osmpbf"
)

type MockPBFReader struct {
	ScanFunc              func() bool
	ObjectFunc            func() osm.Object
	FullyScannedBytesFunc func() int64
	ErrFunc               func() error
	ResetFunc             func()
	TotalSizeFunc         func() int64
	HeaderFunc            func() (*osmpbf.Header, error)
}

func (r *MockPBFReader) Scan() bool {
	return r.ScanFunc()
}

func (r *MockPBFReader) Object() osm.Object {
	return r.ObjectFunc()
}

func (r *MockPBFReader) FullyScannedBytes() int64 {
	return r.FullyScannedBytesFunc()
}

func (r *MockPBFReader) Err() error {
	return r.ErrFunc()
}

func (r *MockPBFReader) Reset() {
	r.ResetFunc()
}

func (r *MockPBFReader) TotalSize() int64 {
	return r.TotalSizeFunc()
}
func (r *MockPBFReader) Header() (*osmpbf.Header, error) {
	return r.HeaderFunc()
}

func NewMockPBFReaderFromObjects(objects ...osm.Object) *MockPBFReader {
	index := -1
	return &MockPBFReader{
		ScanFunc: func() bool {
			if index+1 >= len(objects) {
				return false
			}

			index++
			return true
		},
		ObjectFunc: func() osm.Object {
			return objects[index]
		},
		ErrFunc: func() error {
			return nil
		},
		ResetFunc: func() {
			index = -1
		},
		TotalSizeFunc: func() int64 {
			return 0
		},
		FullyScannedBytesFunc: func() int64 {
			return 0
		},
	}
}
