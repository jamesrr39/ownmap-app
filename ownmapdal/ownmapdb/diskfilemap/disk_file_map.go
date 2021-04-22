package diskfilemap

import (
	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/ownmap-app/ownmap"
)

//go:generate go-mockgen-tool --type OnDiskCollection
type OnDiskCollection interface {
	Get(key []byte) (value []byte, err error) // errorsx.ObjectNotFound if not found
	Set(key, value []byte) errorsx.Error
	Iterator() (Iterator, errorsx.Error)
}

type BucketName []byte

type Iterator interface {
	NextBucket() bool
	GetAllFromCurrentBucketAscending() ([]*ownmap.KVPair, errorsx.Error)
}

type BucketPolicyFunc func(key []byte) (BucketName, errorsx.Error)

type IsKey1LargerThanKey2Func func(key1, key2 []byte) (bool, errorsx.Error)
