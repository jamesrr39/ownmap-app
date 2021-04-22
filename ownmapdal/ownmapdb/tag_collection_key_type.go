package ownmapdb

import (
	"bytes"
	"encoding/binary"
	math "math"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/ownmap-app/ownmap"
	"golang.org/x/exp/errors/fmt"
)

func bucketFromLatOrLon(value float64) int {
	return int(math.Floor(value * 100))
}

func newTagCollectionKeyFromLatLonBucket(latBucket, lonBucket int, objectType ownmap.ObjectType, tagKey string) tagCollectionKeyType {
	return tagCollectionKeyType{
		latBucket, lonBucket, objectType, tagKey,
	}
}

func newTagCollectionKey(lat, lon float64, objectType ownmap.ObjectType, tagKey string) *tagCollectionKeyType {
	latBucket := bucketFromLatOrLon(lat)
	lonBucket := bucketFromLatOrLon(lon)
	return &tagCollectionKeyType{
		latBucket, lonBucket, objectType, tagKey,
	}
}

type tagCollectionKeyType struct {
	LatBucket, LonBucket int
	ObjectType           ownmap.ObjectType
	TagKey               string
}

func signedIntTo4Bytes(val int) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, uint32(val))
	return b
}

const (
	_2pow31 = 1 << 31
	_2pow32 = 1 << 32
)

func bytesToSignedInt(b []byte) int {
	valUint32 := binary.LittleEndian.Uint32(b)
	if valUint32 > _2pow31 {
		return -1 * (_2pow32 - int(valUint32))
	}
	return int(valUint32)
}

func objectTypeToByte(objectType ownmap.ObjectType) byte {
	return byte(objectType)
}

func byteToObjectType(b byte) ownmap.ObjectType {
	return ownmap.ObjectType(b)
}

func (t *tagCollectionKeyType) MarshalKey() []byte {
	// TODO: better grouping for less seeks in the same error? composite key for lat/lon
	var b []byte
	b = append(b, signedIntTo4Bytes(t.LatBucket)...)
	b = append(b, signedIntTo4Bytes(t.LonBucket)...)
	b = append(b, objectTypeToByte(t.ObjectType))
	b = append(b, []byte(t.TagKey)...)
	return b
}

func (t *tagCollectionKeyType) UnmarshalKey(data []byte) errorsx.Error {
	t.LatBucket = bytesToSignedInt(data[0:4])
	t.LonBucket = bytesToSignedInt(data[4:8])
	t.ObjectType = byteToObjectType(data[8])
	t.TagKey = string(data[9:])

	return nil
}

// IsTagCollectionKey1BytesLargerThanKey2 is a more efficient sorting method for tagCollectionKeyTypes.
// It lazily-evaluates key bytes.
func IsTagCollectionKey1BytesLargerThanKey2(t1, t2 []byte) bool {
	t1Lat := bytesToSignedInt(t1[0:4])
	t2Lat := bytesToSignedInt(t2[0:4])

	if t1Lat > t2Lat {
		return true
	} else if t1Lat < t2Lat {
		return false
	}

	t1Lon := bytesToSignedInt(t1[4:8])
	t2Lon := bytesToSignedInt(t2[4:8])

	if t1Lon > t2Lon {
		return true
	} else if t1Lon < t2Lon {
		return false
	}

	t1ObjectTypeByte := byteToObjectType(t1[8])
	t2ObjectTypeByte := byteToObjectType(t2[8])

	if t1ObjectTypeByte > t2ObjectTypeByte {
		return true
	} else if t1ObjectTypeByte < t2ObjectTypeByte {
		return false
	}

	cmpResult := bytes.Compare(t1[8:], t2[8:])
	if cmpResult > 0 {
		return true
	}
	return false
}

func (t *tagCollectionKeyType) Compare(other KeyType) ComparisonResult {
	otherT := other.(*tagCollectionKeyType)

	// lat
	if t.LatBucket > otherT.LatBucket {
		return ComparisonResultAGreaterThanB
	}

	if t.LatBucket < otherT.LatBucket {
		return ComparisonResultALessThanB
	}

	// lon
	if t.LonBucket > otherT.LonBucket {
		return ComparisonResultAGreaterThanB
	}

	if t.LonBucket < otherT.LonBucket {
		return ComparisonResultALessThanB
	}

	// object type
	if t.ObjectType > otherT.ObjectType {
		return ComparisonResultAGreaterThanB
	}

	if t.ObjectType < otherT.ObjectType {
		return ComparisonResultALessThanB
	}

	// tag key
	if t.TagKey > otherT.TagKey {
		return ComparisonResultAGreaterThanB
	}

	if t.TagKey < otherT.TagKey {
		return ComparisonResultALessThanB
	}

	return ComparisonResultEqual
}

func (t *tagCollectionKeyType) String() string {
	return fmt.Sprintf("%d|%d|%d|%s", t.LatBucket, t.LonBucket, t.ObjectType, t.TagKey)
}

func (t *tagCollectionKeyType) LowerThanLowestValidValue() KeyType {
	// since latitude only goes to -90, setting it to -100 must be smaller than every valid value
	x := &tagCollectionKeyType{
		LatBucket: -100,
	}
	return x
}
