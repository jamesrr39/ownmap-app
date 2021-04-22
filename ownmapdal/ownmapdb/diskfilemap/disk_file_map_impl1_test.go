package diskfilemap

import (
	"encoding/binary"
	"testing"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/goutil/gofs/mockfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDiskCollection_Get(t *testing.T) {
	bucketFunc := func(key []byte) (BucketName, errorsx.Error) {
		keyAsUint := binary.LittleEndian.Uint64(key)

		// bucket per 1000 items
		bucketVal := (keyAsUint / 1000) * 1000
		bucketName := make([]byte, 8)
		binary.LittleEndian.PutUint64(bucketName, bucketVal)
		return bucketName, nil
	}
	isKey1LargerFunc := func(key1, key2 []byte) (bool, errorsx.Error) {
		val1 := binary.LittleEndian.Uint64(key1)
		val2 := binary.LittleEndian.Uint64(key2)
		return val1 > val2, nil
	}

	t.Run("1 item test", func(t *testing.T) {
		var err error

		collection, err := NewDiskCollection(mockfs.NewMockFs(), "/tmp/", bucketFunc, isKey1LargerFunc)
		require.NoError(t, err)

		// put in key1
		key1 := make([]byte, 8)
		binary.LittleEndian.PutUint64(key1, 222333)

		val1 := []byte("test string")
		err = collection.Set(key1, val1)
		require.NoError(t, err)

		receivedVal1, err := collection.Get(key1)
		require.NoError(t, err)

		assert.Equal(t, val1, receivedVal1)
	})

	t.Run("2 items in different buckets", func(t *testing.T) {
		var err error

		collection, err := NewDiskCollection(mockfs.NewMockFs(), "/tmp/", bucketFunc, isKey1LargerFunc)
		require.NoError(t, err)

		// key 1
		key1 := make([]byte, 8)
		binary.LittleEndian.PutUint64(key1, 1)

		val1 := []byte("test string 1")
		err = collection.Set(key1, val1)
		require.NoError(t, err)

		// key 2
		key2 := make([]byte, 8)
		binary.LittleEndian.PutUint64(key2, 1001)

		val2 := []byte("test string 2")
		err = collection.Set(key2, val2)
		require.NoError(t, err)

		// now get both (1st from disk, 2nd from cache)
		receivedVal1, err := collection.Get(key1)
		require.NoError(t, err)
		assert.Equal(t, val1, receivedVal1)

		receivedVal2, err := collection.Get(key2)
		require.NoError(t, err)
		assert.Equal(t, val2, receivedVal2)
	})
}
func Test_Set(t *testing.T) {
	bucketFunc := func(key []byte) (BucketName, errorsx.Error) {
		keyAsUint := binary.LittleEndian.Uint64(key)

		// bucket per 1000 items
		bucketVal := (keyAsUint / 1000) * 1000
		bucketName := make([]byte, 8)
		binary.LittleEndian.PutUint64(bucketName, bucketVal)
		return bucketName, nil
	}
	isKey1LargerFunc := func(key1, key2 []byte) (bool, errorsx.Error) {
		val1 := binary.LittleEndian.Uint64(key1)
		val2 := binary.LittleEndian.Uint64(key2)
		return val1 > val2, nil
	}
	t.Run("overwrite", func(t *testing.T) {
		var err error

		collection, err := NewDiskCollection(mockfs.NewMockFs(), "/tmp/", bucketFunc, isKey1LargerFunc)
		require.NoError(t, err)

		// key 1
		key1 := make([]byte, 8)
		binary.LittleEndian.PutUint64(key1, 1)

		val1 := []byte("test string 1")
		err = collection.Set(key1, val1)
		require.NoError(t, err)

		// key 2
		val2 := make([]byte, 8)
		binary.LittleEndian.PutUint64(val2, 1001)
		err = collection.Set(key1, val2)
		require.NoError(t, err)

		fetched, err := collection.Get(key1)
		require.NoError(t, err)

		assert.Equal(t, val2, fetched)
	})
}
