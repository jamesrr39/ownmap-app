package diskfilemap

import (
	"bytes"
	"encoding/base32"
	"errors"
	"io"
	"io/ioutil"
	"path/filepath"
	"sort"

	proto "github.com/gogo/protobuf/proto"
	"github.com/jamesrr39/goutil/algorithms"
	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/goutil/gofs"
	ownmap "github.com/jamesrr39/ownmap-app/ownmap"
)

var (
	ErrBucketNotFound = errors.New("ErrBucketNotFound")
)

type DiskCollection struct {
	fs                               gofs.Fs
	basePath                         string
	bucketFunc                       BucketPolicyFunc
	bucketSortIsKey1Larger           IsKey1LargerThanKey2Func
	bucketNames                      []BucketName
	cachedDiskFiles                  map[string]*cacheEntryType
	cacheFlushAfterXWrites           int
	cacheCurrentWritesSinceLastFlush int
}

const defaultCacheFlushAfterXWrites = 1000

type cacheEntryType struct {
	Sorted bool
	Data   *BucketData
}

func NewDiskCollection(fs gofs.Fs, basePath string, bucketFunc BucketPolicyFunc, bucketSortIsKey1Larger IsKey1LargerThanKey2Func) (*DiskCollection, errorsx.Error) {
	err := fs.MkdirAll(basePath, 0700)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	return &DiskCollection{
		fs,
		basePath,
		bucketFunc,
		bucketSortIsKey1Larger,
		nil,
		make(map[string]*cacheEntryType),
		defaultCacheFlushAfterXWrites,
		0,
	}, nil
}

func (dc *DiskCollection) getIndexOfKey(bucketData *BucketData, key []byte) (int, error) {
	var binarySearchErr errorsx.Error

	binarySearchFunc := func(i int) algorithms.SearchResult {
		thisKey := bucketData.Items[i].Key
		if bytes.Equal(thisKey, key) {
			return algorithms.SearchResultFound
		}

		thisIsLarger, err := dc.bucketSortIsKey1Larger(thisKey, key)
		if err != nil {
			binarySearchErr = err
			return algorithms.SearchResultFound
		}

		if thisIsLarger {
			// this one is greater than the searched for. So look lower
			return algorithms.SearchResultGoLower
		}

		return algorithms.SearchResultGoHigher
	}

	idx, searchResult := algorithms.BinarySearch(len(bucketData.Items), binarySearchFunc)
	if searchResult != algorithms.SearchResultFound {
		// don't wrap this with a stack trace; it's not unexpected for items to be sometimes not found.
		// adding a stacktrace would add a performance penalty for a non-exceptional event
		return 0, errorsx.ObjectNotFound
	}

	if binarySearchErr != nil {
		return 0, binarySearchErr
	}

	return idx, nil
}

func (dc *DiskCollection) Get(key []byte) ([]byte, error) {
	var err error

	bucketName, err := dc.bucketFunc(key)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	bucketData, _, err := dc.getBucketData(bucketName)
	if err != nil {
		if errorsx.Cause(err) == ErrBucketNotFound {
			return nil, errorsx.ObjectNotFound
		}
		return nil, errorsx.Wrap(err)
	}

	idx, err := dc.getIndexOfKey(bucketData, key)
	if err != nil {
		return nil, err
	}

	return bucketData.Items[idx].Value, nil
}

func (dc *DiskCollection) Set(key, value []byte) errorsx.Error {
	var err error

	bucketName, err := dc.bucketFunc(key)
	if err != nil {
		return errorsx.Wrap(err)
	}

	bucketData, bucketFromCache, err := dc.getBucketData(bucketName)
	if err != nil {
		if errorsx.Cause(err) != ErrBucketNotFound {
			return errorsx.Wrap(err, "bucketFromCache", bucketFromCache)
		}
		// create a new bucket
		dc.bucketNames = append(dc.bucketNames, bucketName)
		bucketData = new(BucketData)
	}

	// see if there is an existing one
	idx, err := dc.getIndexOfKey(bucketData, key)
	if err != nil {
		// if errorsx.ObjectNotFound, that's fine. Otherwise, it's an unexpected error
		if errorsx.Cause(err) != errorsx.ObjectNotFound {
			return errorsx.Wrap(err)
		}

		// value didn't exist before, so append to the existing Items
		bucketData.Items = append(bucketData.Items, &ownmap.KVPair{
			Key:   key,
			Value: value,
		})
	} else {
		// value already exists, so go through the list and change the one that previously existed
		bucketData.Items[idx].Value = value
	}

	dc.cacheCurrentWritesSinceLastFlush++

	pathToThisDataFile, err := dc.getPathToBucketFile(bucketName)
	if err != nil {
		return errorsx.Wrap(err)
	}

	if dc.cacheCurrentWritesSinceLastFlush >= dc.cacheFlushAfterXWrites {
		// flush all other items to disk from the cache, onto disk
		for pathToDataFile, cacheEntry := range dc.cachedDiskFiles {
			if pathToDataFile == pathToThisDataFile {
				// the one we are operating on - ignore.
				// quite often the same bucket is inserted into a number of times in a row.
				// So keep this one in memory.
				continue
			}

			if !cacheEntry.Sorted {
				err = dc.sortBucketData(cacheEntry.Data)
				if err != nil {
					return errorsx.Wrap(err)
				}
			}

			b, err := proto.Marshal(cacheEntry.Data)
			if err != nil {
				return errorsx.Wrap(err)
			}

			err = dc.fs.WriteFile(pathToDataFile, b, 0600)
			if err != nil {
				return errorsx.Wrap(err)
			}

			// remove item from cache
			delete(dc.cachedDiskFiles, pathToDataFile)
		}

		dc.cacheCurrentWritesSinceLastFlush = 0
	}

	// now add this item to the cache

	dc.cachedDiskFiles[pathToThisDataFile] = &cacheEntryType{
		Sorted: false,
		Data:   bucketData,
	}

	return nil
}

func (dc *DiskCollection) sortBucketData(bucketData *BucketData) errorsx.Error {
	var sortErr errorsx.Error

	sort.Slice(bucketData.Items, func(i, j int) bool {
		isKey1Larger, err := dc.bucketSortIsKey1Larger(
			bucketData.Items[i].Key,
			bucketData.Items[j].Key,
		)
		if err != nil {
			sortErr = err
			return false
		}

		if isKey1Larger {
			return false
		}
		return true
	})

	if sortErr != nil {
		return sortErr
	}
	return nil
}

type DiskCollectionIterator struct {
	currentBucketIndex int
	sortedBucketNames  []BucketName
	diskCollection     *DiskCollection
}

func (dc *DiskCollection) Iterator() (Iterator, errorsx.Error) {
	// copy
	sortedBucketNames := dc.bucketNames[:]

	// sort new list
	var sortErr errorsx.Error
	sort.Slice(sortedBucketNames, func(i, j int) bool {
		isKey1Larger, err := dc.bucketSortIsKey1Larger(sortedBucketNames[i], sortedBucketNames[j])
		if err != nil {
			sortErr = err
			return false
		}

		if isKey1Larger {
			return false
		}

		return true
	})
	if sortErr != nil {
		return nil, sortErr
	}

	return &DiskCollectionIterator{
		currentBucketIndex: -1,
		sortedBucketNames:  sortedBucketNames,
		diskCollection:     dc,
	}, nil
}

func (dci *DiskCollectionIterator) NextBucket() bool {
	if dci.currentBucketIndex == (len(dci.sortedBucketNames) - 1) {
		return false
	}

	dci.currentBucketIndex++
	return true
}
func (dci *DiskCollectionIterator) GetAllFromCurrentBucketAscending() ([]*ownmap.KVPair, errorsx.Error) {
	bucketName := dci.sortedBucketNames[dci.currentBucketIndex]

	bucketData, _, err := dci.diskCollection.getBucketData(bucketName)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	kvPairs := bucketData.Items

	var sortErr errorsx.Error
	sort.Slice(kvPairs, func(i, j int) bool {
		isKey1Larger, err := dci.diskCollection.bucketSortIsKey1Larger(kvPairs[i].Key, kvPairs[j].Key)
		if err != nil {
			sortErr = err
			return false
		}

		if isKey1Larger {
			return false
		}

		return true
	})
	if sortErr != nil {
		return nil, sortErr
	}

	return kvPairs, nil
}

func (dc *DiskCollection) getPathToBucketFile(bucketName BucketName) (string, errorsx.Error) {
	bb := bytes.NewBuffer(nil)
	encoder := base32.NewEncoder(base32.StdEncoding, bb)
	_, err := io.Copy(encoder, bytes.NewBuffer(bucketName))
	if err != nil {
		return "", errorsx.Wrap(err)
	}

	err = encoder.Close()
	if err != nil {
		return "", errorsx.Wrap(err)
	}

	return filepath.Join(dc.basePath, bb.String()), nil
}

// data, from cache, err
func (dc *DiskCollection) getBucketData(bucketName BucketName) (*BucketData, bool, error) {
	var err error

	bucketFilePath, err := dc.getPathToBucketFile(bucketName)
	if err != nil {
		return nil, false, errorsx.Wrap(err)
	}

	// first, try from cache
	cacheEntry, ok := dc.cachedDiskFiles[bucketFilePath]
	if ok {
		// cache hit! Now sort items if necessary and return data
		if !cacheEntry.Sorted {
			err = dc.sortBucketData(cacheEntry.Data)
			if err != nil {
				return nil, false, errorsx.Wrap(err)
			}
			cacheEntry.Sorted = true
		}

		return cacheEntry.Data, true, nil
	}

	bucketFile, err := dc.fs.Open(bucketFilePath)
	if err != nil {
		return nil, false, ErrBucketNotFound
	}
	defer bucketFile.Close()

	b, err := ioutil.ReadAll(bucketFile)
	if err != nil {
		return nil, false, errorsx.Wrap(err)
	}

	bucketData := new(BucketData)
	err = proto.Unmarshal(b, bucketData)
	if err != nil {
		return nil, false, errorsx.Wrap(err)
	}

	return bucketData, false, nil
}
