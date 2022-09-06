package ownmapdal

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/jamesrr39/goutil/dirtraversal"
	"github.com/jamesrr39/goutil/errorsx"
)

const (
	PBFFileSuffix = ".pbf"
)

type ImportStatus int

const (
	ImportStatusQueued     ImportStatus = 1
	ImportStatusInProgress ImportStatus = 2
	ImportStatusDone       ImportStatus = 3
)

var importStatusNames = []string{
	"",
	"Queued",
	"In Progress",
	"Done",
}

func (i ImportStatus) String() string {
	return importStatusNames[i]
}

type OnImportedSuccessfullyFunc func(dataSource DataSourceConn)

type ProcessImportFunc func(pbfReader, auxPbfReader PBFReader) (DataSourceConn, errorsx.Error)

type ImportQueueItem struct {
	datasource      *DataSourceConn
	RawDataFilePath string
	Status          ImportStatus
	ProgressPercent float64
	TimeInProgress  time.Duration
	processFunc     ProcessImportFunc
}

type ImportQueue struct {
	items       []*ImportQueueItem
	mu          *sync.RWMutex
	pathsConfig *PathsConfig
}

func NewImportQueue(pathsConfig *PathsConfig) *ImportQueue {
	return &ImportQueue{[]*ImportQueueItem{}, new(sync.RWMutex), pathsConfig}
}

func (q *ImportQueue) GetItems() []*ImportQueueItem {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.items
}

func (q *ImportQueue) AddItemToQueue(rawData io.Reader, fileName string, processFunc ProcessImportFunc, onImportedSuccessfully OnImportedSuccessfullyFunc) errorsx.Error {
	var err error

	tryingToGoUp := dirtraversal.IsTryingToTraverseUp(fileName)
	if tryingToGoUp {
		return errorsx.Errorf("not allowed to traverse up with filename %q", fileName)
	}

	rawDataFilePath, err := GenerateFilePathForNewDiskFile(q.pathsConfig.RawDataFilesDir, fileName, PBFFileSuffix)
	if err != nil {
		return errorsx.Wrap(err)
	}

	f, err := os.Create(rawDataFilePath)
	if err != nil {
		return errorsx.Wrap(err)
	}
	defer f.Close()

	_, err = io.Copy(f, rawData)
	if err != nil {
		return errorsx.Wrap(err)
	}

	item := &ImportQueueItem{
		RawDataFilePath: rawDataFilePath,
		Status:          ImportStatusQueued,
		ProgressPercent: 0,
		TimeInProgress:  0,
		processFunc:     processFunc,
	}

	q.mu.Lock()
	q.items = append(q.items, item)
	q.mu.Unlock()

	nextItem := q.getNextDatasourceToProcess()
	if nextItem != nil {
		go func() {
			dataSource, err := q.importQueueItem(nextItem)
			if err != nil {
				log.Printf(
					"ERROR: failed to import queue item. Raw Data file: %q.\nError: %q\nStack: %s\n",
					nextItem.RawDataFilePath, err.Error(), err.Stack())
				return
			}
			onImportedSuccessfully(dataSource)
		}()
	}
	return nil
}

func (q *ImportQueue) getNextDatasourceToProcess() *ImportQueueItem {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, item := range q.items {
		if item.Status == ImportStatusInProgress {
			// there is already an import in progress. Wait.
			return nil
		}
	}

	// no imports in progress. Now go through the list and pick the fist one to move to in progress
	for _, item := range q.items {
		if item.Status == ImportStatusQueued {
			return item
		}
	}

	// all imports are finished
	return nil
}

func (q *ImportQueue) importQueueItem(item *ImportQueueItem) (DataSourceConn, errorsx.Error) {
	var err error

	rawDataFile, err := os.Open(item.RawDataFilePath)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}
	defer rawDataFile.Close()

	pbfReader, err := NewDefaultPBFReader(rawDataFile)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	auxRawDataFile, err := os.Open(item.RawDataFilePath)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}
	defer rawDataFile.Close()

	auxPbfReader, err := NewDefaultPBFReader(auxRawDataFile)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	item.Status = ImportStatusInProgress
	startTime := time.Now()

	go func() {
		totalSize := pbfReader.TotalSize()
		for {
			time.Sleep(2 * time.Second)

			item.TimeInProgress = time.Now().Sub(startTime)
			if item.Status == ImportStatusDone {
				item.ProgressPercent = 100
				return
			}

			progressPercent := float64(pbfReader.FullyScannedBytes()) * 100 / float64(totalSize)
			item.ProgressPercent = progressPercent
		}
	}()

	dataSource, err := item.processFunc(pbfReader, auxPbfReader)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	item.Status = ImportStatusDone

	return dataSource, nil
}

func GenerateFilePathForNewDiskFile(dirPath, fileName, suffix string) (string, errorsx.Error) {
	var err error
	for i := 0; i < 1000000; i++ {
		var id string
		if i != 0 {
			id = fmt.Sprintf("_%d", i)
		}

		fileName := fmt.Sprintf("%s%s%s", fileName, id, suffix)
		filePath := filepath.Join(dirPath, fileName)

		_, err = os.Stat(filePath)
		if err != nil {
			if !os.IsNotExist(err) {
				return "", errorsx.Wrap(err)
			}
		}

		if err == nil {
			// file already exists
			continue
		}

		return filePath, nil
	}

	return "", errorsx.Errorf("ran out of numbers for suffix")
}
