package ownmapdb

import (
	"bytes"
	"io"
	"path/filepath"

	"github.com/gogo/protobuf/proto"
	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/goutil/gofs"
	"github.com/jamesrr39/ownmap-app/ownmapdal/ownmapdb/diskfilemap"
)

func createSectionFromDisk(fs gofs.Fs, diskFileMap diskfilemap.OnDiskCollection, blockData BlockData, tempdirName, tempFilePrefix string, blockSize int64) (*SectionMetadata, io.ReadCloser, errorsx.Error) {
	sectionHeader := new(SectionMetadata)
	sectionDataFile, err := fs.Create(filepath.Join(tempdirName, tempFilePrefix))
	if err != nil {
		return nil, nil, errorsx.Wrap(err)
	}

	var key []byte
	var i int64
	iterator, err := diskFileMap.Iterator()
	if err != nil {
		return nil, nil, errorsx.Wrap(err)
	}

	for iterator.NextBucket() {
		kvPairs, err := iterator.GetAllFromCurrentBucketAscending()
		if err != nil {
			return nil, nil, errorsx.Wrap(err)
		}

		for _, kvPair := range kvPairs {
			// store the key outside the loop, as it might be needed for the last block metadata
			key = kvPair.Key

			err = blockData.Append(bytes.NewBuffer(kvPair.Value))
			if err != nil {
				return nil, nil, errorsx.Wrap(err)
			}

			// every "blockSize" items
			if i != 0 && (i%(blockSize-1) == 0) {
				err = writeBlock(sectionHeader, sectionDataFile, blockData.ToProtoMessage(), key)
				if err != nil {
					return nil, nil, errorsx.Wrap(err)
				}

				// reset block data
				blockData.Reset()
			}
			i++
		}
	}

	// write the rest of the data
	err = writeBlock(sectionHeader, sectionDataFile, blockData.ToProtoMessage(), key)
	if err != nil {
		return nil, nil, errorsx.Wrap(err)
	}

	for _, blockMetadata := range sectionHeader.BlockMetadatas {
		sectionHeader.TotalSize += uint64(blockMetadata.BlockSize)
	}

	_, err = sectionDataFile.Seek(0, io.SeekStart)
	if err != nil {
		return nil, nil, errorsx.Wrap(err)
	}

	return sectionHeader, sectionDataFile, nil
}

// writeBlock writes a data block and metadata to the section header
func writeBlock(sectionHeader *SectionMetadata, sectionDataFile io.Writer, data proto.Message, lastKeyInBlock []byte) errorsx.Error {
	marshalledBytes, err := proto.Marshal(data)
	if err != nil {
		return errorsx.Wrap(err)
	}

	bytesWritten, err := sectionDataFile.Write(marshalledBytes)
	if err != nil {
		return errorsx.Wrap(err)
	}

	var previousOffsetSoFar int64
	for _, blockMetadata := range sectionHeader.BlockMetadatas {
		previousOffsetSoFar += blockMetadata.BlockSize
	}

	sectionHeader.BlockMetadatas = append(sectionHeader.BlockMetadatas, &BlockMetadata{
		StartOffsetFromStartOfSectionData: previousOffsetSoFar,
		LastItemInBlockValue:              lastKeyInBlock,
		BlockSize:                         int64(bytesWritten),
	})

	return nil
}
