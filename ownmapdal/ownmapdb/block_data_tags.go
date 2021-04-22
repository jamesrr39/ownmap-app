package ownmapdb

import (
	"bytes"

	"github.com/gogo/protobuf/proto"
	"github.com/jamesrr39/goutil/errorsx"
)

type TagsFromDiskBlockData struct {
	blockData *TagIndexBlockData
}

func NewTagsFromDiskBlockData() *TagsFromDiskBlockData {
	return &TagsFromDiskBlockData{
		blockData: &TagIndexBlockData{},
	}
}

func (n *TagsFromDiskBlockData) Reset() {
	n.blockData = &TagIndexBlockData{}
}

func (n *TagsFromDiskBlockData) Append(data *bytes.Buffer) errorsx.Error {
	tagIndexRecord := new(TagIndexRecord)
	err := proto.Unmarshal(data.Bytes(), tagIndexRecord)
	if err != nil {
		return errorsx.Wrap(err)
	}
	n.blockData.TagIndexRecords = append(n.blockData.TagIndexRecords, tagIndexRecord)
	return nil
}

func (n *TagsFromDiskBlockData) ToProtoMessage() proto.Message {
	return n.blockData
}
