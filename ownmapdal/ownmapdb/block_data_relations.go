package ownmapdb

import (
	"bytes"

	"github.com/gogo/protobuf/proto"
	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/ownmap-app/ownmap"
)

type RelationsFromDiskBlockData struct {
	blockData *RelationsBlockData
}

func NewRelationsFromDiskBlockData() *RelationsFromDiskBlockData {
	return &RelationsFromDiskBlockData{
		blockData: &RelationsBlockData{},
	}
}

func (n *RelationsFromDiskBlockData) Reset() {
	n.blockData = &RelationsBlockData{}
}

func (n *RelationsFromDiskBlockData) Append(data *bytes.Buffer) errorsx.Error {
	node := new(ownmap.OSMRelation)
	err := proto.Unmarshal(data.Bytes(), node)
	if err != nil {
		return errorsx.Wrap(err)
	}
	n.blockData.Relations = append(n.blockData.Relations, node)
	return nil
}

func (n *RelationsFromDiskBlockData) ToProtoMessage() proto.Message {
	return n.blockData
}
