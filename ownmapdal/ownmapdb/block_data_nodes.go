package ownmapdb

import (
	"bytes"

	"github.com/gogo/protobuf/proto"
	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/ownmap-app/ownmap"
)

type NodesFromDiskBlockData struct {
	blockData *NodesBlockData
}

func NewNodesFromDiskBlockData() *NodesFromDiskBlockData {
	return &NodesFromDiskBlockData{
		blockData: &NodesBlockData{},
	}
}

func (n *NodesFromDiskBlockData) Reset() {
	n.blockData = &NodesBlockData{}
}

func (n *NodesFromDiskBlockData) Append(data *bytes.Buffer) errorsx.Error {
	node := new(ownmap.OSMNode)
	err := proto.Unmarshal(data.Bytes(), node)
	if err != nil {
		return errorsx.Wrap(err)
	}
	n.blockData.Nodes = append(n.blockData.Nodes, node)
	return nil
}

func (n *NodesFromDiskBlockData) ToProtoMessage() proto.Message {
	return n.blockData
}
