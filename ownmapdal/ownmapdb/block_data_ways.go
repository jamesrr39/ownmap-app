package ownmapdb

import (
	"bytes"

	"github.com/gogo/protobuf/proto"
	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/ownmap-app/ownmap"
)

type WaysFromDiskBlockData struct {
	blockData *WaysBlockData
}

func NewWaysFromDiskBlockData() *WaysFromDiskBlockData {
	return &WaysFromDiskBlockData{
		blockData: &WaysBlockData{},
	}
}

func (n *WaysFromDiskBlockData) Reset() {
	n.blockData = &WaysBlockData{}
}

func (n *WaysFromDiskBlockData) Append(data *bytes.Buffer) errorsx.Error {
	way := new(ownmap.OSMWay)
	err := proto.Unmarshal(data.Bytes(), way)
	if err != nil {
		return errorsx.Wrap(err)
	}
	n.blockData.Ways = append(n.blockData.Ways, way)
	return nil
}

func (n *WaysFromDiskBlockData) ToProtoMessage() proto.Message {
	return n.blockData
}
