package ownmapdb

import (
	"bytes"

	"github.com/gogo/protobuf/proto"
	"github.com/jamesrr39/goutil/errorsx"
)

type BlockData interface {
	Reset()
	Append(data *bytes.Buffer) errorsx.Error
	ToProtoMessage() proto.Message
}
