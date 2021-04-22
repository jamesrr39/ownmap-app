package ownmapdb

import (
	"github.com/gogo/protobuf/proto"
)

type SectionType interface {
	itemsCount() int
	addToListBlockItems(indexInList int)
	getKeyBytesForItemAtIndex(indexInList int) []byte
	getItemsInBlock() proto.Message
	resetBlockData()
	tempFilePrefix() string
	sort()
}
