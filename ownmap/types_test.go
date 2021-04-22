package ownmap

import (
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_OSMNode_proto_serialization(t *testing.T) {
	node := &OSMNode{
		ID: 123,
	}

	b, err := proto.Marshal(node)
	require.Nil(t, err)

	received := new(OSMNode)
	err = proto.Unmarshal(b, received)
	require.Nil(t, err)

	assert.Equal(t, int64(123), received.ID)
}
