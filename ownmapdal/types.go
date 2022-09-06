package ownmapdal

import (
	"errors"
	"strings"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/ownmap-app/ownmap"
)

var (
	ErrNoDataAvailable = errors.New("no data available")
)

type TagNodeMap map[ownmap.TagKey][]*ownmap.OSMNode

type TagRelationMap map[ownmap.TagKey][]*ownmap.RelationData

type TagWayMap map[ownmap.TagKey][]*ownmap.OSMWay

type NodeOccurenceMap map[ownmap.TagKey]*ownmap.IndexIDList
type WayOccurenceMap map[ownmap.TagKey]*ownmap.IndexIDList

type TagKeyWithType struct {
	ObjectType ownmap.ObjectType
	TagKey     ownmap.TagKey
}

type GetInBoundsFilter struct {
	Objects []*TagKeyWithType
}

// returns true if the object should be fetched
func (f *GetInBoundsFilter) Filter(objectType ownmap.ObjectType, tagKey ownmap.TagKey) bool {
	for _, objectInFilter := range f.Objects {
		if objectInFilter.ObjectType == objectType && objectInFilter.TagKey == tagKey {
			return true
		}
	}
	return false
}

type DBFileType string

const (
	DBFileTypeMapmakerDB DBFileType = "ownmapdb"
	DBFileTypePostgresql DBFileType = "postgresql"
	DBFileTypeParquet    DBFileType = "parquet"
)

type DBFileConnectionURL struct {
	Type           DBFileType
	ConnectionPath string
}

const ConnectionPathSeparator = "://"

func ParseDBConnFilePath(str string) (DBFileConnectionURL, errorsx.Error) {
	idx := strings.Index(str, ConnectionPathSeparator)
	if idx < 0 {
		return DBFileConnectionURL{}, errorsx.Errorf("couldn't find connection path separator %q in DB file path", ConnectionPathSeparator)
	}

	return DBFileConnectionURL{
		Type:           DBFileType(str[:idx]),
		ConnectionPath: str[idx+len(ConnectionPathSeparator):],
	}, nil
}
