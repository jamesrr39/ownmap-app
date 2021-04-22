package ownmap

import "github.com/paulmach/osm"

type MapData struct {
	Nodes []*osm.Node
}

type TagKey string
