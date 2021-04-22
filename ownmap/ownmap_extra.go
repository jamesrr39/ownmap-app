package ownmap

import (
	"github.com/paulmach/osm"
)

func (m *DatasetInfo_Bounds) ToOSMBounds() osm.Bounds {
	return osm.Bounds{
		MinLat: m.MinLat,
		MaxLat: m.MaxLat,
		MinLon: m.MinLon,
		MaxLon: m.MaxLon,
	}
}

type OSMObject interface {
	OSMObjectID() int64
	GetTags() []*OSMTag // "GetTags" is a protoc-generated method (Tags() is not)
}

func (n *OSMNode) OSMObjectID() int64 {
	return n.ID
}

func (w *OSMWay) OSMObjectID() int64 {
	return w.ID
}

func (r *OSMRelation) OSMObjectID() int64 {
	return r.ID
}

func (w *OSMWay) GetPoints() []*Location {
	var points []*Location
	for _, waypoint := range w.WayPoints {
		points = append(points, waypoint.Point)
	}
	return points
}

func GetPointsFromNodes(nodes []*OSMNode) []*Location {
	var points []*Location
	for _, node := range nodes {
		points = append(points, &Location{Lat: node.Lat, Lon: node.Lon})
	}
	return points
}
