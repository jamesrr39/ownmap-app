package ownmap

type ObjectType int

const (
	ObjectTypeUnknown  ObjectType = 0
	ObjectTypeNode     ObjectType = 1
	ObjectTypeWay      ObjectType = 2
	ObjectTypeRelation ObjectType = 3
)

type ZoomLevel float64

const (
	MinZoomLevel ZoomLevel = 0
	MaxZoomLevel ZoomLevel = 255
)

type RelationMemberData struct {
	Role   string    // outer, inner ?
	Object OSMObject // *ownmap.OSMNode, *ownmap.OSMWay, *ownmap.OSMRelation
}

type RelationData struct {
	RelationID int64
	Tags       []*OSMTag
	Members    []*RelationMemberData
}

func (rd *RelationData) OSMObjectID() int64 {
	return rd.RelationID
}

func (rd *RelationData) GetTags() []*OSMTag {
	return rd.Tags
}
