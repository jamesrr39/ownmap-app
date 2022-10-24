package ownmap

import (
	fmt "fmt"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/paulmach/osm"
)

// Overlaps checks whether an item is at least partially inside a container
func Overlaps(container osm.Bounds, item osm.Bounds) bool {
	if container.MinLat > item.MaxLat {
		// container is wholly above item
		return false
	}

	if container.MaxLat < item.MinLat {
		// container is wholly below item
		return false
	}

	if container.MinLon > item.MaxLon {
		// container is wholly to the right of item
		return false
	}

	if container.MaxLon < item.MinLon {
		// container is wholly to the left of item
		return false
	}

	return true
}

func IsTotallyInside(container osm.Bounds, item osm.Bounds) bool {
	return item.MaxLat <= container.MaxLat && item.MaxLon <= container.MaxLon && item.MinLat >= container.MinLat && item.MinLon >= container.MinLon
}

func GetWholeWorldBounds() osm.Bounds {
	return osm.Bounds{
		MaxLat: 90,
		MinLat: -90,
		MaxLon: 180,
		MinLon: -180,
	}
}

// IsInBounds tests if a point is inside a container
func IsInBounds(bounds osm.Bounds, pointLat, pointLon float64) bool {
	isInLatBounds := pointLat < bounds.MaxLat && pointLat > bounds.MinLat
	if !isInLatBounds {
		return false
	}

	isInLonBounds := pointLon < bounds.MaxLon && pointLon > bounds.MinLon
	if !isInLonBounds {
		return false
	}

	return true
}

func NewMapmakerTagsFromOSMTags(osmTags osm.Tags) []*OSMTag {
	var tags []*OSMTag
	for _, tag := range osmTags {
		tags = append(tags, &OSMTag{
			Key:   tag.Key,
			Value: tag.Value,
		})
	}
	return tags
}

type TagMap map[string]string

func TagListToTagMap(tagList []*OSMTag) TagMap {
	if len(tagList) == 0 {
		return nil
	}

	m := make(map[string]string)
	for _, tagKVPair := range tagList {
		_, ok := m[tagKVPair.Key]
		if ok {
			panic(fmt.Sprintf("tag key duplicate: %q. All tags: %#v", tagKVPair.Key, tagList))
		}
		m[tagKVPair.Key] = tagKVPair.Value
	}

	return m
}

func relationMemberTypeFromOSMMemberType(memberType osm.Type) (OSMRelationMember_OSMMemberType, errorsx.Error) {
	switch memberType {
	case osm.TypeNode:
		return OSM_MEMBER_TYPE_NODE, nil
	case osm.TypeWay:
		return OSM_MEMBER_TYPE_WAY, nil
	case osm.TypeRelation:
		return OSM_MEMBER_TYPE_RELATION, nil
	default:
		return OSM_MEMBER_TYPE_UNKNOWN, errorsx.Errorf("couldn't understand OSM Relation member type: %q", memberType)
	}
}

func NewMapmakerRelationFromOSMRelation(osmRelation *osm.Relation) (*OSMRelation, errorsx.Error) {
	var members []*OSMRelationMember
	for _, member := range osmRelation.Members {
		memberType, err := relationMemberTypeFromOSMMemberType(member.Type)
		if err != nil {
			return nil, errorsx.Wrap(err)
		}

		if member.Orientation != 0 {
			return nil, errorsx.Errorf("orientation %d for relation id %d", member.Orientation, osmRelation.ID)
		}

		members = append(members, &OSMRelationMember{
			ObjectID:   member.Ref,
			MemberType: memberType,
			Role:       member.Role,
		})
	}

	return &OSMRelation{
		ID:      int64(osmRelation.ID),
		Tags:    NewMapmakerTagsFromOSMTags(osmRelation.Tags),
		Members: members,
	}, nil
}

func NewMapmakerNodeFromOSMRelation(obj *osm.Node) *OSMNode {
	return &OSMNode{
		ID:   int64(obj.ID),
		Lat:  obj.Lat,
		Lon:  obj.Lon,
		Tags: NewMapmakerTagsFromOSMTags(obj.Tags),
	}
}

func NewMapmakerWayFromOSMRelation(obj *osm.Way, wayPoints []*WayPoint) *OSMWay {
	return &OSMWay{
		ID:        int64(obj.ID),
		Tags:      NewMapmakerTagsFromOSMTags(obj.Tags),
		WayPoints: wayPoints,
	}
}
