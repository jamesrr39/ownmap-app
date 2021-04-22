package mapboxglstyle

import (
	"log"

	"github.com/jamesrr39/ownmap-app/ownmap"
)

const (
	FilterOperatorEquals   = "=="
	FilterOperatorNotEqual = "!="
	FilterOperatorAny      = "any"
	FilterOperatorAll      = "all"
	FilterOperatorIn       = "in"
	FilterOperatorNotIn    = "!in"
)

const (
	FilterThingType           = "$type"
	FilterThingTypePoint      = "Point"
	FilterThingTypeLineString = "LineString"
	FilterThingTypePolygon    = "Polygon"

	FilterThingClass    = "class"
	FilterThingSubclass = "subclass"
)

/*

    "filter": [
        "all",
        ["==", "$type", "Polygon"],
		["in", "class", "residential", "suburb", "neighbourhood"]
	]

	"filter": ["==", "$type", "Point"],

	"filter": ["all",["==","$type","Polygon"],["in","class","residential","suburb","neighbourhood"]]
*/

// https://openmaptiles.org/schema/#transportation
const (
	SourceLayerTransportation string = "transportation"
	SourceLayerWaterway       string = "waterway"
	SourceLayerPlace          string = "place"
)

type Filter interface{}

func isSubclassShown(subclass, sourceLayer string, tags []*ownmap.OSMTag) bool {
	return isClassTypeShown(subclass, sourceLayer, tags, mapMapboxGLSubclassToOSMTags)
}

func isClassShown(className, sourceLayer string, tags []*ownmap.OSMTag) bool {
	return isClassTypeShown(className, sourceLayer, tags, mapMapboxGLClassToOSMTags)
}

// https://docs.mapbox.com/vector-tiles/reference/mapbox-streets-v8/
func isClassTypeShown(
	className,
	sourceLayer string,
	objectTags []*ownmap.OSMTag,
	mapperFunc func(className, sourceLayer string) []*ownmap.OSMTag,
) bool {
	lookingForOsmTags := mapperFunc(className, sourceLayer)
	for _, needleTag := range lookingForOsmTags {
		for _, objectTag := range objectTags {
			if objectTag.Key == needleTag.Key {
				if needleTag.Value == "*" || needleTag.Value == objectTag.Value {
					return true
				}
			}
		}
	}
	return false
}

func isIn(base []interface{}, sourceLayer string, tags []*ownmap.OSMTag, objectFilterType string) bool {
	thing := base[1].(string)
	switch thing {
	case FilterThingType:
		return base[2].(string) == objectFilterType
	case FilterThingClass:
		for _, class := range base[2:] {
			shown := isClassShown(class.(string), sourceLayer, tags)
			if shown {
				return true
			}
		}
		return false
	case FilterThingSubclass:
		for _, class := range base[2:] {
			shown := isSubclassShown(class.(string), sourceLayer, tags)
			if shown {
				return true
			}
		}
		return false
	case "brunnel", "intermittent", "admin_level":
		// TODO how to map this to OSM?
		return false
	default:
		log.Fatalf("unknown thing: %q. Sourcelayer: %q\n", thing, sourceLayer)
		return false
	}
}

func isObjectShown(filter Filter, sourceLayer string, tags []*ownmap.OSMTag, objectFilterType string) bool {
	if filter == nil {
		return true
	}

	base, ok := filter.([]interface{})
	if !ok {
		panic("unknown filter")
	}

	operator := base[0].(string)
	switch operator {
	case FilterOperatorEquals:
		if len(base) != 3 {
			panic("base len not 3")
		}
		return isIn(base, sourceLayer, tags, objectFilterType)
	case FilterOperatorNotEqual:
		if len(base) != 3 {
			panic("base len not 3")
		}
		return !isIn(base, sourceLayer, tags, objectFilterType)
	case FilterOperatorIn:
		return isIn(base, sourceLayer, tags, objectFilterType)
	case FilterOperatorNotIn:
		return !isIn(base, sourceLayer, tags, objectFilterType)
	case FilterOperatorAny:
		for _, subFilterComponent := range base[1:] {
			shown := isObjectShown(subFilterComponent, sourceLayer, tags, objectFilterType)
			if shown {
				return true
			}
		}
		return false
	case FilterOperatorAll:
		for _, subFilterComponent := range base[1:] {
			shown := isObjectShown(subFilterComponent, sourceLayer, tags, objectFilterType)
			if !shown {
				return false
			}
		}
		return true
	case "<=", "has":
		// TODO
		log.Printf("TODO: operator not implemented: %q\n", operator)
		return false
	default:
		log.Fatalf("not implemented: %q\n", operator)
	}

	panic("hit unexpected point")
}
