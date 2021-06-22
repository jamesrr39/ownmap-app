package mapboxglstyle

import (
	"log"

	"github.com/jamesrr39/ownmap-app/ownmap"
)

type TagWithObjectTypes struct {
	Tag         *ownmap.OSMTag
	ObjectTypes []ownmap.ObjectType
}

func getPossibleObjectTypesForOSMTags(tag *ownmap.OSMTag) []ownmap.ObjectType {
	// TODO
	return []ownmap.ObjectType{
		ownmap.ObjectTypeNode,
		ownmap.ObjectTypeWay,
		ownmap.ObjectTypeRelation,
	}
}

func mapMapboxGLClassToOSMTags(className, sourceLayer string) []*ownmap.OSMTag {
	switch sourceLayer {
	case "landuse", "landcover":
		// according to the docs, "landuse" should be used. However some mapbox styles use "landcover"
		// https://docs.mapbox.com/vector-tiles/reference/mapbox-streets-v8/
		switch className {
		case "agriculture":
			return []*ownmap.OSMTag{
				{Key: "landuse", Value: "farmland"},
				{Key: "landuse", Value: "meadow"},
				{Key: "landuse", Value: "orchard"},
				{Key: "landuse", Value: "agriculture"}, // deprecated by OSM, still may be usages of it though.
			}
		case "grass":
			// TODO: a wider range of values? https://wiki.openstreetmap.org/wiki/Tag:landuse%3Dgrass
			return []*ownmap.OSMTag{
				{Key: "landuse", Value: "grass"},
			}
		case "wood":
			return []*ownmap.OSMTag{
				{Key: "natural", Value: "wood"},
				{Key: "landuse", Value: "forest"},
				{Key: "landcover", Value: "trees"},
			}
		case "sand":
			return []*ownmap.OSMTag{
				{Key: "natural", Value: "sand"},
			}
		case "national_park":
			// TODO: not sure about this
			return []*ownmap.OSMTag{
				{Key: "boundary", Value: "national_park"},
			}
		default:
			log.Printf("unknown className: %q, sourcelayer: %q", className, sourceLayer)
			return nil
		}
	case "transportation":
		switch className {
		case "pier":
			return []*ownmap.OSMTag{
				{Key: "man_made", Value: "pier"},
			}
		case "path":
			return []*ownmap.OSMTag{
				{Key: "highway", Value: "path"},
				{Key: "highway", Value: "footway"},
			}
		case "track":
			return []*ownmap.OSMTag{
				{Key: "highway", Value: "track"},
				{Key: "leisure", Value: "track"},
				{Key: "cycleway", Value: "track"},
			}
		case "minor", "minor_road":
			return []*ownmap.OSMTag{
				{Key: "highway", Value: "unclassified"},
				{Key: "highway", Value: "residential"},
			}
		case "aeroway":
			return []*ownmap.OSMTag{
				{Key: "aeroway", Value: "*"},
			}
		case "trunk", "primary", "service", "secondary", "tertiary", "motorway":
			return []*ownmap.OSMTag{
				{Key: "highway", Value: className},
			}
		case "rail":
			return []*ownmap.OSMTag{
				{Key: "railway", Value: "rail"},
			}
		case "transit":
			return []*ownmap.OSMTag{
				{Key: "railway", Value: "*"},
				{Key: "landuse", Value: "railway"},
			}
		default:
			// TODO error instead of exit
			log.Fatalf("unknown className: %q, sourcelayer: %q\n", className, sourceLayer)
			return nil
		}
	case "aeroway", "airport_label", "housenum_label":
		// OpenStreetMap replication
		return []*ownmap.OSMTag{
			{Key: sourceLayer, Value: className},
		}
	case "boundary":
		// TODO
		log.Printf("unhandled className: %q, sourceLayer: %q\n", className, sourceLayer)
		return nil
	case "place":
		return []*ownmap.OSMTag{
			{Key: sourceLayer, Value: className},
		}
	default:
		log.Fatalf("unknown sourcelayer: %q (className: %q)\n", sourceLayer, className)
		return nil
	}
}

func mapMapboxGLSubclassToOSMTags(subclassName, sourceLayer string) []*ownmap.OSMTag {
	switch subclassName {
	case "ice_shelf":
		return []*ownmap.OSMTag{
			{Key: "glacier:type", Value: "shelf"},
		}
	case "glacier":
		return []*ownmap.OSMTag{
			{Key: "natural", Value: "glacier"},
		}
	default:
		panic("unknown subclassName: " + subclassName)
	}
}

func areTagsInSourceLayer(sourceLayer string, tags []*ownmap.OSMTag) bool {
	for _, tag := range tags {
		switch sourceLayer {
		case "transportation":
			switch tag.Key {
			case "highway", "railway":
				return true
			}
		case "landcover":
			if tag.Key == "landcover" || tag.Key == "landuse" {
				return true
			}
		default:
			if tag.Key == sourceLayer {
				return true
			}
		}
	}
	return false
}
