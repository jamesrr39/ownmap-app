package styling

import (
	"image/color"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/ownmap-app/ownmap"
	"github.com/jamesrr39/ownmap-app/ownmapdal"
)

type CustomBasicStyle struct{}

func (*CustomBasicStyle) GetBackground() color.Color {
	return color.White
}

func (*CustomBasicStyle) GetStyleID() string {
	return BUILTIN_STYLEID
}

var placeZoomLevelMap = map[string]ownmap.ZoomLevel{
	// settlements
	"hamlet":  zoomLevelHamlet,
	"village": zoomLevelVillage,
	"town":    zoomLevelTown,
	"city":    zoomLevelCity,
	// administrative areas
	"country":      zoomLevelCountry,
	"state":        zoomLevelState,
	"region":       zoomLevelRegion,
	"province":     zoomLevelProvince,
	"district":     zoomLevelDistrict,
	"county":       zoomLevelCounty,
	"municipality": zoomLevelMunicipality,
}

const (
	zoomLevelHamlet  = 15
	zoomLevelVillage = 13
	zoomLevelTown    = 11
	zoomLevelCity    = 10

	zoomLevelCountry      = 3
	zoomLevelState        = 4
	zoomLevelRegion       = 4
	zoomLevelProvince     = 5
	zoomLevelDistrict     = 5
	zoomLevelCounty       = 7
	zoomLevelMunicipality = 8

	zoomLevelOther = 16
)

func (*CustomBasicStyle) GetNodeStyle(node *ownmap.OSMNode, zoomLevel ownmap.ZoomLevel) (*NodeStyle, errorsx.Error) {
	var name string
	var isPlace bool
	for _, tag := range node.Tags {
		continueLoop := true
		switch tag.Key {
		case "place":
			// check if it is shown for this zoom level
			minZoomLevel, ok := placeZoomLevelMap[tag.Value]
			if !ok {
				minZoomLevel = zoomLevelOther
			}
			if zoomLevel < minZoomLevel {
				return nil, nil
			}

			// item should be displayed for this zoom level
			isPlace = true
			if name != "" {
				// we have both keys, so skip the rest of the tags
				continueLoop = false
			}
		case "name":
			name = tag.Value
			if isPlace {
				// we have both keys, so skip the rest of the tags
				continueLoop = false
			}
		}
		if !continueLoop {
			break
		}
	}

	if !isPlace || name == "" {
		return nil, nil
	}

	return &NodeStyle{
		TextSize:  16,
		TextColor: color.Black,
		ZIndex:    zindexPlace,
	}, nil
}

const (
	zindexForest      = 1
	zindexResidential = 2
	zindexRailway     = 3
	zindexHighway     = 4
	zindexPlace       = 5
)

var forestStyle = &WayStyle{
	FillColor: color.RGBA{172, 200, 160, 0xff},
	ZIndex:    zindexForest,
}

func (*CustomBasicStyle) GetWayStyle(tags []*ownmap.OSMTag, zoomLevel ownmap.ZoomLevel) (*WayStyle, errorsx.Error) {
	var highwayType string
	for _, tag := range tags {
		continueLoop := true
		switch tag.Key {
		case "highway":
			highwayType = tag.Value
			continueLoop = false
		case "railway":
			return &WayStyle{
				LineColor: color.RGBA{190, 190, 190, 0xff},
				LineWidth: 3,
				ZIndex:    zindexRailway,
			}, nil
		case "natural":
			switch tag.Value {
			case "wood":
				return forestStyle, nil
			}
		case "landuse":
			switch tag.Value {
			case "forest":
				return forestStyle, nil
			case "residential":
				return &WayStyle{
					FillColor: color.RGBA{223, 223, 223, 0xff},
					ZIndex:    zindexResidential,
				}, nil
			}
		}
		if !continueLoop {
			break
		}
	}

	if highwayType == "" {
		// not shown
		return nil, nil
	}

	// highway
	wayStyle := &WayStyle{
		ZIndex: zindexHighway,
	}
	switch highwayType {
	case "motorway":
		wayStyle.LineColor = color.RGBA{0xf3, 0x8d, 0x9e, 0xff}
	case "trunk":
		wayStyle.LineColor = color.RGBA{0xff, 0xae, 0x9b, 0xff}
	case "primary", "primary_link":
		wayStyle.LineColor = color.RGBA{0xff, 0xd4, 0xa5, 0xff}
	case "secondary":
		wayStyle.LineColor = color.RGBA{0xf6, 0xf9, 0xbf, 0xff}
	case "tertiary":
		wayStyle.LineColor = color.RGBA{0xf3, 0x8d, 0x9e, 0xff}
	case "unclassified", "residential", "service", "track", "living_street", "pedestrian":
		wayStyle.LineColor = color.RGBA{0xbc, 0xac, 0xa5, 0xff}
	case "footway", "path", "steps":
		wayStyle.LineColor = color.RGBA{0, 0xff, 0, 0xff}
		wayStyle.LineDashPolicy = []float64{1, 2, 3}
	case "bridleway", "cycleway":
		wayStyle.LineColor = color.RGBA{0, 0xff, 0, 0xff}
		wayStyle.LineDashPolicy = []float64{20, 5}
	default:
		return nil, errorsx.Errorf("unhandled highway type: %q", highwayType)
	}

	return wayStyle, nil
}

func (s *CustomBasicStyle) GetRelationStyle(relationData *ownmap.RelationData, zoomLevel ownmap.ZoomLevel) (*RelationStyle, errorsx.Error) {
	wayStyle, err := s.GetWayStyle(relationData.Tags, zoomLevel)
	if err != nil {
		return nil, err
	}

	if wayStyle == nil {
		return nil, nil
	}

	return &RelationStyle{ZIndex: wayStyle.ZIndex}, nil
}

func (*CustomBasicStyle) GetWantedObjects(zoomLevel ownmap.ZoomLevel) []*ownmapdal.TagKeyWithType {
	return []*ownmapdal.TagKeyWithType{
		{
			ObjectType: ownmap.ObjectTypeNode,
			TagKey:     "place",
		},
		{
			ObjectType: ownmap.ObjectTypeWay,
			TagKey:     "highway",
		},
		{
			ObjectType: ownmap.ObjectTypeWay,
			TagKey:     "waterway",
		},
		{
			ObjectType: ownmap.ObjectTypeWay,
			TagKey:     "water",
		},
		{
			ObjectType: ownmap.ObjectTypeWay,
			TagKey:     "landuse",
		},
		{
			ObjectType: ownmap.ObjectTypeWay,
			TagKey:     "natural",
		},
		{
			ObjectType: ownmap.ObjectTypeWay,
			TagKey:     "railway",
		},
		{
			ObjectType: ownmap.ObjectTypeRelation,
			TagKey:     "highway",
		},
		{
			ObjectType: ownmap.ObjectTypeRelation,
			TagKey:     "waterway",
		},
		{
			ObjectType: ownmap.ObjectTypeRelation,
			TagKey:     "water",
		},
		{
			ObjectType: ownmap.ObjectTypeRelation,
			TagKey:     "landuse",
		},
		{
			ObjectType: ownmap.ObjectTypeRelation,
			TagKey:     "natural",
		},
		{
			ObjectType: ownmap.ObjectTypeRelation,
			TagKey:     "railway",
		},
	}
}
