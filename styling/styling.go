package styling

import (
	"image/color"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/ownmap-app/ownmap"
)

const BUILTIN_STYLEID = "__ownmap_builtin"

type ItemStyle interface {
	GetZIndex() int
}

type WayStyle struct {
	FillColor      color.Color
	LineColor      color.Color
	LineDashPolicy []float64
	LineWidth      float64
	ZIndex         int
}

func (ws *WayStyle) GetZIndex() int {
	return ws.ZIndex
}

type NodeStyle struct {
	TextSize  int
	TextColor color.Color
	ZIndex    int
}

func (ns *NodeStyle) GetZIndex() int {
	return ns.ZIndex
}

type RelationStyle struct {
	ZIndex int
}

func (rs *RelationStyle) GetZIndex() int {
	return rs.ZIndex
}

type Style interface {
	GetNodeStyle(node *ownmap.OSMNode, zoomLevel ownmap.ZoomLevel) (*NodeStyle, errorsx.Error)
	GetWayStyle(tags []*ownmap.OSMTag, zoomLevel ownmap.ZoomLevel) (*WayStyle, errorsx.Error)
	GetRelationStyle(relationData *ownmap.RelationData, zoomLevel ownmap.ZoomLevel) (*RelationStyle, errorsx.Error)
	GetBackground() color.Color
	GetStyleID() string
}

type StyleSet struct {
	stylesMap      map[string]Style // map[Style ID]Style
	defaultStyleID string
}

func NewStyleSet(styles []Style, defaultStyleID string) (*StyleSet, errorsx.Error) {
	styleSet := &StyleSet{
		stylesMap:      make(map[string]Style),
		defaultStyleID: defaultStyleID,
	}

	defaultIDFound := false

	for _, style := range styles {
		styleID := style.GetStyleID()
		_, ok := styleSet.stylesMap[styleID]
		if ok {
			return nil, errorsx.Errorf("duplicate style ID found: %q", styleID)
		}

		styleSet.stylesMap[styleID] = style

		if defaultStyleID == styleID {
			defaultIDFound = true
		}
	}

	if !defaultIDFound {
		return nil, errorsx.Errorf("default ID %q not found in any supplied styles", defaultStyleID)
	}

	return styleSet, nil
}

func (s *StyleSet) GetStyleByID(id string) Style {
	return s.stylesMap[id]
}

func (s *StyleSet) GetDefaultStyle() Style {
	return s.stylesMap[s.defaultStyleID]
}

func (s *StyleSet) GetAllStyleIDs() []string {
	var styleIDs []string

	for id := range s.stylesMap {
		styleIDs = append(styleIDs, id)
	}

	return styleIDs
}
