package styling

import "github.com/jamesrr39/ownmap-app/ownmap"

type styleDefinitionType struct {
	MinZoomLevel ownmap.ZoomLevel
	MaxZoomLevel ownmap.ZoomLevel
	When         conditionType
}

type Style struct {
	variables map[string]string
}
