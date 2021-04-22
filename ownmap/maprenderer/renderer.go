package maprenderer

import (
	"context"
	"image"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/ownmap-app/ownmap"
	"github.com/jamesrr39/ownmap-app/ownmapdal"
	"github.com/jamesrr39/ownmap-app/styling"
	"github.com/paulmach/osm"
)

type MapRenderer interface {
	RenderRaster(ctx context.Context, dbConnSet *ownmapdal.DBConnSet, size image.Rectangle, bounds osm.Bounds, zoomLevel ownmap.ZoomLevel, style styling.Style) (image.Image, errorsx.Error)
	RenderTextTile(size image.Rectangle, text string) (image.Image, errorsx.Error)
}
