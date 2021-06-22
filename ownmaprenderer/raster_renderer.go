package ownmaprenderer

import (
	"context"
	"fmt"
	"image"
	"image/color"
	"image/draw"
	"log"
	"math"
	"sort"
	"sync"
	"unicode/utf8"

	"github.com/golang/freetype"
	"github.com/golang/freetype/truetype"
	"github.com/jamesrr39/go-tracing"
	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/ownmap-app/ownmap"
	"github.com/jamesrr39/ownmap-app/ownmapdal"
	"github.com/jamesrr39/ownmap-app/styling"
	"github.com/llgcode/draw2d/draw2dimg"
	"github.com/paulmach/osm"
)

type RasterRenderer struct {
	font *truetype.Font
}

func NewRasterRenderer(font *truetype.Font) *RasterRenderer {
	return &RasterRenderer{
		font,
	}
}

func (rr *RasterRenderer) RenderTextTile(size image.Rectangle, text string) (image.Image, errorsx.Error) {
	img := image.NewRGBA(size)
	x := size.Max.X / 2
	y := size.Max.Y / 2

	ctx := freetype.NewContext()
	ctx.SetDPI(72)
	ctx.SetFont(rr.font)
	ctx.SetFontSize(16.0)
	ctx.SetClip(img.Bounds())
	ctx.SetDst(img)
	ctx.SetSrc(image.NewUniform(color.Black))

	_, err := ctx.DrawString(text, freetype.Pt(x, y))
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	return img, nil
}

var placeTagKey = &ownmapdal.TagKeyWithType{
	ObjectType: ownmap.ObjectTypeNode,
	TagKey:     "place",
}

func (rr *RasterRenderer) RenderRaster(ctx context.Context, dbConnSet *ownmapdal.DBConnSet, size image.Rectangle, bounds osm.Bounds, zoomLevel ownmap.ZoomLevel, style styling.Style) (image.Image, errorsx.Error) {
	var err error

	objects := style.GetWantedObjects(zoomLevel)

	// TODO: remove filter, or have filter defined by style
	filter := &ownmapdal.GetInBoundsFilter{
		Objects: objects,
	}

	dbConnsSpan := tracing.StartSpan(ctx, "get dbConns")

	dbConns, err := dbConnSet.GetConnsForBounds(bounds)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	if len(dbConns) == 0 {
		return rr.RenderTextTile(size, "(no data found)")
	}

	dbConnsSpan.End(ctx)

	getDataSpan := tracing.StartSpan(ctx, "get data")

	nodeMap := make(ownmapdal.TagNodeMap)
	wayMap := make(ownmapdal.TagWayMap)
	relationMap := make(ownmapdal.TagRelationMap)

	nodeMapChan := make(chan ownmapdal.TagNodeMap)
	wayMapChan := make(chan ownmapdal.TagWayMap)
	relationMapChan := make(chan ownmapdal.TagRelationMap)
	errChan := make(chan errorsx.Error)

	var wg sync.WaitGroup

	go func() {
		for {
			select {
			case errFromChan := <-errChan:
				err = errFromChan
			case nodeMapFromChan := <-nodeMapChan:
				// TODO on conflicting datasets?
				for k, v := range nodeMapFromChan {
					nodeMap[k] = append(nodeMap[k], v...)
				}
			case wayMapFromChan := <-wayMapChan:
				// TODO on conflicting datasets?
				for k, v := range wayMapFromChan {
					wayMap[k] = append(wayMap[k], v...)
				}
			case relationMapFromChan := <-relationMapChan:
				// TODO on conflicting datasets?
				for k, v := range relationMapFromChan {
					relationMap[k] = append(relationMap[k], v...)
				}
			}
			wg.Done()
		}
	}()

	for _, dbConn := range dbConns {
		wg.Add(1)
		go func(dbConn *ownmapdal.ChosenConnForBounds) {
			defer wg.Done()

			span := tracing.StartSpan(ctx, fmt.Sprintf("data for requested area. Datasource: %q", dbConn.Name()))

			nodeMap, wayMap, relationMap, err := dbConn.GetInBounds(ctx, bounds, filter)
			if err != nil {
				if errorsx.Cause(err) == ownmapdal.ErrNoDataAvailable {
					// no data available, but no error either
					log.Printf("datasource: %q. no data found\n", dbConn.Name())

					return
				}
				wg.Add(1)
				errChan <- errorsx.Wrap(err)
				return
			}

			span.End(ctx)
			span = tracing.StartSpan(ctx, fmt.Sprintf("addNearbyPlaces. Datasource: %q", dbConn.Name()))

			// get place names, which need to be drawn on even for places outside the bounds
			// TODO: more generic for other things that should also be drawn on, even when outside the bounds
			extraBoundsFilter := &ownmapdal.GetInBoundsFilter{
				Objects: []*ownmapdal.TagKeyWithType{placeTagKey},
			}

			err = rr.addNearbyPlaces(ctx, dbConn, nodeMap, bounds, extraBoundsFilter)
			if err != nil {
				wg.Add(1)
				errChan <- errorsx.Wrap(err)
				return
			}

			span.End(ctx)

			log.Printf("datasource: %q. Nodemap len: %d, Waymap len: %d\n", dbConn.Name(), len(nodeMap), len(wayMap))

			wg.Add(3)
			nodeMapChan <- nodeMap
			wayMapChan <- wayMap
			relationMapChan <- relationMap
		}(dbConn)
	}

	wg.Wait()

	getDataSpan.End(ctx)

	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	log.Printf("found %d node tag keys and %d way tag keys\n", len(nodeMap), len(wayMap))

	if len(nodeMap) == 0 && len(wayMap) == 0 {
		return rr.RenderTextTile(size, "(no data found)")
	}

	return rr.drawMap(ctx, nodeMap, wayMap, relationMap, size, bounds, zoomLevel, style)
}

const (
	extraLatDegs = 0.01
	extraLonDegs = 0.01
)

func (rr *RasterRenderer) addNearbyPlaces(
	ctx context.Context,
	dbConn ownmapdal.DataSourceConn,
	nodeOccurenceMap ownmapdal.TagNodeMap,
	requestedBounds osm.Bounds,
	extraBoundsFilter *ownmapdal.GetInBoundsFilter,
) errorsx.Error {
	// calculate the extra bounds we need to search. Top and Bottom are only above and below, whereas left and right also cover the corner areas.
	// explainatory diagram best viewed with monospace fonts.
	/*
		+---+---+---+
		|   | T |   |
		|   +---+   |
		| L |   | R |
		|   +---+   |
		|   | B |   |
		+---+---+---+
	*/

	extraTopBounds := osm.Bounds{
		MinLat: requestedBounds.MaxLat,
		MaxLat: requestedBounds.MaxLat + extraLatDegs,
		MinLon: requestedBounds.MinLon,
		MaxLon: requestedBounds.MaxLon,
	}
	extraBottomBounds := osm.Bounds{
		MinLat: requestedBounds.MinLat - extraLatDegs,
		MaxLat: requestedBounds.MinLat,
		MinLon: requestedBounds.MinLon,
		MaxLon: requestedBounds.MaxLon,
	}

	extraLeftBounds := osm.Bounds{
		MinLat: requestedBounds.MinLat - extraLatDegs,
		MaxLat: requestedBounds.MaxLat + extraLatDegs,
		MinLon: requestedBounds.MinLon - extraLonDegs,
		MaxLon: requestedBounds.MinLon,
	}
	extraRightBounds := osm.Bounds{
		MinLat: requestedBounds.MinLat - extraLatDegs,
		MaxLat: requestedBounds.MinLat + extraLatDegs,
		MinLon: requestedBounds.MaxLon,
		MaxLon: requestedBounds.MaxLon + extraLonDegs,
	}

	extraSearchBounds := []osm.Bounds{
		extraTopBounds,
		extraBottomBounds,
		extraLeftBounds,
		extraRightBounds,
	}

	for _, bounds := range extraSearchBounds {
		subRegionNodeOccurenceMap, _, _, err := dbConn.GetInBounds(ctx, bounds, extraBoundsFilter)
		if err != nil {
			if errorsx.Cause(err) != ownmapdal.ErrNoDataAvailable {
				return err
			}
		}
		nodeOccurenceMap[placeTagKey.TagKey] = append(nodeOccurenceMap[placeTagKey.TagKey], subRegionNodeOccurenceMap[placeTagKey.TagKey]...)
	}

	return nil
}

func (rr *RasterRenderer) drawMap(ctx context.Context, nodeMap ownmapdal.TagNodeMap, waysMap ownmapdal.TagWayMap, relationMap ownmapdal.TagRelationMap, size image.Rectangle, bounds osm.Bounds, zoomLevel ownmap.ZoomLevel, style styling.Style) (image.Image, errorsx.Error) {
	drawMapSpan := tracing.StartSpan(ctx, "drawMap")
	defer drawMapSpan.End(ctx)

	bgColor := style.GetBackground()

	img := NewImageWithBackground(size, bgColor)

	type itemWithStyleType struct {
		ItemStyle styling.ItemStyle
		Item      ownmap.OSMObject
	}

	var itemStyles []itemWithStyleType

	for _, ways := range waysMap {
		for _, way := range ways {
			lineStyle, err := style.GetWayStyle(way.Tags, zoomLevel)
			if err != nil {
				log.Printf("error getting style for way. Error: %q\n", err)
				continue
			}

			if lineStyle == nil {
				// this element shouldn't be shown
				continue
			}

			if lineStyle.ZIndex == 0 {
				return nil, errorsx.Errorf("no zindex provided")
			}

			itemStyles = append(itemStyles, itemWithStyleType{lineStyle, way})
		}
	}

	for tagKey, nodes := range nodeMap {
		if tagKey != "place" {
			log.Printf("not implemented tagKey: %q\n", tagKey)
			continue
		}
		for _, node := range nodes {
			nodeStyle, err := style.GetNodeStyle(node, zoomLevel)
			if err != nil {
				return nil, err
			}

			if nodeStyle == nil {
				continue
			}

			if nodeStyle.ZIndex == 0 {
				return nil, errorsx.Errorf("no zindex provided")
			}

			itemStyles = append(itemStyles, itemWithStyleType{nodeStyle, node})
		}
	}

	for _, relationsData := range relationMap {
		for _, relationData := range relationsData {
			relationStyle, err := style.GetRelationStyle(relationData, zoomLevel)
			if err != nil {
				return nil, err
			}

			if relationStyle == nil {
				continue
			}

			if relationStyle.ZIndex == 0 {
				return nil, errorsx.Errorf("no zindex provided")
			}

			itemStyles = append(itemStyles, itemWithStyleType{relationStyle, relationData})
		}
	}

	// sort styles so that the lowest zindex is at the bottom (first to be drawn), and the highest at the top (last to be drawn)
	sort.Slice(itemStyles, func(a, b int) bool {
		return itemStyles[a].ItemStyle.GetZIndex() < itemStyles[b].ItemStyle.GetZIndex()
	})

	for _, itemStyleAndItem := range itemStyles {
		switch itemStyle := itemStyleAndItem.ItemStyle.(type) {
		case *styling.NodeStyle:
			node := itemStyleAndItem.Item.(*ownmap.OSMNode)
			err := rr.drawPlace(img, bounds, node, style, itemStyle)
			if err != nil {
				return nil, err
			}
		case *styling.WayStyle:
			way := itemStyleAndItem.Item.(*ownmap.OSMWay)
			err := drawWay(img, bounds, way.WayPoints, itemStyle)
			if err != nil {
				return nil, err
			}
		case *styling.RelationStyle:
			relationData := itemStyleAndItem.Item.(*ownmap.RelationData)
			err := drawRelation(img, bounds, relationData, style, zoomLevel)
			if err != nil {
				return nil, err
			}

		default:
			return nil, errorsx.Errorf("didn't understand object %#v", itemStyleAndItem.ItemStyle)
		}
	}

	return img, nil
}

func drawRelation(img *image.RGBA, bounds osm.Bounds, relationData *ownmap.RelationData, style styling.Style, zoomLevel ownmap.ZoomLevel) errorsx.Error {
	relationImg := NewImageWithBackground(img.Rect, color.Transparent)

	wayStyle, err := style.GetWayStyle(relationData.Tags, zoomLevel)
	if err != nil {
		return err
	}

	if wayStyle == nil {
		return nil
	}
	var outerWayPoints []*ownmap.WayPoint
	var innerWays []*ownmap.OSMWay

	for _, m := range relationData.Members {
		switch member := m.Object.(type) {
		case *ownmap.OSMRelation:
			panic(fmt.Sprintf("relation as child member of relation. Relation ID: %d, child member: %#v", relationData.RelationID, member))
		case *ownmap.OSMWay:
			switch m.Role {
			case "outer":
				outerWayPoints = append(outerWayPoints, member.WayPoints...)
			case "inner":
				innerWays = append(innerWays, member)
			default:
				println("TODO: unhandled role: ", m.Role)
			}
		default:
			println(fmt.Sprintf("TODO: unhandled relation member type: %T", m.Object))
		}
	}

	if len(outerWayPoints) == 0 {
		return nil
	}

	err = drawWay(relationImg, bounds, outerWayPoints, wayStyle)
	if err != nil {
		return errorsx.Wrap(err)
	}
	holeInRelationStyle := &styling.WayStyle{
		FillColor: style.GetBackground(),
		LineColor: style.GetBackground(),
	}
	for _, innerWay := range innerWays {
		err = drawWay(relationImg, bounds, innerWay.WayPoints, holeInRelationStyle)
		if err != nil {
			return errorsx.Wrap(err)
		}
	}

	// draw non-transparent parts onto the map image
	draw.Draw(img, img.Bounds(), relationImg, image.Point{}, draw.Over)

	return nil
}

func (rr *RasterRenderer) drawPlaceImg(img draw.Image, name string, pt image.Point, style styling.Style, nodeStyle *styling.NodeStyle) errorsx.Error {
	bgColor := style.GetBackground()

	placeNameImage, err := rr.generatePlaceNameImage(name, bgColor, nodeStyle)
	if err != nil {
		return err
	}

	imgBounds := img.Bounds()
	bgR, bgG, bgB, bgA := bgColor.RGBA()
	// copy place name image onto image
	placeNameImageBounds := placeNameImage.Bounds()
	for y := placeNameImageBounds.Min.Y; y < placeNameImageBounds.Max.Y; y++ {
		imgY := pt.Y + y
		if imgY < imgBounds.Min.Y || imgY > imgBounds.Max.Y {
			continue
		}
		for x := placeNameImageBounds.Min.X; x < placeNameImageBounds.Max.X; x++ {
			imgX := pt.X + x
			if imgX < imgBounds.Min.X || imgX > imgBounds.Max.X {
				continue
			}
			colorInPlaceNameImage := placeNameImage.At(x, y)
			r, g, b, a := colorInPlaceNameImage.RGBA()
			if r != bgR || g != bgG || b != bgB || a != bgA {
				img.Set(imgX, imgY, colorInPlaceNameImage)
			}
		}
	}

	return nil
}

func (rr *RasterRenderer) generatePlaceNameImage(name string, bgColor color.Color, nodeStyle *styling.NodeStyle) (image.Image, errorsx.Error) {
	width := utf8.RuneCountInString(name) * nodeStyle.TextSize
	height := nodeStyle.TextSize * 2
	fontImg := NewImageWithBackground(image.Rect(0, 0, width, height), bgColor)

	ctx := freetype.NewContext()
	ctx.SetDPI(72)
	ctx.SetFont(rr.font)
	ctx.SetFontSize(float64(nodeStyle.TextSize))
	ctx.SetClip(fontImg.Bounds())
	ctx.SetDst(fontImg)
	ctx.SetSrc(image.NewUniform(nodeStyle.TextColor))

	_, err := ctx.DrawString(name, freetype.Pt(0, height/2))
	if err != nil {
		return nil, errorsx.Wrap(err)
	}
	return fontImg, nil
}

func (rr *RasterRenderer) drawPlace(img draw.Image, bounds osm.Bounds, node *ownmap.OSMNode, style styling.Style, nodeStyle *styling.NodeStyle) errorsx.Error {
	size := img.Bounds()
	imgSizeX := float64(size.Max.X)
	imgSizeY := float64(size.Max.Y)

	var name string
	for _, tag := range node.Tags {
		if tag.Key == "name" {
			name = tag.Value
			break
		}
	}

	xThroughBounds := (node.Lon - bounds.MinLon) / (bounds.MaxLon - bounds.MinLon)
	yThroughBounds := 1 - ((node.Lat - bounds.MinLat) / (bounds.MaxLat - bounds.MinLat))

	x := int(math.Floor(xThroughBounds * imgSizeX))
	y := int(math.Floor(yThroughBounds * imgSizeY))

	err := rr.drawPlaceImg(img, name, image.Point{X: x, Y: y}, style, nodeStyle)
	if err != nil {
		return err
	}

	return nil
}

func drawWay(img *image.RGBA, bounds osm.Bounds, wayPoints []*ownmap.WayPoint, lineStyle *styling.WayStyle) errorsx.Error {
	lonBetweenMinAndMax := bounds.MaxLon - bounds.MinLon
	latBetweenMinAndMax := bounds.MaxLat - bounds.MinLat
	var points []*Point

	for _, waypoint := range wayPoints {
		x := (waypoint.Point.Lon - bounds.MinLon) / (lonBetweenMinAndMax)
		y := (waypoint.Point.Lat - bounds.MinLat) / (latBetweenMinAndMax)

		point := &Point{
			X: x,
			Y: y,
		}

		points = append(points, point)
	}

	gc := draw2dimg.NewGraphicContext(img)
	defer gc.Close()
	err := drawLine(gc, img, points, lineStyle)
	if err != nil {
		return err
	}

	return nil
}

func calcArtificialPoint(inBoundsPoint, outOfBoundsPoint *Point) *Point {
	deltaX := (inBoundsPoint.X - outOfBoundsPoint.X)
	deltaY := (inBoundsPoint.Y - outOfBoundsPoint.Y)
	gradient := deltaY / deltaX
	isLineGoingUp := deltaY > 0
	isLineGoingRight := deltaX > 0

	xTarget := float64(0)
	if !isLineGoingRight {
		xTarget = 1
	}

	yTarget := float64(0)
	if !isLineGoingUp {
		yTarget = 1
	}

	yAtXTarget := inBoundsPoint.Y - (gradient * (inBoundsPoint.X - xTarget))
	yAtXTargetInBounds := yAtXTarget < 1 && yAtXTarget > 0
	if yAtXTargetInBounds {
		return &Point{
			X: xTarget,
			Y: yAtXTarget,
		}
	}

	return &Point{
		X: inBoundsPoint.X - ((inBoundsPoint.Y - yTarget) / gradient),
		Y: yTarget,
	}
}

type Point struct {
	X float64 // between 0 and 1. 0 = left of image, 1 = right of image
	Y float64 // between 0 and 1. 0 = top of image, 1 = bottom of image
}

func drawLine(gc *draw2dimg.GraphicContext, img draw.Image, points []*Point, lineStyle *styling.WayStyle) errorsx.Error {
	if lineStyle.FillColor != nil {
		gc.SetFillColor(lineStyle.FillColor)
	}
	if lineStyle.LineColor != nil {
		gc.SetStrokeColor(lineStyle.LineColor)
	}
	if lineStyle.LineWidth != 0 {
		gc.SetLineWidth(lineStyle.LineWidth)
	}
	if lineStyle.LineDashPolicy != nil {
		gc.SetLineDash(lineStyle.LineDashPolicy, 0)
	}
	gc.BeginPath()

	imgWidth := float64(img.Bounds().Max.X)
	imgHeight := float64(img.Bounds().Max.Y)

	for i, point := range points {

		pointX := point.X * imgWidth
		pointY := (1 - point.Y) * imgHeight

		if i == 0 {
			gc.MoveTo(pointX, pointY)
		} else {
			gc.LineTo(pointX, pointY)
		}
	}
	if lineStyle.FillColor != nil {
		// final line from last point to first
		if len(points) > 0 {
			point := points[0]
			pointX := point.X * imgWidth
			pointY := (1 - point.Y) * imgHeight
			gc.LineTo(pointX, pointY)
		}

		gc.Fill()
	}
	gc.Stroke()

	return nil
}
