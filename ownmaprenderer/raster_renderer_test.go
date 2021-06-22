package ownmaprenderer

import (
	"image"
	"image/color"
	"image/draw"
	"reflect"
	"testing"

	"github.com/golang/freetype/truetype"
	snapshot "github.com/jamesrr39/go-snapshot-testing"
	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/ownmap-app/fonts"
	"github.com/jamesrr39/ownmap-app/ownmap"
	"github.com/jamesrr39/ownmap-app/ownmapdal"
	"github.com/jamesrr39/ownmap-app/styling"
	"github.com/paulmach/osm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_drawWay(t *testing.T) {
	img := image.NewRGBA(image.Rectangle{
		Max: image.Point{
			X: 10,
			Y: 10,
		},
	})
	draw.Draw(img, img.Bounds(), image.NewUniform(color.White), image.Point{}, draw.Src)

	bounds := osm.Bounds{
		MinLat: 2,
		MaxLat: 4,
		MinLon: 10,
		MaxLon: 12,
	}
	way := []*ownmap.WayPoint{}

	err := drawWay(img, bounds, way, &styling.WayStyle{LineColor: color.Black})
	require.NoError(t, err)

	snapshot.AssertMatchesSnapshot(t, "Test_drawWay_1", snapshot.NewImageSnapshot(img))
}

type calcArtificialPointTest struct {
	Expected         Point
	InBoundsPoint    *Point
	OutOfBoundsPoint *Point
	Info             string
}

func Test_calcArtificialPoint(t *testing.T) {
	var tests = []calcArtificialPointTest{
		{
			Expected:         Point{X: 0, Y: 0.25},
			InBoundsPoint:    &Point{X: 0.5, Y: 0.5},
			OutOfBoundsPoint: &Point{X: -0.5, Y: 0},
			Info:             "from bottom left to center",
		}, {
			Expected:         Point{X: 0, Y: 0.5},
			InBoundsPoint:    &Point{X: 0.5, Y: 0.5},
			OutOfBoundsPoint: &Point{X: -0.5, Y: 0.5},
			Info:             "from left to center (delta Y = 0)",
		}, {
			Expected:         Point{X: 1, Y: 1},
			InBoundsPoint:    &Point{X: 0.5, Y: 0.5},
			OutOfBoundsPoint: &Point{X: 2, Y: 2},
			Info:             "from top right to center",
		}, {
			Expected:         Point{X: 0.25, Y: 1},
			InBoundsPoint:    &Point{X: 0.5, Y: 0.5},
			OutOfBoundsPoint: &Point{X: 0, Y: 1.5},
			Info:             "from top left to center",
		}, {
			Expected:         Point{X: 1, Y: 0},
			InBoundsPoint:    &Point{X: 0.5, Y: 0.5},
			OutOfBoundsPoint: &Point{X: 1.5, Y: -0.5},
			Info:             "from bottom right to center",
		},
	}
	for _, test := range tests {
		point := calcArtificialPoint(test.InBoundsPoint, test.OutOfBoundsPoint)
		assert.Equal(t, test.Expected, *point, test.Info)
	}
}

func TestRasterRenderer_RenderTextTile(t *testing.T) {
	type fields struct {
		font     *truetype.Font
		fontSize int
		readDAL  ownmapdal.ReadDAL
	}
	type args struct {
		size image.Rectangle
		text string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   image.Image
		want1  errorsx.Error
	}{
		{
			"my tow1",
			fields{
				font:     fonts.DefaultFont(),
				fontSize: 72,
				readDAL:  nil,
			},
			args{
				size: image.Rect(0, 0, 128, 40),
				text: "my town",
			},
			nil,
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rr := &RasterRenderer{
				font: tt.fields.font,
			}
			got, got1 := rr.RenderTextTile(tt.args.size, tt.args.text)
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("RasterRenderer.RenderTextTile() got1 = %v, want %v", got1, tt.want1)
			}

			snapshot.AssertMatchesSnapshot(t, "RenderTextTile", snapshot.NewImageSnapshot(got))
		})
	}
}

func TestRasterRenderer_drawPlace(t *testing.T) {
	type fields struct {
		font     *truetype.Font
		fontSize int
		readDAL  ownmapdal.ReadDAL
	}
	type args struct {
		img    *image.RGBA
		bounds osm.Bounds
		node   *ownmap.OSMNode
		style  styling.Style
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   errorsx.Error
	}{
		{
			"pos lat, pos lon",
			fields{fonts.DefaultFont(), 72, nil},
			args{
				image.NewRGBA(image.Rect(0, 0, 256, 256)),
				osm.Bounds{MaxLat: 1, MinLat: 0, MaxLon: 1, MinLon: 0},
				&ownmap.OSMNode{
					Tags: []*ownmap.OSMTag{
						{
							Key:   "place",
							Value: "my town",
						},
					},
					Lat: 0.9,
					Lon: 0.8,
				},
				&styling.CustomBasicStyle{},
			},
			nil,
		},
		{
			"pos lat, neg lon",
			fields{fonts.DefaultFont(), 72, nil},
			args{
				image.NewRGBA(image.Rect(0, 0, 256, 256)),
				osm.Bounds{MaxLat: 1, MinLat: 0, MaxLon: 0, MinLon: -1},
				&ownmap.OSMNode{
					Tags: []*ownmap.OSMTag{
						{
							Key:   "place",
							Value: "my town",
						},
					},
					Lat: 0.9,
					Lon: -0.8,
				},
				&styling.CustomBasicStyle{},
			},
			nil,
		},
		{
			"neg lat, pos lon",
			fields{fonts.DefaultFont(), 72, nil},
			args{
				image.NewRGBA(image.Rect(0, 0, 256, 256)),
				osm.Bounds{MaxLat: 0, MinLat: -1, MaxLon: 1, MinLon: 0},
				&ownmap.OSMNode{
					Tags: []*ownmap.OSMTag{
						{
							Key:   "place",
							Value: "my town",
						},
					},
					Lat: -0.9,
					Lon: 0.8,
				},
				&styling.CustomBasicStyle{},
			},
			nil,
		},
		{
			"neg lat, neg lon",
			fields{fonts.DefaultFont(), 72, nil},
			args{
				image.NewRGBA(image.Rect(0, 0, 256, 256)),
				osm.Bounds{MaxLat: 0, MinLat: -1, MaxLon: 0, MinLon: -1},
				&ownmap.OSMNode{
					Tags: []*ownmap.OSMTag{
						{
							Key:   "place",
							Value: "my town",
						},
					},
					Lat: -0.9,
					Lon: -0.8,
				},
				&styling.CustomBasicStyle{},
			},
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rr := &RasterRenderer{
				font: tt.fields.font,
			}

			nodeStyle := &styling.NodeStyle{TextSize: 8}
			if got := rr.drawPlace(tt.args.img, tt.args.bounds, tt.args.node, tt.args.style, nodeStyle); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("RasterRenderer.drawPlace() = %v, want %v", got, tt.want)
			}

			snapshot.AssertMatchesSnapshot(t, "RasterRenderer_drawPlace_"+tt.name, snapshot.NewImageSnapshot(tt.args.img))
		})
	}
}

func TestRasterRenderer_drawRelation(t *testing.T) {
	type args struct {
		relationImg  *image.RGBA
		bounds       osm.Bounds
		relationData *ownmap.RelationData
		style        styling.Style
		zoomLevel    ownmap.ZoomLevel
	}
	tests := []struct {
		name string
		args args
		want errorsx.Error
	}{
		{
			args: args{
				relationImg: image.NewRGBA(image.Rect(0, 0, 256, 256)),
				bounds: osm.Bounds{
					MinLat: -1,
					MinLon: -1,
					MaxLat: 1,
					MaxLon: 1,
				},
				relationData: &ownmap.RelationData{
					Tags: []*ownmap.OSMTag{
						{Key: "landuse", Value: "forest"},
					},
					Members: []*ownmap.RelationMemberData{
						{Role: "outer", Object: &ownmap.OSMWay{
							WayPoints: []*ownmap.WayPoint{
								{Point: &ownmap.Location{Lat: 0.8, Lon: 0.8}},
								{Point: &ownmap.Location{Lat: 0.8, Lon: -0.8}},
								{Point: &ownmap.Location{Lat: -0.8, Lon: -0.8}},
								{Point: &ownmap.Location{Lat: -0.8, Lon: 0.8}},
								{Point: &ownmap.Location{Lat: 0.8, Lon: 0.8}},
							},
						}},
						{Role: "inner", Object: &ownmap.OSMWay{
							WayPoints: []*ownmap.WayPoint{
								{Point: &ownmap.Location{Lat: 0.6, Lon: 0.6}},
								{Point: &ownmap.Location{Lat: 0.6, Lon: 0}},
								{Point: &ownmap.Location{Lat: 0, Lon: 0}},
								{Point: &ownmap.Location{Lat: 0, Lon: 0.6}},
								{Point: &ownmap.Location{Lat: 0.6, Lon: 0.6}},
							},
						}},
					},
				},
				style:     &styling.CustomBasicStyle{},
				zoomLevel: 15,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			img := NewImageWithBackground(tt.args.relationImg.Rect, tt.args.style.GetBackground())
			err := drawRelation(img, tt.args.bounds, tt.args.relationData, tt.args.style, tt.args.zoomLevel)
			require.NoError(t, err)

			snapshot.AssertMatchesSnapshot(t, t.Name(), snapshot.NewImageSnapshot(img))
		})
	}
}
