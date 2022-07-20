package webservices

import (
	"math"

	"github.com/paulmach/osm"
)

func Deg2num(lat, lon float64, zoomLevel int) (x, y int) {
	x = int(
		math.Floor((lon + 180.0) / 360.0 * (math.Exp2(float64(zoomLevel)))),
	)
	y = int(
		math.Floor(
			(1.0 - math.Log(
				math.Tan(lat*math.Pi/180.0)+1.0/math.Cos(lat*math.Pi/180.0))/math.Pi) / 2.0 * (math.Exp2(float64(zoomLevel))),
		),
	)
	return
}

func Num2deg(x, y, zoomLevel int) (lat, long float64) {
	n := math.Pi - 2.0*math.Pi*float64(y)/math.Exp2(float64(zoomLevel))
	lat = 180.0 / math.Pi * math.Atan(0.5*(math.Exp(n)-math.Exp(-n)))
	long = float64(x)/math.Exp2(float64(zoomLevel))*360.0 - 180.0
	return lat, long
}

func XYZToBounds(x, y, zoomLevel int) osm.Bounds {
	n := math.Pow(2, float64(zoomLevel))
	longitudeMin := float64(x)/n*360 - 180
	lat_rad := math.Atan(math.Sinh(math.Pi * (1 - 2*float64(y)/n)))
	latitudeMin := lat_rad * 180 / math.Pi

	longitudeMax := float64(x+1)/n*360 - 180
	lat_rad = math.Atan(math.Sinh(math.Pi * (1 - 2*float64(y+1)/n)))
	latitudeMax := lat_rad * 180 / math.Pi

	return osm.Bounds{
		MinLat: latitudeMax,
		MaxLat: latitudeMin,
		MinLon: longitudeMin,
		MaxLon: longitudeMax,
	}
}
