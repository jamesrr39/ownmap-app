package webservices

import (
	"image"
	"image/png"
	"log"
	"net"
	"net/http"
	"strconv"

	"github.com/go-chi/chi"
	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/goutil/logpkg"
	"github.com/jamesrr39/ownmap-app/ownmap"
	"github.com/jamesrr39/ownmap-app/ownmap/maprenderer"
	"github.com/jamesrr39/ownmap-app/ownmapdal"
	"github.com/jamesrr39/ownmap-app/styling"
	"github.com/jamesrr39/semaphore"
	"github.com/pkg/profile"
)

type TileService struct {
	logger        *logpkg.Logger
	dbConnSet     *ownmapdal.DBConnSet
	sema          *semaphore.Semaphore
	rasterer      maprenderer.MapRenderer
	styleSet      *styling.StyleSet
	shouldProfile bool
	chi.Router
}

func NewTileService(logger *logpkg.Logger, dbConnSet *ownmapdal.DBConnSet, rasterer maprenderer.MapRenderer, styleSet *styling.StyleSet, shouldProfile bool) *TileService {
	ts := &TileService{logger, dbConnSet, semaphore.NewSemaphore(4), rasterer, styleSet, shouldProfile, chi.NewRouter()}

	ts.Get("/raster/{z}/{x}/{y}", ts.handleGetTile)

	return ts
}

func (ts *TileService) getStyle(styleID string) (styling.Style, errorsx.Error) {
	if styleID == "" {
		return ts.styleSet.GetDefaultStyle(), nil
	}

	style := ts.styleSet.GetStyleByID(styleID)
	if style == nil {
		return nil, errorsx.Errorf("couldn't get requested style %q (style not loaded)", styleID)
	}

	return style, nil
}

func (ts *TileService) handleGetTile(w http.ResponseWriter, r *http.Request) {
	if ts.shouldProfile {
		defer profile.Start().Stop()
	}
	x := chi.URLParam(r, "x")
	y := chi.URLParam(r, "y")
	zStr := chi.URLParam(r, "z")
	styleID := r.URL.Query().Get("styleId")

	ints, err := stringsToInts(x, y, zStr)
	if err != nil {
		errorsx.HTTPError(w, ts.logger, errorsx.Wrap(err), 400)
		return
	}

	style, err := ts.getStyle(styleID)
	if err != nil {
		errorsx.HTTPError(w, ts.logger, errorsx.Wrap(err), 400)
		return
	}

	size := image.Rect(0, 0, 256, 256)

	z := ints[2]

	bounds := XYZToBounds(ints[0], ints[1], ints[2])
	log.Printf("serving x, y, z: %s %s %s. Bounds (NW, SE): [%f %f, %f %f]\n", x, y, zStr, bounds.MaxLat, bounds.MinLon, bounds.MinLat, bounds.MaxLon)

	ts.sema.Add()
	defer ts.sema.Done()

	img, err := ts.rasterer.RenderRaster(ts.dbConnSet, size, bounds, ownmap.ZoomLevel(z), style)
	if err != nil {
		errorsx.HTTPError(w, ts.logger, errorsx.Wrap(err), 500)
		return
	}

	err = png.Encode(w, img)
	if err != nil {
		switch err.(type) {
		case *net.OpError:
			// broken pipe (request cancelled). Do nothing
		default:
			errorsx.HTTPError(w, ts.logger, errorsx.Wrap(err), 500)
		}
		return
	}
}

func stringsToInts(s ...string) ([]int, error) {
	var ints []int
	for _, str := range s {
		i, err := strconv.Atoi(str)
		if err != nil {
			return nil, err
		}
		ints = append(ints, i)
	}

	return ints, nil
}
