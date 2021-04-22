package webservices

import (
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/go-chi/chi"
	"github.com/go-chi/render"
	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/goutil/logpkg"
	"github.com/jamesrr39/ownmap-app/ownmap"
	"github.com/jamesrr39/ownmap-app/ownmapdal"
	"github.com/paulmach/osm"
)

type NearbyThingsWebService struct {
	logger    *logpkg.Logger
	dbConnSet *ownmapdal.DBConnSet
	chi.Router
}

func NewNearbyThingsWebService(logger *logpkg.Logger, dbConnSet *ownmapdal.DBConnSet) *NearbyThingsWebService {
	router := chi.NewRouter()
	service := &NearbyThingsWebService{logger, dbConnSet, router}

	router.Get("/", service.handleGet)
	return service
}

type getNearbyPlacesResponseType struct {
	Places []*ownmap.OSMNode `json:"places"`
}

func (s *NearbyThingsWebService) handleGet(w http.ResponseWriter, r *http.Request) {
	var err error
	bounds, err := parseBoundsString(r.URL.Query().Get("bounds"))
	if err != nil {
		errorsx.HTTPError(w, s.logger, errorsx.Wrap(err), http.StatusBadRequest)
		return
	}

	// desiredTagKey := ownmapdal.TagKeyWithType{
	// 	TagKey:     ownmap.TagKey("place"),
	// 	ObjectType: ownmap.ObjectTypeNode,
	// }
	desiredTagKey := &ownmapdal.TagKeyWithType{
		TagKey:     ownmap.TagKey("natural"),
		ObjectType: ownmap.ObjectTypeWay,
	}

	conns, err := s.dbConnSet.GetConnsForBounds(*bounds)
	if err != nil {
		errorsx.HTTPError(w, s.logger, errorsx.Wrap(err), http.StatusInternalServerError)
		return
	}

	if len(conns) == 0 {
		render.JSON(w, r, getNearbyPlacesResponseType{})
		return
	}

	errChan := make(chan errorsx.Error)
	nodeMapChan := make(chan ownmapdal.TagNodeMap)
	nodeMap := make(ownmapdal.TagNodeMap)

	var wg sync.WaitGroup
	go func() {
		for {
			select {
			case errFromChan := <-errChan:
				err = errFromChan
			case nodeMapFromChan := <-nodeMapChan:
				for k, v := range nodeMapFromChan {
					nodeMap[k] = append(nodeMap[k], v...)
				}
			}
			wg.Done()
		}
	}()

	for _, conn := range conns {
		wg.Add(1)
		go func(conn *ownmapdal.ChosenConnForBounds) {
			defer wg.Done()

			nodeMap, _, _, err := conn.GetInBounds(*bounds, &ownmapdal.GetInBoundsFilter{
				Objects: []*ownmapdal.TagKeyWithType{desiredTagKey},
			})
			if err != nil {
				wg.Add(1)
				errChan <- errorsx.Wrap(err)
				return
			}

			wg.Add(1)
			nodeMapChan <- nodeMap
		}(conn)
	}

	wg.Wait()

	if err != nil {
		errorsx.HTTPError(w, s.logger, errorsx.Wrap(err), http.StatusInternalServerError)
		return
	}

	render.JSON(w, r, getNearbyPlacesResponseType{nodeMap[desiredTagKey.TagKey]})
}

// (S,W,N,E)
// (52.533251,-1.394072,52.800548,-0.898208)
func parseBoundsString(boundsString string) (*osm.Bounds, errorsx.Error) {
	bounds := &osm.Bounds{}

	withoutBrackets := strings.TrimPrefix(strings.TrimSuffix(boundsString, ")"), "(")
	fragments := strings.Split(withoutBrackets, ",")
	if len(fragments) != 4 {
		return nil, errorsx.Errorf("expected 4 bounds, but got %d. A bounds URL parameter should be in the format 'bounds=(S,W,N,E)'", len(fragments))
	}

	for index, fragment := range fragments {
		trimmedFragment := strings.TrimSpace(fragment)
		coordinate, err := strconv.ParseFloat(trimmedFragment, 64)
		if err != nil {
			return nil, errorsx.Wrap(err)
		}

		switch index {
		case 0:
			bounds.MinLat = coordinate
		case 1:
			bounds.MinLon = coordinate
		case 2:
			bounds.MaxLat = coordinate
		case 3:
			bounds.MaxLon = coordinate
		}
	}

	return bounds, nil
}
