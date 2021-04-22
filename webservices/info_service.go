package webservices

import (
	"net/http"
	"sort"

	"github.com/go-chi/chi"
	"github.com/go-chi/render"
	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/goutil/logpkg"
	"github.com/jamesrr39/ownmap-app/ownmap"
	"github.com/jamesrr39/ownmap-app/ownmapdal"
	"github.com/jamesrr39/ownmap-app/styling"
)

func NewInfoService(logger *logpkg.Logger, dbConnSet *ownmapdal.DBConnSet, styleSet *styling.StyleSet) *InfoService {
	ws := &InfoService{logger, dbConnSet, styleSet, chi.NewRouter()}
	ws.Get("/", ws.handleGet)

	return ws
}

type InfoService struct {
	logger    *logpkg.Logger
	dbConnSet *ownmapdal.DBConnSet
	styleSet  *styling.StyleSet
	chi.Router
}

type stylesType struct {
	DefaultStyleID string   `json:"defaultStyleId"`
	StyleIDs       []string `json:"styleIds"`
}

type datasetType struct {
	Style    stylesType            `json:"style"`
	Datasets []*ownmap.DatasetInfo `json:"datasets"`
}

func (ws *InfoService) handleGet(w http.ResponseWriter, r *http.Request) {
	infos := []*ownmap.DatasetInfo{}

	for _, conn := range ws.dbConnSet.GetConns() {
		info, err := conn.DatasetInfo()
		if err != nil {
			errorsx.HTTPError(w, ws.logger, err, http.StatusInternalServerError)
			return
		}

		infos = append(infos, info)
	}

	// make deterministic
	sort.Slice(infos, func(a, b int) bool {
		if infos[a].Bounds.MinLon < infos[b].Bounds.MinLon {
			return true
		}

		// TODO more thorough sort
		return false
	})

	style := stylesType{
		ws.styleSet.GetDefaultStyle().GetStyleID(),
		ws.styleSet.GetAllStyleIDs(),
	}

	render.JSON(w, r, datasetType{style, infos})
}
