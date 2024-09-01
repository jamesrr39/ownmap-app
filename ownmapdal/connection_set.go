package ownmapdal

import (
	"context"
	"log"
	"sync"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/ownmap-app/ownmap"
	"github.com/paulmach/osm"
)

type DataSourceConn interface {
	// Info methods
	Name() string
	DatasetInfo() (*ownmap.DatasetInfo, errorsx.Error)

	// Data fetch methods
	GetInBounds(ctx context.Context, bounds osm.Bounds, filter *GetInBoundsFilter) (TagNodeMap, TagWayMap, TagRelationMap, errorsx.Error)
}

type DBConnSet struct {
	conns []DataSourceConn
	mu    *sync.RWMutex
}

func NewDBConnSet(conns []DataSourceConn) *DBConnSet {
	return &DBConnSet{conns, new(sync.RWMutex)}
}

func (dbcs *DBConnSet) GetConns() []DataSourceConn {
	dbcs.mu.RLock()
	defer dbcs.mu.RUnlock()
	return dbcs.conns
}

func (dbcs *DBConnSet) AddDBConn(conn DataSourceConn) {
	dbcs.mu.Lock()
	defer dbcs.mu.Unlock()
	dbcs.conns = append(dbcs.conns, conn)
}

//go:generate stringer -type=MatchLevel
type MatchLevel int

const (
	MatchLevelNone MatchLevel = iota
	MatchLevelPartial
	MatchLevelFull
)

type ChosenConnForBounds struct {
	MatchLevel MatchLevel
	DataSourceConn
}

func getMatchLevel(conn DataSourceConn, bounds osm.Bounds) (MatchLevel, errorsx.Error) {
	datasetInfo, err := conn.DatasetInfo()
	if err != nil {
		return 0, errorsx.Wrap(err)
	}

	dataSourceBounds := datasetInfo.Bounds.ToOSMBounds()

	atLeastPartialMatch := ownmap.Overlaps(dataSourceBounds, bounds)
	if !atLeastPartialMatch {
		return MatchLevelNone, nil
	}

	isFullMatch := ownmap.IsTotallyInside(dataSourceBounds, bounds)
	if isFullMatch {
		return MatchLevelFull, nil
	}

	return MatchLevelPartial, nil
}

// GetConnForBounds selects a connection to use to provide data for a given bounds
func (dbcs *DBConnSet) GetConnsForBounds(bounds osm.Bounds) ([]*ChosenConnForBounds, errorsx.Error) {
	var chosen []*ChosenConnForBounds

	for _, conn := range dbcs.GetConns() {
		matchLevel, err := getMatchLevel(conn, bounds)
		if err != nil {
			return nil, err
		}

		log.Printf("matchlevel: %s, file: %v\n", matchLevel, conn.Name())

		if matchLevel == MatchLevelNone {
			continue
		}

		chosen = append(chosen, &ChosenConnForBounds{
			DataSourceConn: conn,
			MatchLevel:     matchLevel,
		})
	}

	return chosen, nil
}
