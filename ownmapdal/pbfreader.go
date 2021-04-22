package ownmapdal

import (
	"context"
	"io"
	"runtime"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/goutil/gofs"
	"github.com/jamesrr39/ownmap-app/ownmap"
	"github.com/paulmach/osm"
	"github.com/paulmach/osm/osmpbf"
)

type PBFReader interface {
	Header() (*osmpbf.Header, error)
	Scan() bool
	Object() osm.Object
	Err() error
	Reset() errorsx.Error
	FullyScannedBytes() int64
	TotalSize() int64
}

type DefaultPBFReader struct {
	file gofs.File
	*osmpbf.Scanner
	totalSize int64
}

func NewDefaultPBFReader(file gofs.File) (*DefaultPBFReader, errorsx.Error) {
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, errorsx.Wrap(err)
	}
	osmPBFReader := osmpbf.New(context.Background(), file, runtime.NumCPU())

	return &DefaultPBFReader{file, osmPBFReader, fileInfo.Size()}, nil
}

func (r *DefaultPBFReader) TotalSize() int64 {
	return r.totalSize
}

func (r *DefaultPBFReader) Reset() errorsx.Error {
	err := r.Scanner.Close()
	if err != nil {
		return errorsx.Wrap(err)
	}
	_, err = r.file.Seek(0, io.SeekStart)
	if err != nil {
		return errorsx.Wrap(err)
	}
	r.Scanner = osmpbf.New(context.Background(), r.file, runtime.NumCPU())
	return nil
}

type ImportRunType struct {
	Bounds             osm.Bounds
	RequiredTagKeysMap map[string]bool
	Rescan             *Rescan
	MaxItemsPerBatch   uint64
}

func (importRun *ImportRunType) Validate() errorsx.Error {
	if importRun.MaxItemsPerBatch == 0 {
		return errorsx.Errorf("no MaxItemsPerBatch specified")
	}

	zeroBounds := osm.Bounds{}
	if importRun.Bounds == zeroBounds {
		return errorsx.Errorf("bounds not set")
	}

	return nil
}

type OnNodeReceivedFuncType func(node *ownmap.OSMNode) errorsx.Error
type OnWayReceivedFuncType func(way *ownmap.OSMWay, nodes []*ownmap.OSMNode) errorsx.Error
type OnRelationReceivedFuncType func(relation *ownmap.OSMRelation, nodes []*ownmap.OSMNode) errorsx.Error

type GetNodeByIDType func(id int64) (*ownmap.OSMNode, errorsx.Error)
type GetWayByIDType func(id int64) (*ownmap.OSMWay, errorsx.Error)
type GetRelationByIDType func(id int64) (*ownmap.OSMRelation, errorsx.Error)

// scanBatch returns (reader has more objects to scan, error)
func scanBatch(
	pbfReader PBFReader,
	importRun *ImportRunType,
	scanObject scanObjectFunc,
) (bool, errorsx.Error) {
	var err error

	// scan batch
	for i := uint64(0); i < importRun.MaxItemsPerBatch; i++ {
		cont := pbfReader.Scan()
		if !cont {
			return false, nil
		}

		err = scanObject(pbfReader.Object())
		if err != nil {
			return false, errorsx.Wrap(err)
		}
	}

	if pbfReader.Err() != nil {
		return false, errorsx.Wrap(pbfReader.Err())
	}

	return true, nil
}

func calcBoundsForWay(points []*ownmap.Location) osm.Bounds {
	objBounds := osm.Bounds{
		MaxLat: -90,
		MinLat: 90,
		MaxLon: -180,
		MinLon: 180,
	}
	for _, point := range points {
		if point.Lat < objBounds.MinLat {
			objBounds.MinLat = point.Lat
		}
		if point.Lat > objBounds.MaxLat {
			objBounds.MaxLat = point.Lat
		}
		if point.Lon < objBounds.MinLon {
			objBounds.MinLon = point.Lon
		}
		if point.Lon > objBounds.MaxLon {
			objBounds.MaxLon = point.Lon
		}
	}
	return objBounds
}

func atLeastOneNodeInBounds(points []*ownmap.Location, bounds osm.Bounds) bool {
	if len(points) == 0 {
		// none of the nodes are within the specified bounds for the import. Ignore this way.
		return false
	}

	boundsForWay := calcBoundsForWay(points)
	if !ownmap.Overlaps(boundsForWay, bounds) {
		return false
	}

	return true
}
