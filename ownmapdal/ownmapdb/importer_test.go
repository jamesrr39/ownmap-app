package ownmapdb

import (
	"encoding/binary"
	"reflect"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/goutil/gofs"
	"github.com/jamesrr39/goutil/logpkg"
	"github.com/jamesrr39/ownmap-app/ownmap"
	"github.com/jamesrr39/ownmap-app/ownmapdal/ownmapdb/diskfilemap"
	"github.com/paulmach/osm/osmpbf"
)

func Test_getTagIndexesForWay(t *testing.T) {
	type args struct {
		tags       []*ownmap.OSMTag
		nodes      []*ownmap.Location
		objectType ownmap.ObjectType
	}
	tests := []struct {
		name string
		args args
		want []*tagCollectionKeyType
	}{
		{
			name: "no tags",
			args: args{
				tags: nil,
				nodes: []*ownmap.Location{
					{
						Lat: 0,
						Lon: 0,
					}, {
						Lat: 0.0000000001,
						Lon: 0.0000000001,
					},
				},
				objectType: ownmap.ObjectTypeWay,
			},
			want: nil,
		}, {
			name: "same box",
			args: args{
				tags: []*ownmap.OSMTag{
					{Key: "place"},
				},
				nodes: []*ownmap.Location{
					{
						Lat: 0,
						Lon: 0,
					}, {
						Lat: 0.0000000001,
						Lon: 0.0000000001,
					},
				},
				objectType: ownmap.ObjectTypeWay,
			},
			want: []*tagCollectionKeyType{
				{LatBucket: 0, LonBucket: 0, ObjectType: ownmap.ObjectTypeWay, TagKey: "place"},
			},
		}, {
			name: "positive lat & lon gradient",
			args: args{
				tags: []*ownmap.OSMTag{
					{Key: "place"},
				},
				nodes: []*ownmap.Location{
					{
						Lat: 0,
						Lon: 3,
					}, {
						Lat: 0.01,
						Lon: 3.01,
					}, {
						Lat: 0.03,
						Lon: 3.03,
					},
				},
				objectType: ownmap.ObjectTypeWay,
			},
			want: []*tagCollectionKeyType{
				{LatBucket: 0, LonBucket: 300, ObjectType: ownmap.ObjectTypeWay, TagKey: "place"},
				{LatBucket: 0, LonBucket: 301, ObjectType: ownmap.ObjectTypeWay, TagKey: "place"},
				{LatBucket: 1, LonBucket: 300, ObjectType: ownmap.ObjectTypeWay, TagKey: "place"},
				{LatBucket: 1, LonBucket: 301, ObjectType: ownmap.ObjectTypeWay, TagKey: "place"},
				{LatBucket: 1, LonBucket: 302, ObjectType: ownmap.ObjectTypeWay, TagKey: "place"},
				{LatBucket: 1, LonBucket: 303, ObjectType: ownmap.ObjectTypeWay, TagKey: "place"},
				{LatBucket: 2, LonBucket: 301, ObjectType: ownmap.ObjectTypeWay, TagKey: "place"},
				{LatBucket: 2, LonBucket: 302, ObjectType: ownmap.ObjectTypeWay, TagKey: "place"},
				{LatBucket: 2, LonBucket: 303, ObjectType: ownmap.ObjectTypeWay, TagKey: "place"},
				{LatBucket: 3, LonBucket: 301, ObjectType: ownmap.ObjectTypeWay, TagKey: "place"},
				{LatBucket: 3, LonBucket: 302, ObjectType: ownmap.ObjectTypeWay, TagKey: "place"},
				{LatBucket: 3, LonBucket: 303, ObjectType: ownmap.ObjectTypeWay, TagKey: "place"},
			},
		}, {
			name: "positive lat gradient",
			args: args{
				tags: []*ownmap.OSMTag{
					{Key: "place"},
				},
				nodes: []*ownmap.Location{
					{
						Lat: 0,
						Lon: 0,
					}, {
						Lat: 0.01,
						Lon: 0,
					}, {
						Lat: 0.03,
						Lon: 0,
					},
				},
				objectType: ownmap.ObjectTypeWay,
			},
			want: []*tagCollectionKeyType{
				{LatBucket: 0, LonBucket: 0, ObjectType: ownmap.ObjectTypeWay, TagKey: "place"},
				{LatBucket: 1, LonBucket: 0, ObjectType: ownmap.ObjectTypeWay, TagKey: "place"},
				{LatBucket: 2, LonBucket: 0, ObjectType: ownmap.ObjectTypeWay, TagKey: "place"},
				{LatBucket: 3, LonBucket: 0, ObjectType: ownmap.ObjectTypeWay, TagKey: "place"},
			},
		}, {
			name: "negative lat & lon gradient",
			args: args{
				tags: []*ownmap.OSMTag{
					{Key: "place"},
				},
				nodes: []*ownmap.Location{
					{
						Lat: 0,
						Lon: 0.01,
					}, {
						Lat: -0.03,
						Lon: -0.01,
					},
				},
				objectType: ownmap.ObjectTypeWay,
			},
			want: []*tagCollectionKeyType{
				{LatBucket: -3, LonBucket: -1, ObjectType: ownmap.ObjectTypeWay, TagKey: "place"},
				{LatBucket: -3, LonBucket: 0, ObjectType: ownmap.ObjectTypeWay, TagKey: "place"},
				{LatBucket: -3, LonBucket: 1, ObjectType: ownmap.ObjectTypeWay, TagKey: "place"},
				{LatBucket: -2, LonBucket: -1, ObjectType: ownmap.ObjectTypeWay, TagKey: "place"},
				{LatBucket: -2, LonBucket: 0, ObjectType: ownmap.ObjectTypeWay, TagKey: "place"},
				{LatBucket: -2, LonBucket: 1, ObjectType: ownmap.ObjectTypeWay, TagKey: "place"},
				{LatBucket: -1, LonBucket: -1, ObjectType: ownmap.ObjectTypeWay, TagKey: "place"},
				{LatBucket: -1, LonBucket: 0, ObjectType: ownmap.ObjectTypeWay, TagKey: "place"},
				{LatBucket: -1, LonBucket: 1, ObjectType: ownmap.ObjectTypeWay, TagKey: "place"},
				{LatBucket: 0, LonBucket: -1, ObjectType: ownmap.ObjectTypeWay, TagKey: "place"},
				{LatBucket: 0, LonBucket: 0, ObjectType: ownmap.ObjectTypeWay, TagKey: "place"},
				{LatBucket: 0, LonBucket: 1, ObjectType: ownmap.ObjectTypeWay, TagKey: "place"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := buildTagIndexesForObject(tt.args.tags, tt.args.nodes, tt.args.objectType); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getTagIndexesForWay() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestImporter_getTagIndexesForRelation(t *testing.T) {
	c := &Collections{
		RelationCollection: &diskfilemap.MockOnDiskCollection{},
		WayCollection: &diskfilemap.MockOnDiskCollection{
			GetFunc: func(key []byte) ([]byte, error) {
				id := binary.LittleEndian.Uint64(key)
				switch id {
				case 2:
					return proto.Marshal(&ownmap.OSMWay{
						ID: int64(id),
						Tags: []*ownmap.OSMTag{
							{Key: "place", Value: "city"},
						},
						WayPoints: []*ownmap.WayPoint{
							{Point: &ownmap.Location{Lat: 1, Lon: 1}},
						},
					})
				case 3:
					return proto.Marshal(&ownmap.OSMWay{
						ID: int64(id),
						Tags: []*ownmap.OSMTag{
							{Key: "place", Value: "city"},
						},
						WayPoints: []*ownmap.WayPoint{
							{Point: &ownmap.Location{Lat: 1.001, Lon: 1.002}},
						},
					})
				}

				return nil, errorsx.ObjectNotFound
			},
		},
	}

	type fields struct {
		logger                   *logpkg.Logger
		fs                       gofs.Fs
		workDir                  string
		outFilePath              string
		ownmapDBFileHandlerLimit uint
		collections              *Collections
		pbfHeader                *osmpbf.Header
		options                  ImportOptions
	}
	type args struct {
		relation *ownmap.OSMRelation
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   tagCollectionKeySet
		want1  errorsx.Error
	}{
		{
			name: "relation",
			fields: fields{
				collections: c,
			},
			args: args{
				relation: &ownmap.OSMRelation{
					ID:   1,
					Tags: []*ownmap.OSMTag{{Key: "place", Value: "city"}},
					Members: []*ownmap.OSMRelationMember{
						{
							ObjectID:   2,
							MemberType: ownmap.OSM_MEMBER_TYPE_WAY,
							Role:       "outer",
						}, {
							ObjectID:   3,
							MemberType: ownmap.OSM_MEMBER_TYPE_WAY,
							Role:       "inner",
						},
					},
				},
			},
			want: tagCollectionKeySet{
				tagCollectionKeyType{
					LatBucket:  100,
					LonBucket:  100,
					ObjectType: ownmap.ObjectTypeRelation,
					TagKey:     "place",
				}: struct{}{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			importer := &Importer{
				logger:                   tt.fields.logger,
				fs:                       tt.fields.fs,
				workDir:                  tt.fields.workDir,
				outFilePath:              tt.fields.outFilePath,
				ownmapDBFileHandlerLimit: tt.fields.ownmapDBFileHandlerLimit,
				collections:              tt.fields.collections,
				pbfHeader:                tt.fields.pbfHeader,
				options:                  tt.fields.options,
			}
			got, got1 := importer.getTagIndexesForRelation(tt.args.relation)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Importer.getTagIndexesForRelation() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("Importer.getTagIndexesForRelation() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}
