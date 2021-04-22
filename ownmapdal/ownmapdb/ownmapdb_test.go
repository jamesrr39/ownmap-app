package ownmapdb

import (
	"reflect"
	"testing"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/ownmap-app/ownmap"
)

func Test_getBlockIndexesForKeys(t *testing.T) {
	section0 := &tagCollectionKeyType{
		LatBucket:  800,
		LonBucket:  1000,
		ObjectType: ownmap.ObjectTypeNode,
		TagKey:     "place",
	}
	section1 := &tagCollectionKeyType{
		LatBucket:  900,
		LonBucket:  1000,
		ObjectType: ownmap.ObjectTypeNode,
		TagKey:     "place",
	}
	section2 := &tagCollectionKeyType{
		LatBucket:  1000,
		LonBucket:  1000,
		ObjectType: ownmap.ObjectTypeNode,
		TagKey:     "place",
	}
	section3 := &tagCollectionKeyType{
		LatBucket:  1100,
		LonBucket:  1000,
		ObjectType: ownmap.ObjectTypeNode,
		TagKey:     "place",
	}
	section4 := &tagCollectionKeyType{
		LatBucket:  1200,
		LonBucket:  1000,
		ObjectType: ownmap.ObjectTypeNode,
		TagKey:     "place",
	}

	searchForItem1TagCollectionKey := &tagCollectionKeyType{
		LatBucket:  1150,
		LonBucket:  1000,
		ObjectType: ownmap.ObjectTypeNode,
		TagKey:     "place",
	}

	type args struct {
		sectionMetadata *SectionMetadata
		keys            []KeyType
		emptyKey        KeyType
	}
	tests := []struct {
		name  string
		args  args
		want  blockIdxKeyMapType
		want1 errorsx.Error
	}{
		{
			name: "4th bucket",
			args: args{
				sectionMetadata: &SectionMetadata{
					BlockMetadatas: []*BlockMetadata{
						{LastItemInBlockValue: section0.MarshalKey()},
						{LastItemInBlockValue: section1.MarshalKey()},
						{LastItemInBlockValue: section2.MarshalKey()},
						{LastItemInBlockValue: section3.MarshalKey()},
						{LastItemInBlockValue: section4.MarshalKey()},
					},
				},
				keys: []KeyType{
					searchForItem1TagCollectionKey,
				},
				emptyKey: new(tagCollectionKeyType),
			},
			want: map[int][]KeyType{
				4: {searchForItem1TagCollectionKey},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := getBlockIndexesForKeys(tt.args.sectionMetadata, tt.args.keys, tt.args.emptyKey)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getBlockIndexesForKeys() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("getBlockIndexesForKeys() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}
