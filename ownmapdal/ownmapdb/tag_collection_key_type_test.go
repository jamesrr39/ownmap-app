package ownmapdb

import (
	"testing"

	"github.com/jamesrr39/ownmap-app/ownmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_bucketFromLatOrLon(t *testing.T) {
	type args struct {
		value float64
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "small positive",
			args: args{
				0.005,
			},
			want: 0,
		}, {
			name: "positive",
			args: args{
				0.01,
			},
			want: 1,
		}, {
			name: "small negative",
			args: args{
				-0.005,
			},
			want: -1,
		}, {
			name: "large negative",
			args: args{
				-0.77,
			},
			want: -77,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := bucketFromLatOrLon(tt.args.value); got != tt.want {
				t.Errorf("bucketFromLatOrLon() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_bytesToSignedInt_signedIntTo4Bytes(t *testing.T) {
	assert.Equal(t, 4, bytesToSignedInt(signedIntTo4Bytes(4)))
	assert.Equal(t, -4, bytesToSignedInt(signedIntTo4Bytes(-4)))
	assert.Equal(t, 18000, bytesToSignedInt(signedIntTo4Bytes(18000)))
	assert.Equal(t, -18000, bytesToSignedInt(signedIntTo4Bytes(-18000)))
	assert.Equal(t, 0, bytesToSignedInt(signedIntTo4Bytes(0)))
}

func Test_tagCollectionKeyType_IsLargerThan(t *testing.T) {
	zeroValueType := &tagCollectionKeyType{}

	zeroZeroCoord := &tagCollectionKeyType{
		ObjectType: ownmap.ObjectTypeNode,
		TagKey:     "place",
	}

	type fields struct {
		LatBucket  int
		LonBucket  int
		ObjectType ownmap.ObjectType
		TagKey     string
	}
	type args struct {
		other KeyType
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   ComparisonResult
	}{
		{
			name: "zero value",
			fields: fields{
				LatBucket:  -90.00,
				LonBucket:  -180.00,
				ObjectType: ownmap.ObjectTypeNode,
				TagKey:     "place",
			},
			args: args{
				other: zeroValueType.LowerThanLowestValidValue(),
			},
			want: ComparisonResultAGreaterThanB,
		}, {
			name: "smaller than other",
			fields: fields{
				LatBucket:  -90.00,
				LonBucket:  -180.00,
				ObjectType: ownmap.ObjectTypeNode,
				TagKey:     "place",
			},
			args: args{
				other: zeroZeroCoord,
			},
			want: ComparisonResultALessThanB,
		}, {
			name: "greater than",
			fields: fields{
				LatBucket:  90.00,
				LonBucket:  -180.00,
				ObjectType: ownmap.ObjectTypeNode,
				TagKey:     "place",
			},
			args: args{
				other: zeroZeroCoord,
			},
			want: ComparisonResultAGreaterThanB,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tckt := &tagCollectionKeyType{
				LatBucket:  tt.fields.LatBucket,
				LonBucket:  tt.fields.LonBucket,
				ObjectType: tt.fields.ObjectType,
				TagKey:     tt.fields.TagKey,
			}
			assert.Equal(t, tt.want, tckt.Compare(tt.args.other))
		})
	}
}
func Test_tagCollectionKeyType_MarshalKey_UnmarshalKey(t *testing.T) {
	tckts := []*tagCollectionKeyType{
		{LatBucket: 90, LonBucket: 180, ObjectType: ownmap.ObjectTypeNode, TagKey: "place"},
		{LatBucket: 0, LonBucket: 0, ObjectType: ownmap.ObjectTypeWay, TagKey: "waterway"},
		{LatBucket: -90, LonBucket: -180, ObjectType: ownmap.ObjectTypeWay, TagKey: "highway"},
	}

	for _, tckt := range tckts {
		t.Run(tckt.String(), func(t *testing.T) {
			b := tckt.MarshalKey()
			newTckt := new(tagCollectionKeyType)
			err := newTckt.UnmarshalKey(b)
			require.NoError(t, err)

			assert.Equal(t, tckt, newTckt)
		})
	}

}

func TestIsTagCollectionKey1BytesLargerThanKey2(t *testing.T) {
	type args struct {
		t1 []byte
		t2 []byte
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			"larger by lat bucket",
			args{
				(&tagCollectionKeyType{LatBucket: 10}).MarshalKey(),
				(&tagCollectionKeyType{LatBucket: 9}).MarshalKey(),
			},
			true,
		}, {
			"smaller by lat bucket",
			args{
				(&tagCollectionKeyType{LatBucket: -10}).MarshalKey(),
				(&tagCollectionKeyType{LatBucket: 9}).MarshalKey(),
			},
			false,
		}, {
			"larger by lon bucket",
			args{
				(&tagCollectionKeyType{LatBucket: -10}).MarshalKey(),
				(&tagCollectionKeyType{LatBucket: 9}).MarshalKey(),
			},
			false,
		}, {
			"smaller by lon bucket",
			args{
				(&tagCollectionKeyType{LonBucket: -10}).MarshalKey(),
				(&tagCollectionKeyType{LonBucket: 9}).MarshalKey(),
			},
			false,
		}, {
			"larger by object type",
			args{
				(&tagCollectionKeyType{ObjectType: ownmap.ObjectTypeWay}).MarshalKey(),
				(&tagCollectionKeyType{ObjectType: ownmap.ObjectTypeNode}).MarshalKey(),
			},
			true,
		}, {
			"smaller by object type",
			args{
				(&tagCollectionKeyType{ObjectType: ownmap.ObjectTypeNode}).MarshalKey(),
				(&tagCollectionKeyType{ObjectType: ownmap.ObjectTypeWay}).MarshalKey(),
			},
			false,
		}, {
			"larger by tag key",
			args{
				(&tagCollectionKeyType{TagKey: "place"}).MarshalKey(),
				(&tagCollectionKeyType{TagKey: "Place"}).MarshalKey(),
			},
			true,
		}, {
			"smaller by tag key",
			args{
				(&tagCollectionKeyType{TagKey: "Place"}).MarshalKey(),
				(&tagCollectionKeyType{TagKey: "place"}).MarshalKey(),
			},
			false,
		}, {
			"same tag key",
			args{
				(&tagCollectionKeyType{TagKey: "place"}).MarshalKey(),
				(&tagCollectionKeyType{TagKey: "place"}).MarshalKey(),
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsTagCollectionKey1BytesLargerThanKey2(tt.args.t1, tt.args.t2); got != tt.want {
				t.Errorf("IsTagCollectionKey1BytesLargerThanKey2() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_tagCollectionKeyType_sortMatch(t *testing.T) {
	type testType struct {
		Key1, Key2 *tagCollectionKeyType
	}
	testCases := []testType{
		{
			Key1: &tagCollectionKeyType{LatBucket: 10},
			Key2: &tagCollectionKeyType{LatBucket: 9},
		}, {
			Key1: &tagCollectionKeyType{LatBucket: -10},
			Key2: &tagCollectionKeyType{LatBucket: 9},
		}, {
			Key1: &tagCollectionKeyType{LonBucket: 10},
			Key2: &tagCollectionKeyType{LonBucket: 9},
		}, {
			Key1: &tagCollectionKeyType{LonBucket: -10},
			Key2: &tagCollectionKeyType{LonBucket: 9},
		}, {
			Key1: &tagCollectionKeyType{ObjectType: ownmap.ObjectTypeNode},
			Key2: &tagCollectionKeyType{ObjectType: ownmap.ObjectTypeWay},
		}, {
			Key1: &tagCollectionKeyType{ObjectType: ownmap.ObjectTypeWay},
			Key2: &tagCollectionKeyType{ObjectType: ownmap.ObjectTypeNode},
		}, {
			Key1: &tagCollectionKeyType{TagKey: "place"},
			Key2: &tagCollectionKeyType{TagKey: "Place"},
		}, {
			Key1: &tagCollectionKeyType{TagKey: "place"},
			Key2: &tagCollectionKeyType{TagKey: "place"},
		}, {
			Key1: &tagCollectionKeyType{TagKey: "Place"},
			Key2: &tagCollectionKeyType{TagKey: "place"},
		},
	}

	for _, testCase := range testCases {
		assert.Equal(t,
			testCase.Key1.Compare(testCase.Key2) == ComparisonResultAGreaterThanB,
			IsTagCollectionKey1BytesLargerThanKey2(testCase.Key1.MarshalKey(), testCase.Key2.MarshalKey()),
		)
	}
}
