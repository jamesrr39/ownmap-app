package ownmapdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_int64ItemType_IsLargerThan(t *testing.T) {
	zero := new(int64ItemType)
	*zero = 0

	ten := new(int64ItemType)
	*ten = 10

	twenty := new(int64ItemType)
	*twenty = 20

	type args struct {
		other KeyType
	}
	tests := []struct {
		name string
		t    *int64ItemType
		args args
		want ComparisonResult
	}{
		{
			name: "less than",
			t:    zero,
			args: args{
				other: ten,
			},
			want: ComparisonResultALessThanB,
		}, {
			name: "equal",
			t:    ten,
			args: args{
				other: ten,
			},
			want: ComparisonResultEqual,
		}, {
			name: "greater than 10",
			t:    twenty,
			args: args{
				other: ten,
			},
			want: ComparisonResultAGreaterThanB,
		}, {
			name: "greater than zero",
			t:    twenty,
			args: args{
				other: zero,
			},
			want: ComparisonResultAGreaterThanB,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.t.Compare(tt.args.other), tt.want)
		})
	}
}

func Test_int64ItemType_UnmarshalKey(t *testing.T) {
	item := new(int64ItemType)
	err := item.UnmarshalKey([]byte{10, 0, 0, 0, 0, 0, 0, 0})
	require.NoError(t, err)

	assert.Equal(t, int64ItemType(10), *item)
}
