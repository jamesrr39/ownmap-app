package ownmap

import (
	"testing"

	"github.com/paulmach/osm"
)

// 1: item above container: false
// 2: item below container: false
// 3: item to the left of container: false
// 4: item to the right of container: false
// 5: item fully inside container: true
// 6: item paritially inside container (top side): true
// 7: item paritially inside container (bottom side): true
// 8: item paritially inside container (left side): true
// 9: item paritially inside container (right side): true
// 10: item paritially inside container (top-left side): true
// 11: item paritially inside container (top-right side): true
// 12: item paritially inside container (bottom-left side): true
// 13: item paritially inside container (bottom-right side): true
// 14: item == container: true
func TestOverlaps(t *testing.T) {
	containerBounds := osm.Bounds{
		MaxLat: 1,
		MinLat: -1,
		MaxLon: 1,
		MinLon: -1,
	}

	type args struct {
		container osm.Bounds
		item      osm.Bounds
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			"item above container",
			args{
				containerBounds,
				osm.Bounds{MaxLat: 90, MinLat: 89, MaxLon: 1, MinLon: -1},
			},
			false,
		},
		{
			"item below container",
			args{
				containerBounds,
				osm.Bounds{MaxLat: -50, MinLat: -51, MaxLon: 1, MinLon: -1},
			},
			false,
		},
		{
			"item to the left of container",
			args{
				containerBounds,
				osm.Bounds{MaxLat: 1, MinLat: -1, MaxLon: -2, MinLon: -3},
			},
			false,
		},
		{
			"item to the right of container",
			args{
				containerBounds,
				osm.Bounds{MaxLat: 1, MinLat: -1, MaxLon: 3, MinLon: 2},
			},
			false,
		}, {
			"item fully inside container",
			args{
				containerBounds,
				osm.Bounds{MaxLat: 0.5, MinLat: -0.5, MaxLon: 0.5, MinLon: -0.5},
			},
			true,
		}, {
			"item paritially inside container (top side)",
			args{
				containerBounds,
				osm.Bounds{MaxLat: 2, MinLat: 1, MaxLon: 0.8, MinLon: 0.2},
			},
			true,
		}, {
			"item paritially inside container (bottom side)",
			args{
				containerBounds,
				osm.Bounds{MaxLat: -1, MinLat: -2, MaxLon: 0.8, MinLon: 0.2},
			},
			true,
		}, {
			"item paritially inside container (left side)",
			args{
				containerBounds,
				osm.Bounds{MaxLat: 1, MinLat: -1, MaxLon: -1, MinLon: -2},
			},
			true,
		}, {
			"item paritially inside container (right side)",
			args{
				containerBounds,
				osm.Bounds{MaxLat: 1, MinLat: -1, MaxLon: 2, MinLon: -1},
			},
			true,
		},
		{
			"item paritially inside container (top-left side)",
			args{
				containerBounds,
				osm.Bounds{MaxLat: 1.5, MinLat: 0.5, MaxLon: -0.5, MinLon: -1.5},
			},
			true,
		},
		{
			"item paritially inside container (top-right side)",
			args{
				containerBounds,
				osm.Bounds{MaxLat: 1.5, MinLat: 0.5, MaxLon: 1.5, MinLon: 0.5},
			},
			true,
		},
		{
			"item paritially inside container (bottom-left side)",
			args{
				containerBounds,
				osm.Bounds{MaxLat: -0.5, MinLat: -1.5, MaxLon: -0.5, MinLon: -1.5},
			},
			true,
		},
		{
			"item paritially inside container (bottom-right side)",
			args{
				containerBounds,
				osm.Bounds{MaxLat: -0.5, MinLat: -1.5, MaxLon: 1.5, MinLon: 0.5},
			},
			true,
		},
		{
			"item == container",
			args{
				containerBounds,
				containerBounds,
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Overlaps(tt.args.container, tt.args.item); got != tt.want {
				t.Errorf("Overlaps() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsInBounds(t *testing.T) {
	type args struct {
		bounds   osm.Bounds
		pointLat float64
		pointLon float64
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			"is in bounds",
			args{
				bounds: osm.Bounds{
					MinLat: -1,
					MaxLat: 1,
					MinLon: -1,
					MaxLon: 1,
				},
				pointLat: 0.5,
				pointLon: -0.5,
			},
			true,
		}, {
			"is above bounds",
			args{
				bounds: osm.Bounds{
					MinLat: -1,
					MaxLat: 1,
					MinLon: -1,
					MaxLon: 1,
				},
				pointLat: 1.5,
				pointLon: -0.5,
			},
			false,
		}, {
			"is to the left of bounds",
			args{
				bounds: osm.Bounds{
					MinLat: -1,
					MaxLat: 1,
					MinLon: -1,
					MaxLon: 1,
				},
				pointLat: 0.5,
				pointLon: -1.5,
			},
			false,
		}, {
			"is below bounds",
			args{
				bounds: osm.Bounds{
					MinLat: -1,
					MaxLat: 1,
					MinLon: -1,
					MaxLon: 1,
				},
				pointLat: -1.5,
				pointLon: -0.5,
			},
			false,
		}, {
			"is to the right of bounds",
			args{
				bounds: osm.Bounds{
					MinLat: -1,
					MaxLat: 1,
					MinLon: -1,
					MaxLon: 1,
				},
				pointLat: 0.5,
				pointLon: 1.5,
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsInBounds(tt.args.bounds, tt.args.pointLat, tt.args.pointLon); got != tt.want {
				t.Errorf("IsInBounds() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsTotallyInside(t *testing.T) {
	container := osm.Bounds{
		MaxLat: 1,
		MinLat: -1,
		MaxLon: 1,
		MinLon: -1,
	}

	type args struct {
		container osm.Bounds
		item      osm.Bounds
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			"is totally inside",
			args{
				container: container,
				item: osm.Bounds{
					MaxLat: 0.5,
					MinLat: -0.5,
					MaxLon: 0.5,
					MinLon: -0.5,
				},
			},
			true,
		}, {
			"is the same as the container inside",
			args{
				container: container,
				item: osm.Bounds{
					MaxLat: 1,
					MinLat: -1,
					MaxLon: 1,
					MinLon: -1,
				},
			},
			true,
		}, {
			"is out to the west",
			args{
				container: container,
				item: osm.Bounds{
					MaxLat: 1,
					MinLat: -1,
					MaxLon: 1,
					MinLon: -1.1,
				},
			},
			false,
		}, {
			"is out to the east",
			args{
				container: container,
				item: osm.Bounds{
					MaxLat: 1,
					MinLat: -1,
					MaxLon: 1.1,
					MinLon: -1,
				},
			},
			false,
		}, {
			"is out to the north",
			args{
				container: container,
				item: osm.Bounds{
					MaxLat: 1.1,
					MinLat: -1,
					MaxLon: 1,
					MinLon: -1,
				},
			},
			false,
		}, {
			"is out to the south",
			args{
				container: container,
				item: osm.Bounds{
					MaxLat: 1,
					MinLat: -1,
					MaxLon: 1,
					MinLon: -1.1,
				},
			},
			false,
		}, {
			"is totally outside",
			args{
				container: container,
				item: osm.Bounds{
					MaxLat: 3,
					MinLat: 2,
					MaxLon: 3,
					MinLon: 2,
				},
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsTotallyInside(tt.args.container, tt.args.item); got != tt.want {
				t.Errorf("IsTotallyInside() = %v, want %v", got, tt.want)
			}
		})
	}
}
