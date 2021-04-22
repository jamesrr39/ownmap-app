package styling

import (
	"testing"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const rawTestSheet = `Map {
	background-color: @land-color;
}
// another comment

@water-color: #aad3df;
@land-color: #f2efe9;


/* For the main linear features, such as roads and railways. */

@tertiary-fill: #ffffff;
@residential-fill: #ffffff;
@service-fill: @residential-fill;
@living-street-fill: #ededed;
@pedestrian-fill: #dddde8;
@raceway-fill: pink;

`

func TestParse(t *testing.T) {
	// TODO: reinstate test when we support cartocss
	t.Skip()

	type args struct {
		stylesheet string
	}
	tests := []struct {
		name  string
		args  args
		want  *Style
		want1 errorsx.Error
	}{
		{
			name: "simple 1 statement stylesheet",
			args: args{
				stylesheet: `Map {
					background-color: @land-color;
				}`,
			},
			want: &Style{
				variables: make(map[string]string),
			},
		},
		{
			name: "variables",
			args: args{
				rawTestSheet,
			},
			want: &Style{
				variables: map[string]string{
					"pedestrian-fill":    "#dddde8",
					"raceway-fill":       "pink",
					"tertiary-fill":      "#ffffff",
					"residential-fill":   "#ffffff",
					"service-fill":       "@residential-fill",
					"living-street-fill": "#ededed",
					"water-color":        "#aad3df",
					"land-color":         "#f2efe9",
				},
			},
		},
		{
			name: "roads",
			args: args{
				stylesheet: `
#roads-low-zoom[zoom < 10],
#roads-fill[zoom >= 10],
#bridges[zoom >= 10],
#tunnels[zoom >= 10] {
	::fill {
		[feature = 'highway_primary'] {
			[zoom >= 8][link != 'yes'],
			[zoom >= 10] {
				line-width: @primary-width-z8;
				line-color: @primary-low-zoom;
				[zoom >= 9] { line-width: @primary-width-z9; }
				[zoom >= 10] { line-width: @primary-width-z10; }
				[zoom >= 11] { line-width: @primary-width-z11; }
				[zoom >= 12] {
					line-color: @primary-fill;
					line-width: @primary-width-z12 - 2 * @major-casing-width-z12;
					[zoom >= 13] { line-width: @primary-width-z13 - 2 * @major-casing-width-z13; }
					[zoom >= 15] { line-width: @primary-width-z15 - 2 * @major-casing-width-z15; }
					[zoom >= 17] { line-width: @primary-width-z17 - 2 * @major-casing-width-z17; }
					[zoom >= 18] { line-width: @primary-width-z18 - 2 * @major-casing-width-z18; }
					[zoom >= 19] { line-width: @primary-width-z19 - 2 * @major-casing-width-z19; }
					[link = 'yes'] {
						line-width: @primary-link-width-z12 - 2 * @casing-width-z12;
						[zoom >= 13] { line-width: @primary-link-width-z13 - 2 * @casing-width-z13; }
						[zoom >= 15] { line-width: @primary-link-width-z15 - 2 * @casing-width-z15; }
						[zoom >= 17] { line-width: @primary-link-width-z17 - 2 * @casing-width-z17; }
						[zoom >= 18] { line-width: @primary-link-width-z18 - 2 * @casing-width-z18; }
						[zoom >= 19] { line-width: @primary-link-width-z19 - 2 * @casing-width-z19; }
					}
				}
			}
		}
	}
}
				`,
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			style, err := Parse(tt.args.stylesheet)
			require.NoError(t, err)
			assert.Equal(t, tt.want, style)
		})
	}
}
