package fonts

import (
	_ "embed"

	"github.com/golang/freetype"
	"github.com/golang/freetype/truetype"
	"github.com/jamesrr39/goutil/errorsx"
)

//go:embed Roboto-Regular.ttf
var robotoFontBytes []byte

var robotoFont *truetype.Font

func init() {
	font, err := loadRoboto()
	if err != nil {
		panic(err)
	}

	robotoFont = font
}

func loadRoboto() (*truetype.Font, errorsx.Error) {
	font, err := freetype.ParseFont(robotoFontBytes)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	return font, nil
}

func DefaultFont() *truetype.Font {
	return robotoFont
}
