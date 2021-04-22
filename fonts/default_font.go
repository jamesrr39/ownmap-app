package fonts

import (
	"bytes"
	"encoding/base64"
	"io/ioutil"

	"github.com/golang/freetype"
	"github.com/golang/freetype/truetype"
	"github.com/jamesrr39/goutil/errorsx"
)

var robotoFont *truetype.Font

func init() {
	font, err := loadRoboto()
	if err != nil {
		panic(err)
	}

	robotoFont = font
}

func loadRoboto() (*truetype.Font, errorsx.Error) {

	base64Reader := base64.NewDecoder(base64.StdEncoding, bytes.NewBuffer([]byte(robotoRegularBase64)))
	fontBytes, err := ioutil.ReadAll(base64Reader)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}
	font, err := freetype.ParseFont(fontBytes)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	return font, nil
}

func DefaultFont() *truetype.Font {
	return robotoFont
}
