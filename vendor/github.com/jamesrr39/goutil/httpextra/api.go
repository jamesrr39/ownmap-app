package httpextra

import (
	"bytes"
	"encoding/json"
	"io"

	"github.com/jamesrr39/goutil/errorsx"
)

type DataResponse struct {
	Data interface{} `json:"data"`
}

func DecodeJSONDataResponse(reader io.Reader, dest interface{}) errorsx.Error {
	type wrapperType struct {
		Data json.RawMessage `json:"data"`
	}

	d := new(wrapperType)

	err := json.NewDecoder(reader).Decode(&d)
	if err != nil {
		return errorsx.Wrap(err)
	}

	err = json.NewDecoder(bytes.NewBuffer(d.Data)).Decode(&dest)
	if err != nil {
		return errorsx.Wrap(err)
	}

	return nil
}
