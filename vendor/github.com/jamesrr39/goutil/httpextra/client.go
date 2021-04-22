package httpextra

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
)

// Doer is implemented by http.DefaultClient
type Doer interface {
	Do(req *http.Request) (*http.Response, error)
}

type MockDoer struct {
	DoFunc func(req *http.Request) (*http.Response, error)
}

func (d *MockDoer) Do(req *http.Request) (*http.Response, error) {
	return d.DoFunc(req)
}

func CheckResponseCode(expected, got int) error {
	if got != expected {
		return fmt.Errorf("expected response code %d but got %d", expected, got)
	}

	return nil
}

// GetBodyOrErrorMsg returns the response body, or a string with the error message when getting the response body. It's useful for logging information, when receiving an error.
func GetBodyOrErrorMsg(resp *http.Response) string {
	r, err := RemoveGzip(resp)
	if err != nil {
		return fmt.Sprintf("ERROR: couldn't remove gzip. Error: %q", err)
	}
	defer r.Close()

	b, err := ioutil.ReadAll(r)
	if err != nil {
		return fmt.Sprintf("ERROR: couldn't read reader. Error: %q", err)
	}

	return string(b)
}

func RemoveGzip(resp *http.Response) (io.ReadCloser, error) {
	contentEncoding := resp.Header.Get("content-encoding")

	switch strings.ToLower(contentEncoding) {
	case "":
		// no compression
		return resp.Body, nil
	case "gzip":
		r, err := gzip.NewReader(resp.Body)
		if err != nil {
			return nil, err
		}

		bb := bytes.NewBuffer(nil)
		_, err = io.Copy(bb, r)
		if err != nil {
			return nil, err
		}

		err = r.Close()
		if err != nil {
			return nil, err
		}

		return ioutil.NopCloser(bb), nil
	}

	return nil, fmt.Errorf("unsupported content-encoding: %q", contentEncoding)
}
