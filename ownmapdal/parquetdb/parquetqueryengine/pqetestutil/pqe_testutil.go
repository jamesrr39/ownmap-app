package pqetestutil

import (
	"bytes"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
)

const (
	testFilename       = "testdata.parquet"
	testFileSHA512Hash = "71590854e2946770def7247c4eacd55a8b7d716a4118f2a4f969206e605085db923f9c7de190649dfd31e9f00c5718b2020e16adfe0a2899abf5b7055b647ff6"
)

type tagMapType map[string]string

type testNodeType struct {
	ID   int64      `json:"id"`
	Lat  float64    `json:"lat"`
	Lon  float64    `json:"lon"`
	Tags tagMapType `json:"tags"`
}

func EnsureTestFile() (source.ParquetFile, errorsx.Error) {
	var err error
	err = ensureOSFile()
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	f, err := local.NewLocalFileReader(testFilename)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	return f, nil
}

func ensureOSFile() errorsx.Error {
	existingFile, err := os.Open(testFilename)
	if err == nil {
		var err error
		// file already exists
		defer existingFile.Close()

		actual, err := hashFile(existingFile)
		if err != nil {
			return errorsx.Wrap(err)
		}

		expected, err := hex.DecodeString(testFileSHA512Hash)
		if err != nil {
			return errorsx.Wrap(err)
		}

		if bytes.Equal(expected, actual) {
			// file already exists as expected
			return nil
		}

		// file exists but is the wrong version, delete it so a new one can be created
		err = os.Remove(testFilename)
		if err != nil {
			return errorsx.Wrap(err)
		}
	}

	if err != nil && !os.IsNotExist(err) {
		return errorsx.Wrap(err)
	}

	// file does not exist (or has been deleted, for an updated file to take its place)

	f, err := local.NewLocalFileWriter(testFilename)
	if err != nil {
		return errorsx.Wrap(err)
	}
	defer f.Close()

	w, err := writer.NewJSONWriter(OsmNodesSchema, f, 1)
	if err != nil {
		return errorsx.Wrap(err)
	}

	w.RowGroupSize = 128
	w.PageSize = 64
	w.CompressionType = parquet.CompressionCodec_SNAPPY
	for i, testNode := range TestNodes {
		testNodeJSON, err := json.Marshal(testNode)
		if err != nil {
			println("marshal error", err)
			return errorsx.Wrap(err)
		}

		err = w.Write(string(testNodeJSON))
		if err != nil {
			println("write error", i, string(testNodeJSON), err.Error())
			return errorsx.Wrap(err)
		}

		println("written", i, string(testNodeJSON))
	}

	err = w.WriteStop()
	if err != nil {
		return errorsx.Wrap(err)
	}

	// check new file has correct hash

	newFile, err := os.Open(testFilename)
	if err != nil {
		return errorsx.Wrap(err)
	}
	defer newFile.Close()

	actual, err := hashFile(newFile)
	if err != nil {
		return errorsx.Wrap(err)
	}

	expected, err := hex.DecodeString(testFileSHA512Hash)
	if err != nil {
		return errorsx.Wrap(err)
	}

	if !bytes.Equal(expected, actual) {
		return errorsx.Errorf("got different hash to expected for file %q. Expected: %s, actual %s", testFilename, hex.EncodeToString(expected), hex.EncodeToString(actual))
	}

	return nil
}

func hashFile(reader io.Reader) ([]byte, errorsx.Error) {
	h := sha512.New()
	testFileBytes, err := io.ReadAll(reader)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}
	_, err = h.Write(testFileBytes)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}
	hash := h.Sum(nil)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	return hash, nil
}
