package parquetqueryengine

import (
	"bytes"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"runtime"
	"sort"
	"testing"

	"github.com/jamesrr39/goutil/errorsx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
)

func Test_Run(t *testing.T) {
	ensureTestFileErr := ensureTestFile()
	require.NoError(t, ensureTestFileErr)

	f, err := local.NewLocalFileReader(testFilename)
	require.NoError(t, err)
	defer f.Close()

	parquetReader, err := reader.NewParquetReader(f, osmNodesSchema, int64(runtime.NumCPU()))
	require.NoError(t, err)

	query := &Query{
		Select: []string{"Id", "Lat", "Lon"},
		Where: &LogicalFilter{
			Operator: LogicalFilterOperatorAnd,
			ChildFilters: []Filter{
				&ComparativeFilter{
					FieldName: "Id",
					Operator:  ComparativeOperatorLessThan,
					Operand:   Int64Operand(5),
				},
				// &ComparativeFilter{
				// 	FieldName: "Lat",
				// 	Operator:  ComparativeOperatorLessThan,
				// 	Operand:   Float64Operand(-10.1),
				// },
			},
		},
	}

	expectedResults := []*ResultRow{
		{int64(1), float64(10), float64(-10)},
		{int64(2), float64(10.1), float64(-10.1)},
		{int64(3), float64(10.2), float64(-10.2)},
		{int64(4), float64(10.3), float64(-10.3)},
	}

	results, runErr := query.Run(parquetReader, "Parquet_go_root")
	require.NoError(t, runErr)

	// sort results for deterministic result set
	sort.Slice(results, func(i, j int) bool {
		// [0] field is Id field
		iResult := *results[i]
		jResult := *results[j]

		return iResult[0].(int64) < jResult[0].(int64)

	})
	assert.Equal(t, expectedResults, results)
}

const (
	testFilename       = "testdata.parquet"
	testFileSHA512Hash = "0319181ae009933fa5ca19aef4a629f08ca8628351246ce2e015b6c009b4a957e13b3a13f901ef699c16785cdf24b68e3df03b6e9befbef15de65118a602cec0"
)

type tagType struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type testNodeType struct {
	ID   int64     `json:"id"`
	Lat  float64   `json:"lat"`
	Lon  float64   `json:"lon"`
	Tags []tagType `json:"tags"`
}

func isExpectedSHA512Hash(expected string, reader io.Reader) (bool, errorsx.Error) {
	expectedHash, err := hex.DecodeString(expected)
	if err != nil {
		return false, errorsx.Wrap(err)
	}

	h := sha512.New()
	testFileBytes, err := io.ReadAll(reader)
	if err != nil {
		return false, errorsx.Wrap(err)
	}
	_, err = h.Write(testFileBytes)
	if err != nil {
		return false, errorsx.Wrap(err)
	}
	hash := h.Sum(nil)
	if err != nil {
		return false, errorsx.Wrap(err)
	}

	if bytes.Equal(hash, expectedHash) {
		// file already exists and has the expected SHA-512 hash, no need to write a new one
		return true, nil
	}

	return false, nil
}

func ensureTestFile() errorsx.Error {
	existingFile, err := os.Open(testFilename)
	if err == nil {
		var err error
		// file already exists
		defer existingFile.Close()

		isExpected, err := isExpectedSHA512Hash(testFileSHA512Hash, existingFile)
		if err != nil {
			return errorsx.Wrap(err)
		}

		if isExpected {
			// 	// file already exists and has the expected SHA-512 hash, no need to write a new one
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

	w, err := writer.NewJSONWriter(osmNodesSchema, f, int64(runtime.NumCPU()))
	if err != nil {
		return errorsx.Wrap(err)
	}

	w.RowGroupSize = 128
	w.PageSize = 64
	w.CompressionType = parquet.CompressionCodec_SNAPPY
	for _, testNode := range testNodes {
		testNodeJSON, err := json.Marshal(testNode)
		if err != nil {
			return errorsx.Wrap(err)
		}

		err = w.Write(string(testNodeJSON))
		if err != nil {
			return errorsx.Wrap(err)
		}
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

	isExpectedHash, err := isExpectedSHA512Hash(testFileSHA512Hash, newFile)
	if err != nil {
		return errorsx.Wrap(err)
	}

	if !isExpectedHash {
		return errorsx.Errorf("got different hash to expected for file %q", testFilename)
	}

	return nil
}

const osmNodesSchema = `
{
	"Tag": "name=parquet_go_root",
	"Fields": [
	  {
		"Tag": "name=id, type=INT64"
	  },
	  {
		"Tag": "name=tags, type=LIST",
		"Fields": [
		  {
			"Tag": "name=element",
			"Fields": [
			  {
				"Tag": "name=key, type=BYTE_ARRAY, convertedtype=UTF8"
			  },
			  {
				"Tag": "name=value, type=BYTE_ARRAY, convertedtype=UTF8"
			  }
			]
		  }
		]
	  },
	  {
		"Tag": "name=lat, type=DOUBLE"
	  },
	  {
		"Tag": "name=lon, type=DOUBLE"
	  }
	]
  }`

var testNodes = []testNodeType{
	{ID: 1, Lat: 10, Lon: -10},
	{ID: 2, Lat: 10.1, Lon: -10.1},
	{ID: 3, Lat: 10.2, Lon: -10.2},
	{ID: 4, Lat: 10.3, Lon: -10.3},
	{ID: 5, Lat: 10.4, Lon: -10.4},
	{ID: 6, Lat: 10.5, Lon: -10.5},
	{ID: 7, Lat: 10.6, Lon: -10.6},
	{ID: 8, Lat: 10.7, Lon: -10.7},
	{ID: 9, Lat: 10.8, Lon: -10.8},
	{ID: 10, Lat: 10.9, Lon: -10.9},
	{ID: 11, Lat: 11, Lon: -11},
	{ID: 12, Lat: 11.1, Lon: -11.1},
	{ID: 13, Lat: 11.2, Lon: -11.2},
	{ID: 14, Lat: 11.3, Lon: -11.3},
	{ID: 15, Lat: 11.4, Lon: -11.4},
	{ID: 16, Lat: 11.5, Lon: -11.5},
	{ID: 17, Lat: 11.6, Lon: -11.6},
	{ID: 18, Lat: 11.7, Lon: -11.7},
	{ID: 19, Lat: 11.8, Lon: -11.8},
	{ID: 20, Lat: 11.9, Lon: -11.9},
	{ID: 21, Lat: 12, Lon: -12},
	{ID: 22, Lat: 12.1, Lon: -12.1},
	{ID: 23, Lat: 12.2, Lon: -12.2},
	{ID: 24, Lat: 12.3, Lon: -12.3},
	{ID: 25, Lat: 12.4, Lon: -12.4},
	{ID: 26, Lat: 12.5, Lon: -12.5},
	{ID: 27, Lat: 12.6, Lon: -12.6},
	{ID: 28, Lat: 12.7, Lon: -12.7},
	{ID: 29, Lat: 12.8, Lon: -12.8},
	{ID: 30, Lat: 12.9, Lon: -12.9},
	{ID: 31, Lat: 13, Lon: -13},
	{ID: 32, Lat: 13.1, Lon: -13.1},
	{ID: 33, Lat: 13.2, Lon: -13.2},
	{ID: 34, Lat: 13.3, Lon: -13.3},
	{ID: 35, Lat: 13.4, Lon: -13.4},
	{ID: 36, Lat: 13.5, Lon: -13.5},
	{ID: 37, Lat: 13.6, Lon: -13.6},
	{ID: 38, Lat: 13.7, Lon: -13.7},
	{ID: 39, Lat: 13.8, Lon: -13.8},
	{ID: 40, Lat: 13.9, Lon: -13.9},
	{ID: 41, Lat: 14, Lon: -14},
	{ID: 42, Lat: 14.1, Lon: -14.1},
	{ID: 43, Lat: 14.2, Lon: -14.2},
	{ID: 44, Lat: 14.3, Lon: -14.3},
	{ID: 45, Lat: 14.4, Lon: -14.4},
	{ID: 46, Lat: 14.5, Lon: -14.5},
	{ID: 47, Lat: 14.6, Lon: -14.6},
	{ID: 48, Lat: 14.7, Lon: -14.7},
	{ID: 49, Lat: 14.8, Lon: -14.8},
	{ID: 50, Lat: 14.9, Lon: -14.9},
	{ID: 51, Lat: 15, Lon: -15},
	{ID: 52, Lat: 15.1, Lon: -15.1},
	{ID: 53, Lat: 15.2, Lon: -15.2},
	{ID: 54, Lat: 15.3, Lon: -15.3},
	{ID: 55, Lat: 15.4, Lon: -15.4},
	{ID: 56, Lat: 15.5, Lon: -15.5},
	{ID: 57, Lat: 15.6, Lon: -15.6},
	{ID: 58, Lat: 15.7, Lon: -15.7},
	{ID: 59, Lat: 15.8, Lon: -15.8},
	{ID: 60, Lat: 15.9, Lon: -15.9},
	{ID: 61, Lat: 16, Lon: -16},
	{ID: 62, Lat: 16.1, Lon: -16.1},
	{ID: 63, Lat: 16.2, Lon: -16.2},
	{ID: 64, Lat: 16.3, Lon: -16.3},
	{ID: 65, Lat: 16.4, Lon: -16.4},
	{ID: 66, Lat: 16.5, Lon: -16.5},
	{ID: 67, Lat: 16.6, Lon: -16.6},
	{ID: 68, Lat: 16.7, Lon: -16.7},
	{ID: 69, Lat: 16.8, Lon: -16.8},
	{ID: 70, Lat: 16.9, Lon: -16.9},
	{ID: 71, Lat: 17, Lon: -17},
	{ID: 72, Lat: 17.1, Lon: -17.1},
	{ID: 73, Lat: 17.2, Lon: -17.2},
	{ID: 74, Lat: 17.3, Lon: -17.3},
	{ID: 75, Lat: 17.4, Lon: -17.4},
	{ID: 76, Lat: 17.5, Lon: -17.5},
	{ID: 77, Lat: 17.6, Lon: -17.6},
	{ID: 78, Lat: 17.7, Lon: -17.7},
	{ID: 79, Lat: 17.8, Lon: -17.8},
	{ID: 80, Lat: 17.9, Lon: -17.9},
	{ID: 81, Lat: 18, Lon: -18},
	{ID: 82, Lat: 18.1, Lon: -18.1},
	{ID: 83, Lat: 18.2, Lon: -18.2},
	{ID: 84, Lat: 18.3, Lon: -18.3},
	{ID: 85, Lat: 18.4, Lon: -18.4},
	{ID: 86, Lat: 18.5, Lon: -18.5},
	{ID: 87, Lat: 18.6, Lon: -18.6},
	{ID: 88, Lat: 18.7, Lon: -18.7},
	{ID: 89, Lat: 18.8, Lon: -18.8},
	{ID: 90, Lat: 18.9, Lon: -18.9},
	{ID: 91, Lat: 19, Lon: -19},
	{ID: 92, Lat: 19.1, Lon: -19.1},
	{ID: 93, Lat: 19.2, Lon: -19.2},
	{ID: 94, Lat: 19.3, Lon: -19.3},
	{ID: 95, Lat: 19.4, Lon: -19.4},
	{ID: 96, Lat: 19.5, Lon: -19.5},
	{ID: 97, Lat: 19.6, Lon: -19.6},
	{ID: 98, Lat: 19.7, Lon: -19.7},
	{ID: 99, Lat: 19.8, Lon: -19.8},
	{ID: 100, Lat: 19.9, Lon: -19.9},
	{ID: 101, Lat: 20, Lon: -20},
	{ID: 102, Lat: 20.1, Lon: -20.1},
	{ID: 103, Lat: 20.2, Lon: -20.2},
	{ID: 104, Lat: 20.3, Lon: -20.3},
	{ID: 105, Lat: 20.4, Lon: -20.4},
	{ID: 106, Lat: 20.5, Lon: -20.5},
	{ID: 107, Lat: 20.6, Lon: -20.6},
	{ID: 108, Lat: 20.7, Lon: -20.7},
	{ID: 109, Lat: 20.8, Lon: -20.8},
	{ID: 110, Lat: 20.9, Lon: -20.9},
	{ID: 111, Lat: 21, Lon: -21},
	{ID: 112, Lat: 21.1000000000001, Lon: -21.1000000000001},
	{ID: 113, Lat: 21.2000000000001, Lon: -21.2000000000001},
	{ID: 114, Lat: 21.3000000000001, Lon: -21.3000000000001},
	{ID: 115, Lat: 21.4000000000001, Lon: -21.4000000000001},
	{ID: 116, Lat: 21.5000000000001, Lon: -21.5000000000001},
	{ID: 117, Lat: 21.6000000000001, Lon: -21.6000000000001},
	{ID: 118, Lat: 21.7000000000001, Lon: -21.7000000000001},
	{ID: 119, Lat: 21.8000000000001, Lon: -21.8000000000001},
	{ID: 120, Lat: 21.9000000000001, Lon: -21.9000000000001},
	{ID: 121, Lat: 22.0000000000001, Lon: -22.0000000000001},
	{ID: 122, Lat: 22.1000000000001, Lon: -22.1000000000001},
	{ID: 123, Lat: 22.2000000000001, Lon: -22.2000000000001},
	{ID: 124, Lat: 22.3000000000001, Lon: -22.3000000000001},
	{ID: 125, Lat: 22.4000000000001, Lon: -22.4000000000001},
	{ID: 126, Lat: 22.5000000000001, Lon: -22.5000000000001},
	{ID: 127, Lat: 22.6000000000001, Lon: -22.6000000000001},
	{ID: 128, Lat: 22.7000000000001, Lon: -22.7000000000001},
	{ID: 129, Lat: 22.8000000000001, Lon: -22.8000000000001},
	{ID: 130, Lat: 22.9000000000001, Lon: -22.9000000000001},
	{ID: 131, Lat: 23.0000000000001, Lon: -23.0000000000001},
	{ID: 132, Lat: 23.1000000000001, Lon: -23.1000000000001},
	{ID: 133, Lat: 23.2000000000001, Lon: -23.2000000000001},
	{ID: 134, Lat: 23.3000000000001, Lon: -23.3000000000001},
	{ID: 135, Lat: 23.4000000000001, Lon: -23.4000000000001},
	{ID: 136, Lat: 23.5000000000001, Lon: -23.5000000000001},
	{ID: 137, Lat: 23.6000000000001, Lon: -23.6000000000001},
	{ID: 138, Lat: 23.7000000000001, Lon: -23.7000000000001},
	{ID: 139, Lat: 23.8000000000001, Lon: -23.8000000000001},
	{ID: 140, Lat: 23.9000000000001, Lon: -23.9000000000001},
	{ID: 141, Lat: 24.0000000000001, Lon: -24.0000000000001},
	{ID: 142, Lat: 24.1000000000001, Lon: -24.1000000000001},
	{ID: 143, Lat: 24.2000000000001, Lon: -24.2000000000001},
	{ID: 144, Lat: 24.3000000000001, Lon: -24.3000000000001},
	{ID: 145, Lat: 24.4000000000001, Lon: -24.4000000000001},
	{ID: 146, Lat: 24.5000000000001, Lon: -24.5000000000001},
	{ID: 147, Lat: 24.6000000000001, Lon: -24.6000000000001},
	{ID: 148, Lat: 24.7000000000001, Lon: -24.7000000000001},
	{ID: 149, Lat: 24.8000000000001, Lon: -24.8000000000001},
	{ID: 150, Lat: 24.9000000000001, Lon: -24.9000000000001},
}
