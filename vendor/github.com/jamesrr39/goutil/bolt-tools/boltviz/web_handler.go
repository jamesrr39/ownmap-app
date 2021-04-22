package boltviz

import (
	"encoding/base64"
	"fmt"
	"html/template"
	"net/http"
	"strconv"
	"strings"

	"github.com/boltdb/bolt"
	"github.com/jamesrr39/goutil/errorsx"
)

type TemplateMap struct {
	PrintKey   func(pair KVPairDisplay) string
	PrintValue func(pair KVPairDisplay) string
}

func NewHandlerFunc(dbConn *bolt.DB, templateMap TemplateMap, uniqueID string) (http.HandlerFunc, errorsx.Error) {
	rootTmpl, err := template.New("boltviz_root_" + uniqueID).
		Funcs(template.FuncMap{
			"printKey":   templateMap.PrintKey,
			"printValue": templateMap.PrintValue,
		}).
		Parse(rootTmplStr)

	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	return func(w http.ResponseWriter, r *http.Request) {
		tx, err := dbConn.Begin(false)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		defer tx.Rollback()

		fragments := filterOutEmptyStrings(
			strings.Split(
				strings.TrimPrefix(r.URL.Path, "/"),
				"/",
			),
		)

		pageStr := r.URL.Query().Get("page")
		if pageStr == "" {
			pageStr = "1"
		}

		page, err := strconv.ParseUint(pageStr, 10, 64)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		renderer := &rendererType{
			PageNumber:      page,
			Req:             r,
			RespWriter:      w,
			Base64Fragments: fragments,
			rootTmpl:        rootTmpl,
		}

		renderBucket(tx, renderer)
	}, nil
}

type rendererType struct {
	PageNumber      uint64
	Req             *http.Request
	RespWriter      http.ResponseWriter
	Base64Fragments []string
	rootTmpl        *template.Template
}

type bucketData struct {
	Name       string
	Base64Name string
	Stats      bolt.BucketStats
}

type KVPairDisplay struct {
	Key           []byte
	Value         []byte
	PathFragments []string
}

type rootData struct {
	BucketName         string
	Buckets            []bucketData
	KVPairs            []KVPairDisplay
	NextPageNumber     uint64
	PreviousPageNumber uint64
	PageNumber         uint64
}

const pageSize = uint64(100)

func (renderer *rendererType) makeBucketData(cursor *bolt.Cursor, bucketName string) *rootData {
	data := &rootData{
		PageNumber: renderer.PageNumber,
		BucketName: bucketName,
	}

	if renderer.PageNumber != 1 {
		data.PreviousPageNumber = renderer.PageNumber - 1
	}

	firstItemIdx := pageSize * (renderer.PageNumber - 1)
	lastItemIdx := (pageSize * renderer.PageNumber) - 1

	itemsScanned := uint64(0)
	scanItem := func(k, v []byte) (exit bool) {
		defer func() { itemsScanned++ }()

		if k == nil && v == nil {
			// end of bucket
			return true
		}

		if itemsScanned < firstItemIdx {
			// haven't reached the items we are looking for yet, continue
			return false
		}

		if itemsScanned > lastItemIdx {
			data.NextPageNumber = renderer.PageNumber + 1
			return true
		}

		// item within the range
		addToData(renderer.Req, data, cursor, renderer.Base64Fragments, k, v)

		return false
	}

	k, v := cursor.First()
	exit := scanItem(k, v)
	if !exit {
		for {
			k, v = cursor.Next()
			exit = scanItem(k, v)
			if exit {
				break
			}
		}
	}

	return data
}

func renderBucket(tx *bolt.Tx, renderer *rendererType) {
	var bucketNames []string
	cursor := tx.Cursor()

	for _, fragment := range renderer.Base64Fragments {
		bucketHierarchy, err := base64.StdEncoding.DecodeString(fragment)
		if err != nil {
			renderError(renderer.RespWriter, err, http.StatusBadRequest)
			return
		}
		bucketNames = append(bucketNames, string(bucketHierarchy))

		bucket := cursor.Bucket().Bucket(bucketHierarchy)

		if bucket == nil {
			renderError(renderer.RespWriter, fmt.Errorf("bucket %q does not exist", fragment), http.StatusNotFound)
			return
		}

		cursor = bucket.Cursor()
	}

	bucketName := strings.Join(bucketNames, "::")

	data := renderer.makeBucketData(cursor, bucketName)

	err := renderer.rootTmpl.Execute(renderer.RespWriter, data)
	if err != nil {
		renderError(renderer.RespWriter, err, 500)
		return
	}
}

func renderError(w http.ResponseWriter, err error, code int) {
	http.Error(w, err.Error(), code)
}

const rootTmplStr = `
<html>
	<body>
		<h1>{{.BucketName}}</h1>
		{{range .Buckets}}
			<p>
				bucket: <a href="{{.Base64Name}}">{{.Name}}</a>
				{{with .Stats}}
					({{.KeyN}} items)
				{{end}}
			</p>
		{{end}}
		{{range .KVPairs}}
			<p>
				key: {{. | printKey}}
				value: {{. | printValue}}
			<p>
		{{end}}
		{{if .PreviousPageNumber}}
			<a href="?page={{.PreviousPageNumber}}">Previous page</a>
		{{end}}
		Page {{.PageNumber}}
		{{if .NextPageNumber}}
			<a href="?page={{.NextPageNumber}}">Next page</a>
		{{end}}
	</body>
</html>
`

func addToData(r *http.Request, data *rootData, cursor *bolt.Cursor, base64Fragments []string, k, v []byte) {
	if k == nil && v == nil {
		return
	}
	if v == nil {
		subBucket := cursor.Bucket().Bucket(k)

		base64Link := base64.StdEncoding.EncodeToString(k)
		if len(base64Fragments) != 0 && !strings.HasSuffix(r.URL.String(), "/") {
			lastFragment := base64Fragments[len(base64Fragments)-1]
			base64Link = strings.Join(
				[]string{lastFragment, base64Link},
				"/",
			)
		}

		bd := bucketData{string(k), base64Link, subBucket.Stats()}
		data.Buckets = append(data.Buckets, bd)
	} else {
		data.KVPairs = append(data.KVPairs, KVPairDisplay{
			Key:           k,
			Value:         v,
			PathFragments: base64Fragments,
		})
	}
}

func filterOutEmptyStrings(strs []string) []string {
	var filtered []string
	for _, str := range strs {
		if str == "" {
			continue
		}
		filtered = append(filtered, str)
	}
	return filtered
}
