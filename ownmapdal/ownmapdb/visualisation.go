package ownmapdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/jamesrr39/goutil/errorsx"
)

const (
	tagIndexSectionName = "tagIndex"
	nodeSectionName     = "nodes"
	waySectionName      = "ways"
	relationSectionName = "relations"
)

var (
	visFuncs = template.FuncMap{
		"keyToStr": func(keyBytes []byte, keyTypeStr string) string {
			if len(keyBytes) == 0 {
				return "(no data)"
			}

			var key KeyType
			switch keyTypeStr {
			case tagIndexSectionName:
				key = new(tagCollectionKeyType)
				err := key.UnmarshalKey(keyBytes)
				if err != nil {
					panic(err)
				}
			case nodeSectionName, waySectionName, relationSectionName:
				key = new(int64ItemType)
				err := key.UnmarshalKey(keyBytes)
				if err != nil {
					panic(err)
				}
			default:
				return "unknown key type: " + keyTypeStr
			}
			return key.String()
		},
	}
	visTmpl               = template.Must(template.New("ownmap_db_visualisation").Funcs(visFuncs).Parse(visTmplStr))
	sectionOverviewTmpl   = template.Must(template.New("ownmap_db_visualisation_section_overview").Funcs(visFuncs).Parse(sectionOverviewTmplStr))
	sectionDataTmpl       = template.Must(template.New("ownmap_db_section_data_visualisation").Funcs(visFuncs).Parse(sectionDataTmplStr))
	sectionDataRegexp     = regexp.MustCompile(`/(?P<SectionName>\S+)/(?P<Index>\S+)`)
	sectionOverviewRegexp = regexp.MustCompile(`/(?P<SectionName>\S+)`)
)

type displayKVType struct {
	Key, Value string
}

type sectionType struct {
	SectionMetadata      *SectionMetadata
	onFoundBlockDataFunc onFoundBlockDataFuncType
	GetKVPairsFunc       func() []*displayKVType
	SectionStartOffset   int64
}

func sectionByName(db *MapmakerDBConn, name string) (*sectionType, errorsx.Error) {
	var kvPairs []*displayKVType
	switch name {
	case tagIndexSectionName:
		onFoundBlockDataFunc := func(blockDataBytes *bytes.Buffer, wantedKeys []KeyType) errorsx.Error {
			blockData := new(TagIndexBlockData)

			err := proto.Unmarshal(blockDataBytes.Bytes(), blockData)
			if err != nil {
				return errorsx.Wrap(err)
			}

			for _, record := range blockData.TagIndexRecords {
				recordKey := new(tagCollectionKeyType)
				err = recordKey.UnmarshalKey(record.IndexKey)
				if err != nil {
					return errorsx.Wrap(err)
				}

				var values []string
				for _, itemID := range record.ItemIDs {
					values = append(values, fmt.Sprintf("%d", itemID))
				}

				kvPairs = append(kvPairs, &displayKVType{
					Key:   recordKey.String(),
					Value: strings.Join(values, " "),
				})
			}
			return nil
		}

		return &sectionType{
			SectionMetadata:      db.header.TagIndexSectionMetadata,
			onFoundBlockDataFunc: onFoundBlockDataFunc,
			GetKVPairsFunc:       func() []*displayKVType { return kvPairs },
			SectionStartOffset:   db.offsetOfTagIndexSectionFromStartOfFile(),
		}, nil
	case nodeSectionName:
		onFoundBlockDataFunc := func(blockDataBytes *bytes.Buffer, wantedKeys []KeyType) errorsx.Error {
			blockData := new(NodesBlockData)

			err := proto.Unmarshal(blockDataBytes.Bytes(), blockData)
			if err != nil {
				return errorsx.Wrap(err)
			}

			for _, record := range blockData.Nodes {
				data, err := json.Marshal(record)
				if err != nil {
					return errorsx.Wrap(err)
				}
				kvPairs = append(kvPairs, &displayKVType{
					Key:   fmt.Sprintf("%d", record.ID),
					Value: string(data),
				})
			}
			return nil
		}

		return &sectionType{
			SectionMetadata:      db.header.NodesSectionMetadata,
			onFoundBlockDataFunc: onFoundBlockDataFunc,
			GetKVPairsFunc:       func() []*displayKVType { return kvPairs },
			SectionStartOffset:   db.offsetOfNodeSectionFromStartOfFile(),
		}, nil
	case waySectionName:
		onFoundBlockDataFunc := func(blockDataBytes *bytes.Buffer, wantedKeys []KeyType) errorsx.Error {
			blockData := new(WaysBlockData)

			err := proto.Unmarshal(blockDataBytes.Bytes(), blockData)
			if err != nil {
				return errorsx.Wrap(err)
			}

			for _, record := range blockData.Ways {
				data, err := json.Marshal(record)
				if err != nil {
					return errorsx.Wrap(err)
				}
				kvPairs = append(kvPairs, &displayKVType{
					Key:   fmt.Sprintf("%d", record.ID),
					Value: string(data),
				})
			}
			return nil
		}

		return &sectionType{
			SectionMetadata:      db.header.WaysSectionMetadata,
			onFoundBlockDataFunc: onFoundBlockDataFunc,
			GetKVPairsFunc:       func() []*displayKVType { return kvPairs },
			SectionStartOffset:   db.offsetOfWaySectionFromStartOfFile(),
		}, nil
	case relationSectionName:
		onFoundBlockDataFunc := func(blockDataBytes *bytes.Buffer, wantedKeys []KeyType) errorsx.Error {
			blockData := new(RelationsBlockData)

			err := proto.Unmarshal(blockDataBytes.Bytes(), blockData)
			if err != nil {
				return errorsx.Wrap(err)
			}

			for _, record := range blockData.Relations {
				data, err := json.Marshal(record)
				if err != nil {
					return errorsx.Wrap(err)
				}
				kvPairs = append(kvPairs, &displayKVType{
					Key:   fmt.Sprintf("%d", record.ID),
					Value: string(data),
				})
			}
			return nil
		}

		return &sectionType{
			SectionMetadata:      db.header.RelationsSectionMetadata,
			onFoundBlockDataFunc: onFoundBlockDataFunc,
			GetKVPairsFunc:       func() []*displayKVType { return kvPairs },
			SectionStartOffset:   db.offsetOfRelationSectionFromStartOfFile(),
		}, nil
	default:
		return nil, errorsx.Errorf("didn't understand section name: %q", name)
	}
}

func renderRoot(db *MapmakerDBConn, w http.ResponseWriter) {
	data := map[string]interface{}{
		"Name":   db.Name(),
		"Header": db.header,
		"SectionNames": map[string]string{
			"Nodes":     nodeSectionName,
			"Ways":      waySectionName,
			"Relations": relationSectionName,
			"Tags":      tagIndexSectionName,
		},
	}

	err := visTmpl.Execute(w, data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func renderSectionData(db *MapmakerDBConn, w http.ResponseWriter, r *http.Request) {
	var err error
	data := map[string]interface{}{}

	matches := sectionDataRegexp.FindAllStringSubmatch(r.URL.Path, -1)[0]
	for i, name := range sectionDataRegexp.SubexpNames() {
		if name == "" {
			continue
		}
		data[name] = matches[i]
	}

	section, err := sectionByName(db, data["SectionName"].(string))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	index, err := strconv.Atoi(data["Index"].(string))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	blockMetadata := section.SectionMetadata.BlockMetadatas[index]

	data["BlockMetadata"] = blockMetadata

	file := db.fileHandlerPool.Get()
	defer db.fileHandlerPool.Release(file)

	err = db.decodeBlock(file, blockMetadata, section.SectionStartOffset, section.onFoundBlockDataFunc, nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	data["BlockData"] = section.GetKVPairsFunc()

	err = sectionDataTmpl.Execute(w, data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func renderSectionOverview(db *MapmakerDBConn, w http.ResponseWriter, r *http.Request) {
	var err error

	data := map[string]interface{}{}

	matches := sectionOverviewRegexp.FindAllStringSubmatch(r.URL.Path, -1)[0]
	for i, name := range sectionOverviewRegexp.SubexpNames() {
		if name == "" {
			continue
		}
		data[name] = matches[i]
	}

	section, err := sectionByName(db, data["SectionName"].(string))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	data["SectionMetadata"] = section.SectionMetadata

	err = sectionOverviewTmpl.Execute(w, data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func VisualiseDBHandleFunc(db *MapmakerDBConn) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if sectionDataRegexp.MatchString(r.URL.Path) {
			renderSectionData(db, w, r)
			return
		}

		if sectionOverviewRegexp.MatchString(r.URL.Path) {
			renderSectionOverview(db, w, r)
			return
		}

		if r.URL.Path == "/" {
			renderRoot(db, w)
			return
		}

		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}
}

const (
	visTmplStr = `
	<html>
	<head><title>Mapmaker DB visualisation</title></head>
	<body>
		<h1>{{.Name}}</h1>
		{{with .Header}}
			<p>ownmap file format version: {{.Version}}</p>
		{{end}}
		{{with .SectionNames}}
		<div>
			<p><a href="{{.Nodes}}">Nodes</a></p>
			<p><a href="{{.Ways}}">Ways</a></p>
			<p><a href="{{.Relations}}">Relations</a></p>
			<p><a href="{{.Tags}}">Tags</a></p>
		</div>
		{{end}}
	</body>
	</html>
	`
	sectionOverviewTmplStr = `
	<html>
	<head><title>Section overview: {{$.SectionName}}</title></head>
	<body>
		<h3>Section Overview: {{$.SectionName}}</h3>
		{{with .SectionMetadata}}
			<p>TotalSize (bytes): {{.TotalSize}}</p>
			{{with .BlockMetadatas}}
			{{range $index, $element := .}}
				<div style="background-color: #ccc; margin: 10px">
					<p><a href="{{$.SectionName}}/{{$index}}">Section {{$index}}</a></p>
					<p>Start offset: {{.StartOffsetFromStartOfSectionData}}</p>
					<p>Last item: {{keyToStr .LastItemInBlockValue $.SectionName}}</p>
					<p>Block Size:{{.BlockSize}}</p>
				</div>
			{{end}}
			{{end}}
		{{end}}
	</body>
	</html>
	`
	sectionDataTmplStr = `
	<html>
	<head><title>Section data: {{$.SectionName}} :: {{$.Index}}</title></head>
	<body>
		<h1>{{.SectionName}}</h1>
		<h2>Index: {{.Index}}</h2>
		{{with .BlockMetadata}}
			<div style="background-color: #ccc; margin: 10px">
				<p>Start offset: {{.StartOffsetFromStartOfSectionData}}</p>
				<p>Last item: {{keyToStr .LastItemInBlockValue $.SectionName}}</p>
				<p>Block Size:{{.BlockSize}}</p>
			</div>
		{{end}}
		<ul>
		{{range .BlockData}}
			<li>{{.Key}} :: {{.Value}}</li>
		{{end}}
		</ul>
	</body>
	</html>
	`
)
