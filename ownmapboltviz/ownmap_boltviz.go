package ownmapboltviz

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/gogo/protobuf/proto"
	"github.com/jamesrr39/goutil/bolt-tools/boltviz"
	"github.com/jamesrr39/ownmap-app/ownmap"
)

func GetTemplateMap() boltviz.TemplateMap {
	return boltviz.TemplateMap{
		PrintKey: func(pair boltviz.KVPairDisplay) string {
			switch len(pair.PathFragments) {
			case 4:
				return string(pair.Key)
			case 1:
				decodedPathFragment, err := base64.StdEncoding.DecodeString(pair.PathFragments[0])
				if err != nil {
					return "error: " + err.Error()
				}
				switch string(decodedPathFragment) {
				case "nodes", "ways":
					valUint64 := binary.LittleEndian.Uint64(pair.Key)
					val := int64(valUint64)
					return fmt.Sprintf("%d", val)

				default:
					return string(pair.Key)
				}
			default:
				return "(unknown)"
			}
		},
		PrintValue: func(pair boltviz.KVPairDisplay) string {
			switch len(pair.PathFragments) {
			case 4:
				if len(pair.Value) == 0 {
					return "(empty)"
				}
				bucket1BytesName, err := base64.StdEncoding.DecodeString(pair.PathFragments[2])
				if err != nil {
					return fmt.Sprintf("error: %q", err)
				}
				var obj proto.Message
				switch string(bucket1BytesName) {
				case "ways":
					obj = new(ownmap.IndexIDList)
					err := proto.Unmarshal(pair.Value, obj)
					if err != nil {
						return fmt.Sprintf("error decoding way: %q", err)
					}
				case "nodes":
					obj = new(ownmap.IndexIDList)
					err := proto.Unmarshal(pair.Value, obj)
					if err != nil {
						return fmt.Sprintf("error decoding node: %q", err)
					}
				default:
					return fmt.Sprintf("unrecognised bucket name: %q", string(bucket1BytesName))
				}
				jsonBytes, err := json.Marshal(obj)
				if err != nil {
					return fmt.Sprintf("error: %q", err)
				}
				return string(jsonBytes)
			case 1:
				bucket1BytesName, err := base64.StdEncoding.DecodeString(pair.PathFragments[0])
				if err != nil {
					return fmt.Sprintf("error: %q", err)
				}
				var obj proto.Message
				switch string(bucket1BytesName) {
				case "nodes":
					obj = new(ownmap.OSMNode)
				case "ways":
					obj = new(ownmap.OSMWay)
				case "info":
					obj = new(ownmap.DatasetInfo)
				default:
					return fmt.Sprintf("unknown bucket name: %q", string(bucket1BytesName))
				}
				err = proto.Unmarshal(pair.Value, obj)
				if err != nil {
					return fmt.Sprintf("error: %q", err)
				}
				jsonBytes, err := json.Marshal(obj)
				if err != nil {
					return fmt.Sprintf("error: %q", err)
				}
				return string(jsonBytes)
			default:
				return "(unknown)"
			}
		},
	}
}
