.PHONY: install-protobuf-generators
install-protobuf-generators:
	# to install protobuf packages/binaries
	go get github.com/gogo/protobuf/protoc-gen-gofast
	go get github.com/gogo/protobuf/proto
	go get github.com/gogo/protobuf/protoc-gen-gogoslick
	go get github.com/gogo/protobuf/gogoproto


.PHONY: generate-protobufs
generate-protobufs:
	protoc --gogoslick_out=. -I thirdparty/github.com/google/protobuf/src -I . --gogoslick_opt=paths=source_relative *.proto
