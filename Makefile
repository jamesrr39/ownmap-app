.DEFAULT_GOAL := help

.PHONY: help
help:
	echo "no help available"

.PHONY: install-protobuf-generators
install-protobuf-generators:
	# to install protobuf packages/binaries
	go get github.com/gogo/protobuf/protoc-gen-gofast
	go get github.com/gogo/protobuf/proto
	go get github.com/gogo/protobuf/protoc-gen-gogoslick
	go get github.com/gogo/protobuf/gogoproto

.PHONY: generate-protobufs
generate-protobufs:
	protoc --gogoslick_out=. -I thirdparty/github.com/google/protobuf/src -I . --gogoslick_opt=paths=source_relative ownmap/*.proto
	protoc --gogoslick_out=. -I thirdparty/github.com/google/protobuf/src -I . --gogoslick_opt=paths=source_relative ownmapdal/ownmapdb/ownmapdb.proto
	protoc --gogoslick_out=. -I thirdparty/github.com/google/protobuf/src -I . --gogoslick_opt=paths=source_relative ownmapdal/ownmapdb/diskfilemap/disk_file_map_types.proto

.PHONY: generate-static
generate-static:
	go run vendor/github.com/rakyll/statik/statik.go -src static_assets -dest staticassetsbundle

.PHONY: test
test:
	go test ./...


.PHONY: update_snapshots
update_snapshots:
	UPDATE_SNAPSHOTS=1 go test ./...

.PHONY: run_dev_server__mapboxgl_style
run_dev_server__mapboxgl_style:
	CGO_ENABLED=0 go run cmd/ownmap-app-main.go serve ownmapdb://data/data_files/sample_database.db --default-style-id="basic" --extra-styles=data/styles/gl-style

.PHONY: run_dev_server__mapboxgl_style_postgres
run_dev_server__mapboxgl_style_postgres:
	CGO_ENABLED=0 go run cmd/ownmap-app-main.go serve postgresql://docker:docker@localhost:5432/ownmap?sslmode=disable --default-style-id="basic" --extra-styles=data/styles/gl-style


.PHONY: run_dev_server__mapboxgl_styles
run_dev_server__mapboxgl_styles:
	CGO_ENABLED=0 go run cmd/ownmap-app-main.go serve ownmapdb://data/data_files/sample_database.db --default-style-id="basic" --extra-styles=data/styles/gl-style,data/styles/gl-style2

.PHONY: run_dev_server__basic_style
run_dev_server__basic_style:
	CGO_ENABLED=0 go run cmd/ownmap-app-main.go serve ownmapdb://data/data_files/sample_database.db

# DEV_DOCKER_IMAGE=jamesrr39/run_dev_import_docker
# MAX_MEMORY=8g

.PHONY: build
build:
	CGO_ENABLED=0 go build -o build/default/makmaker cmd/ownmap-app-main.go

# .PHONY: run_dev_import_docker
# run_dev_import_docker:
# 	make build
# 	docker build -t ${DEV_DOCKER_IMAGE} docker/default
# 	docker run --memory=${MAX_MEMORY} ${DEV_DOCKER_IMAGE}


DEV_IMPORT_DIR := data/dev_import/$(shell date +%Y-%m-%d_%H_%M_%S)
DEV_IMPORT_TMP_DIR := ${DEV_IMPORT_DIR}/tmp
DEV_IMPORT_BIG_DIR := data/dev_import_big/$(shell date +%Y-%m-%d_%H_%M_%S)
DEV_IMPORT_BIG_TMP_DIR := data/dev_import_big/$(shell date +%Y-%m-%d_%H_%M_%S)/tmp

.PHONY: run_dev_import
run_dev_import:
	mkdir -p ${DEV_IMPORT_TMP_DIR}
	go build -o ${DEV_IMPORT_TMP_DIR}/ownmap-app cmd/ownmap-app-main.go
	${DEV_IMPORT_TMP_DIR}/ownmap-app \
		import \
		ownmapdb://data/data_files/sample_database.db \
		data/sample-pbf-file.pbf \
		--tmp-dir ${DEV_IMPORT_TMP_DIR} \
		--profile \
		--keep-work-dir

run_dev_import_parquet:
	mkdir -p ${DEV_IMPORT_TMP_DIR}
	mkdir -p data/data_files/parquet_files
	go build -o ${DEV_IMPORT_TMP_DIR}/ownmap-app cmd/ownmap-app-main.go
	${DEV_IMPORT_TMP_DIR}/ownmap-app \
		import \
		parquet://data/data_files/parquet_files \
		data/sample-pbf-file.pbf \
		--tmp-dir ${DEV_IMPORT_TMP_DIR} \
		--profile \
		--keep-work-dir


.PHONY: run_dev_import_postgres
run_dev_import_postgres:
	# docker exec -it ownmapapp_postgresql_1 psql -U docker -c 'DROP DATABASE IF EXISTS ownmap; CREATE DATABASE ownmap;'
	go build -o ${DEV_IMPORT_TMP_DIR}/ownmap-app cmd/ownmap-app-main.go
	${DEV_IMPORT_TMP_DIR}/ownmap-app \
		import \
		postgresql://docker:docker@localhost:5432/ownmap?sslmode=disable \
		data/sample-pbf-file.pbf \
		--tmp-dir ${DEV_IMPORT_TMP_DIR} \
		--profile

.PHONY: run_dev_import_big
run_dev_import_big:
	mkdir -p ${DEV_IMPORT_BIG_TMP_DIR}
	go build -o ${DEV_IMPORT_BIG_TMP_DIR}/ownmap-app cmd/ownmap-app-main.go
	${DEV_IMPORT_BIG_TMP_DIR}/ownmap-app \
		import \
		ownmapdb://${DEV_IMPORT_BIG_DIR}/sample_big_database.db \
		data/dev_import/sample-big-pbf-file.pbf \
		--tmp-dir ${DEV_IMPORT_BIG_TMP_DIR} \
		--profile \
		--keep-work-dir
