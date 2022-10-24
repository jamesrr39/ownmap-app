// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

// Package duckdb implements a databse/sql driver for the DuckDB database.
package duckdb

/*
#include <duckdb.h>
*/
import "C"

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net/url"
	"unsafe"
)

func init() {
	sql.Register("duckdb", impl{})
}

type impl struct{}

func (impl) Open(dataSourceName string) (driver.Conn, error) {
	var db C.duckdb_database
	var con C.duckdb_connection

	parsedDSN, err := url.Parse(dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", parseConfigError, err.Error())
	}

	path := C.CString(parsedDSN.Path)
	defer C.free(unsafe.Pointer(path))

	// Check for config options.
	if len(parsedDSN.RawQuery) == 0 {
		if state := C.duckdb_open(path, &db); state == C.DuckDBError {
			return nil, openError
		}
	} else {
		config, err := prepareConfig(parsedDSN.Query())
		if err != nil {
			return nil, err
		}

		errMsg := C.CString("")
		defer C.duckdb_free(unsafe.Pointer(errMsg))

		if state := C.duckdb_open_ext(path, &db, config, &errMsg); state == C.DuckDBError {
			return nil, fmt.Errorf("%w: %s", openError, C.GoString(errMsg))
		}
	}

	if state := C.duckdb_connect(db, &con); state == C.DuckDBError {
		return nil, openError
	}

	return &conn{db: &db, con: &con}, nil
}

func prepareConfig(options map[string][]string) (C.duckdb_config, error) {
	var config C.duckdb_config
	if state := C.duckdb_create_config(&config); state == C.DuckDBError {
		return nil, createConfigError
	}

	for k, v := range options {
		if len(v) > 0 {
			state := C.duckdb_set_config(config, C.CString(k), C.CString(v[0]))
			if state == C.DuckDBError {
				return nil, fmt.Errorf("%w: affected config option %s=%s", prepareConfigError, k, v[0])
			}
		}
	}

	return config, nil
}

var (
	openError          = errors.New("could not open database")
	parseConfigError   = errors.New("could not parse config for database")
	createConfigError  = errors.New("could not create config for database")
	prepareConfigError = errors.New("could not set config for database")
)
