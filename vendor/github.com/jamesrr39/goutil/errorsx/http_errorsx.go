package errorsx

import (
	"encoding/json"
	"net/http"
)

type logger interface {
	Warn(message string, args ...interface{})
	Error(message string, args ...interface{})
}

// HTTPError writes a warning or error to the logger and writes the error message as plain text to the ResponseWriter
func HTTPError(w http.ResponseWriter, log logger, err Error, statusCode int) {
	w.WriteHeader(statusCode)
	if statusCode < 500 {
		log.Warn("%s. Stack trace:\n%s", err.Error(), err.Stack())
	} else {
		log.Error("%s. Stack trace:\n%s", err.Error(), err.Stack())
	}

	w.Write([]byte(err.Error()))
}

type JSONErrorMessageType struct {
	Message string `json:"message"`
}

// HTTPJSONError writes a warning or error to the logger and writes the error message as the Message property a JSONErrorMessageType
func HTTPJSONError(w http.ResponseWriter, log logger, err Error, statusCode int) {
	w.WriteHeader(statusCode)
	if statusCode < 500 {
		log.Warn("%s. Stack trace:\n%s", err.Error(), err.Stack())
	} else {
		log.Error("%s. Stack trace:\n%s", err.Error(), err.Stack())
	}

	json.NewEncoder(w).Encode(JSONErrorMessageType{err.Error()})
}
