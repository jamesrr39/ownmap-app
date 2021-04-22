package errorsx

import (
	"net/http"
)

type logger interface {
	Warn(message string, args ...interface{})
	Error(message string, args ...interface{})
}

func HTTPError(w http.ResponseWriter, log logger, err Error, statusCode int) {
	w.WriteHeader(statusCode)
	if statusCode < 500 {
		log.Warn("%s. Stack trace:\n%s", err.Error(), err.Stack())
	} else {
		log.Error("%s. Stack trace:\n%s", err.Error(), err.Stack())
	}

	w.Write([]byte(err.Error()))
}
