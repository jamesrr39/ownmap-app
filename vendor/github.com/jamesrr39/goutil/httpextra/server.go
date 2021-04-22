package httpextra

import (
	"net/http"
	"time"
)

// NewServerWithTimeouts returns an http server with some sensible timeouts
func NewServerWithTimeouts() *http.Server {
	return &http.Server{
		ReadTimeout:       time.Second * 20,
		ReadHeaderTimeout: time.Second * 10,
		WriteTimeout:      time.Minute,
	}
}
