package httpextra

import (
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"

	"github.com/jamesrr39/goutil/errorsx"
)

// LocalDevServerEnvVarName is the environment variable name for the dev server
// For example the standard create-react-app webpack server, this value will be: "http://localhost:3000/"
// If using create-react-app, you can also add BROWSER=0 to the start of the npm/yarn "start" task, and that will prevent a browser tab being automatically opened.
const LocalDevServerEnvVarName = "LOCAL_DEV_SERVER_URL"

// NewLocalDevServerProxy creates a http handler to proxy a local dev server setup
func NewLocalDevServerProxy() (http.Handler, errorsx.Error) {
	originURL := os.Getenv(LocalDevServerEnvVarName)
	if originURL == "" {
		return nil, errorsx.Errorf("no %s environment variable set", LocalDevServerEnvVarName)
	}

	origin, err := url.Parse(originURL)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	director := func(req *http.Request) {
		req.Header.Add("X-Forwarded-Host", req.Host)
		req.Header.Add("X-Origin-Host", origin.Host)
		req.URL.Scheme = "http"
		req.URL.Host = origin.Host
	}

	proxy := httputil.ReverseProxy{
		Director: director,
	}

	return &proxy, nil
}
