package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	tracing "github.com/jamesrr39/go-tracing"
	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/goutil/gofs"
	"github.com/jamesrr39/goutil/httpextra"
	"github.com/jamesrr39/goutil/logpkg"
	"github.com/jamesrr39/goutil/open"
	"github.com/jamesrr39/goutil/userextra"
	"github.com/jamesrr39/ownmap-app/fonts"
	"github.com/jamesrr39/ownmap-app/ownmapdal"
	"github.com/jamesrr39/ownmap-app/ownmapdal/ownmapdb"
	"github.com/jamesrr39/ownmap-app/ownmapdal/ownmapsqldb/ownmappostgresql"
	"github.com/jamesrr39/ownmap-app/ownmapdal/parquetdb"
	"github.com/jamesrr39/ownmap-app/ownmaprenderer"
	"github.com/jamesrr39/ownmap-app/styling"
	"github.com/jamesrr39/ownmap-app/styling/mapboxglstyle"
	"github.com/jamesrr39/ownmap-app/webservices"
	"github.com/paulmach/osm"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/pkg/profile"
)

const (
	MAX_SERVER_RUNNING_ATTEMPTS    = 50
	DEFAULT_PARQUET_ROW_GROUP_SIZE = 128 * 1024 * 1024 * 4 //128M * 4
)

var logger *logpkg.Logger

func main() {
	if len(os.Args) == 1 {
		logger = logpkg.NewLogger(os.Stderr, logpkg.LogLevelInfo)
		// start in desktop "double-click" visual mode
		err := setupDesktopMode()
		if err != nil {
			log.Fatalf("failed to start server: %q\n%s\n", err.Error(), err.Stack())
		}
	} else {
		// start in server mode
		verbose := kingpin.Flag("v", "verbose logging").Bool()

		logLevel := logpkg.LogLevelInfo
		if *verbose {
			logLevel = logpkg.LogLevelDebug
		}
		logger = logpkg.NewLogger(os.Stderr, logLevel)

		setupServe()
		setupImport()

		kingpin.Parse()
	}
}

func ensureDefaultPathsConfig() (*ownmapdal.PathsConfig, errorsx.Error) {
	rootDir, err := userextra.ExpandUser("~/.local/share/github.com/jamesrr39/ownmap/")
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	pathsConfig := &ownmapdal.PathsConfig{
		StylesDir:       filepath.Join(rootDir, "styles"),
		DataDir:         filepath.Join(rootDir, "data_files"),
		RawDataFilesDir: filepath.Join(rootDir, "raw_data_files"),
		TempDir:         filepath.Join(rootDir, "tmp"),
		TraceDir:        filepath.Join(rootDir, "trace"),
	}

	err = pathsConfig.EnsurePaths()
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	return pathsConfig, nil
}

func loadStylesFromDir(dir string) (*styling.StyleSet, errorsx.Error) {
	fileInfos, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	styles := []styling.Style{&styling.CustomBasicStyle{}}
	for _, fileInfo := range fileInfos {
		style, err := loadStyle(filepath.Join(dir, fileInfo.Name()))
		if err != nil {
			log.Printf("error loading styles from %q. Error: %q\n", filepath.Join(dir, fileInfo.Name()), err)
			continue
		}

		styles = append(styles, style)
	}

	sort.Slice(styles, func(a, b int) bool {
		return styles[a].GetStyleID() > styles[b].GetStyleID()
	})

	styleSet, err := styling.NewStyleSet(styles, styling.BUILTIN_STYLEID)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	return styleSet, nil
}

func loadDBConnsFromDir(logger *logpkg.Logger, pathsConfig *ownmapdal.PathsConfig, ownmapDBFileHandlerLimit uint) (*ownmapdal.DBConnSet, errorsx.Error) {
	dirItems, err := ioutil.ReadDir(pathsConfig.DataDir)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	var conns []ownmapdal.DataSourceConn
	for _, dirItem := range dirItems {
		filePath := filepath.Join(pathsConfig.DataDir, dirItem.Name())
		dbConn, err := loadDBConn(filePath, ownmapDBFileHandlerLimit)
		if err != nil {
			logger.Error("failed to load %q as Bolt DB. Error: %q\nStack: %s", filePath, err.Error(), err.Stack())
			continue
		}

		conns = append(conns, dbConn)
	}

	return ownmapdal.NewDBConnSet(conns), nil
}

func setupDesktopMode() errorsx.Error {
	// TODO is the server already running?
	var err error
	pathsConfig, err := ensureDefaultPathsConfig()
	if err != nil {
		return errorsx.Wrap(err)
	}

	styleSet, err := loadStylesFromDir(pathsConfig.StylesDir)
	if err != nil {
		return errorsx.Wrap(err)
	}

	dbConns, err := loadDBConnsFromDir(logger, pathsConfig, DEFAULT_MAPMAKER_DB_FILE_HANDLER_LIMIT)
	if err != nil {
		return errorsx.Wrap(err)
	}

	shouldProfile := false
	router, err := createServer(dbConns, styleSet, pathsConfig, logger, DEFAULT_MAPMAKER_DB_FILE_HANDLER_LIMIT, shouldProfile)
	if err != nil {
		return errorsx.Wrap(err)
	}

	server := httpextra.NewServerWithTimeouts()
	server.Addr = fmt.Sprintf("localhost:%d", DEFAULT_PORT)
	server.Handler = router

	errChan := make(chan errorsx.Error)

	go func() {
		err = server.ListenAndServe()
		if err != nil {
			errChan <- errorsx.Wrap(err)
			return
		}
	}()

	go func() {
		// test server is running
		for i := 0; i < MAX_SERVER_RUNNING_ATTEMPTS; i++ {
			r, err := http.NewRequest(http.MethodGet, fmt.Sprintf("http://%s/api/info", server.Addr), nil)
			if err != nil {
				errChan <- errorsx.Wrap(err)
				return
			}

			client := http.Client{
				Timeout: time.Second * 10,
			}
			resp, err := client.Do(r)
			if err != nil {
				// retry after wait
				time.Sleep(time.Millisecond * 500)
				continue
			}
			if resp.StatusCode != http.StatusOK {
				errChan <- errorsx.Errorf("expected response code %d from /api/info call, but got %d", http.StatusOK, resp.StatusCode)
				return
			}

			errChan <- nil
			return
		}

		errChan <- errorsx.Errorf("server did not start after %d attempts", MAX_SERVER_RUNNING_ATTEMPTS)
	}()

	err = <-errChan
	if err != nil {
		return errorsx.Wrap(err)
	}

	// wait for API to be running
	time.Sleep(time.Second)

	err = open.OpenURL(fmt.Sprintf("http://%s", server.Addr))
	if err != nil {
		return errorsx.Wrap(err)
	}

	// TODO better listening
	doneChan := make(chan struct{})
	<-doneChan

	return nil
}

const (
	DEFAULT_PORT                           = 9000
	DEFAULT_MAPMAKER_DB_FILE_HANDLER_LIMIT = 20
)

var addrHelp = fmt.Sprintf(
	`address to serve on. Ex: ':%d' listen on port %d to traffic from anywhere. 'localhost:%d' listen on port %d to traffic from localhost`,
	DEFAULT_PORT, DEFAULT_PORT, DEFAULT_PORT, DEFAULT_PORT,
)

func setupServe() {
	cmd := kingpin.Command("serve", "serve webserver")
	addr := cmd.Flag("addr", addrHelp).Default(fmt.Sprintf(":%d", DEFAULT_PORT)).String()
	dbFilePath := cmd.Arg("db-file", "DB file to read from").Required().String()
	defaultStyleID := cmd.Flag("default-style-id", "default style type to use to render").Default(styling.BUILTIN_STYLEID).String()
	extraStyleDefinitionPathsStr := cmd.Flag("extra-styles", "comma separated list of paths to folder containing style definitions (currently supports only mapbox GL styles)").String()
	ownmapDBFileHandlerLimit := cmd.Flag("ownmapdb-file-handler-limit", "maximum amount of file handlers per ownmap DB").Default(fmt.Sprintf("%d", DEFAULT_MAPMAKER_DB_FILE_HANDLER_LIMIT)).Uint()
	shouldProfile := cmd.Flag("profile", "profile the request performance").Bool()
	cmd.Action(func(ctx *kingpin.ParseContext) error {
		run := func() errorsx.Error {
			var err error
			var extraStyleDefinitionPaths []string
			for _, path := range strings.Split(*extraStyleDefinitionPathsStr, ",") {
				if path == "" {
					continue
				}
				extraStyleDefinitionPaths = append(extraStyleDefinitionPaths, path)
			}

			// create the style set
			styles := []styling.Style{&styling.CustomBasicStyle{}}
			for _, styleDefinitionPath := range extraStyleDefinitionPaths {
				style, err := loadStyle(styleDefinitionPath)
				if err != nil {
					return errorsx.Wrap(err)
				}

				styles = append(styles, style)
			}

			styleSet, err := styling.NewStyleSet(styles, *defaultStyleID)
			if err != nil {
				return errorsx.Wrap(err)
			}

			logger := logpkg.NewLogger(os.Stderr, logpkg.LogLevelDebug)

			dbConn, err := loadDBConn(*dbFilePath, *ownmapDBFileHandlerLimit)
			if err != nil {
				return errorsx.Wrap(err)
			}

			dbConnSet := ownmapdal.NewDBConnSet([]ownmapdal.DataSourceConn{dbConn})

			// create the router
			router, err := createServer(dbConnSet, styleSet, nil, logger, *ownmapDBFileHandlerLimit, *shouldProfile)
			if err != nil {
				return errorsx.Wrap(err)
			}

			server := httpextra.NewServerWithTimeouts()
			server.Addr = *addr
			server.Handler = router

			logger.Info("about to start serving on %q", *addr)

			err = server.ListenAndServe()
			if err != nil {
				return errorsx.Wrap(err)
			}
			return nil
		}

		err := run()
		if err != nil {
			return fmt.Errorf("error: %q\nStack trace:\n%s", err.Error(), err.Stack())
		}
		return nil
	})
}

func boundsStrToOSMBounds(boundsStr []string) (osm.Bounds, errorsx.Error) {
	if len(boundsStr) == 1 && boundsStr[0] == "" {
		return osm.Bounds{
			MaxLat: 90,
			MinLat: -90,
			MaxLon: 180,
			MinLon: -180,
		}, nil
	}

	bounds := osm.Bounds{}

	if len(boundsStr) != 4 {
		return bounds, errorsx.Errorf("expected 4 (or 0) bounds, but found %d", len(boundsStr))
	}

	for idx, boundStr := range boundsStr {
		boundFloat, err := strconv.ParseFloat(boundStr, 64)
		if err != nil {
			return bounds, errorsx.Wrap(err)
		}
		switch idx {
		case 0:
			bounds.MinLon = boundFloat
		case 1:
			bounds.MaxLat = boundFloat
		case 2:
			bounds.MaxLon = boundFloat
		case 3:
			bounds.MinLat = boundFloat
		}
	}

	return bounds, nil
}

var dbFileHelp = fmt.Sprintf("DB file to import into. It should be the type, followed by the separator (%s), followed by the path or URL. For example: %s%smy/db/file",
	ownmapdal.ConnectionPathSeparator,
	string(ownmapdal.DBFileTypeMapmakerDB),
	ownmapdal.ConnectionPathSeparator,
)

func setupImport() {
	cmd := kingpin.Command("import", "import PBF file")
	dbFileConnString := cmd.Arg("db-file", dbFileHelp).Required().String()
	filePath := cmd.Arg("file", "PBF file to import").Required().String()
	tmpDirFlag := cmd.Flag("tmp-dir", "temp dir to use, if applicable for this DB file type (note: recommended to be in the same partition as the resulting outputted file").String()
	// boundsStr := cmd.Flag("bounds", "set the bounds that the importer should import within. [W,N,E,S] Example: -1,1,1,-1").Default("").String()
	// keepWorkDirFlag := cmd.Flag("keep-work-dir", "keep the working directory used during the import (for debugging)").Bool()
	// ownmapDBFileHandlerLimit := cmd.Flag("ownmapdb-file-handler-limit", "maximum amount of file handlers per ownmap DB").Default(fmt.Sprintf("%d", DEFAULT_MAPMAKER_DB_FILE_HANDLER_LIMIT)).Uint()
	shouldProfile := cmd.Flag("profile", "profile the import performance").Bool()
	parquetRowGroupSize := cmd.Flag("parquet-row-group-size", `(applies only to imports in the parquet format) Amount of rows in one parquet "group"`).Default(fmt.Sprintf("%d", DEFAULT_PARQUET_ROW_GROUP_SIZE)).Int64()
	cmd.Action(func(ctx *kingpin.ParseContext) (err error) {
		defer func() {
			errorx, ok := err.(errorsx.Error)
			if ok {
				log.Printf("%s\n%s\n", errorx.Error(), errorx.Stack())
			}
		}()

		fs := gofs.NewOsFs()

		var workDirPath string
		if tmpDirFlag != nil {
			workDirPath = *tmpDirFlag
		} else {
			workDirPath, err = ioutil.TempDir("", "")
			if err != nil {
				return errorsx.Wrap(err)
			}
		}
		if *shouldProfile {
			defer profile.Start(profile.ProfilePath(workDirPath), profile.CPUProfile).Stop()
		}

		// bounds := ownmap.GetWholeWorldBounds()
		// boundsStrs := strings.Split(*boundsStr, ",")
		// bounds, err = boundsStrToOSMBounds(boundsStrs)
		// if err != nil {
		// 	return err
		// }

		startTime := time.Now()

		logger.Info("filePath: %s", *filePath)

		file, err := fs.Open(*filePath)
		if err != nil {
			return errorsx.Wrap(err)
		}
		defer file.Close()

		auxFile, err := fs.Open(*filePath)
		if err != nil {
			return errorsx.Wrap(err)
		}
		defer auxFile.Close()

		fileInfo, err := file.Stat()
		if err != nil {
			return errorsx.Wrap(err)
		}

		pbfReader, err := ownmapdal.NewDefaultPBFReader(file)
		if err != nil {
			return errorsx.Wrap(err)
		}
		defer pbfReader.Close()

		auxillaryPBFReader, err := ownmapdal.NewDefaultPBFReader(auxFile)
		if err != nil {
			return errorsx.Wrap(err)
		}
		defer auxillaryPBFReader.Close()

		pbfHeader, err := pbfReader.Header()
		if err != nil {
			return errorsx.Wrap(err)
		}

		finishedChan := make(chan bool)

		go runLogProgress(pbfReader, finishedChan, fileInfo.Size())

		dbConnConfig, err := ownmapdal.ParseDBConnFilePath(*dbFileConnString)
		if err != nil {
			return errorsx.Wrap(err, "db file path", *dbFileConnString)
		}

		var finalStorage ownmapdal.FinalStorage
		switch ownmapdal.DBFileType(dbConnConfig.Type) {
		// case ownmapdal.DBFileTypeMapmakerDB:
		// 	options := ownmapdb.ImportOptions{
		// 		KeepWorkDir: *keepWorkDirFlag,
		// 	}

		// 	finalStorage, err = ownmapdb.NewFinalStorage(logger, fs, workDirPath, dbConnConfig.ConnectionPath, *ownmapDBFileHandlerLimit, pbfHeader, options)
		// 	if err != nil {
		// 		return errorsx.Wrap(err)
		// 	}

		// case ownmapdal.DBFileTypePostgresql:
		// 	finalStorage, err = ownmappostgresql.NewFinalStorage(dbConnConfig.ConnectionPath, pbfHeader)
		// 	if err != nil {
		// 		return errorsx.Wrap(err)
		// 	}

		case ownmapdal.DBFileTypeParquet:
			finalStorage, err = parquetdb.NewFinalStorage(dbConnConfig.ConnectionPath, pbfHeader, *parquetRowGroupSize)
			if err != nil {
				return errorsx.Wrap(err)
			}

		default:
			return errorsx.Errorf("unknown DB file type: %q\n", dbConnConfig.Type)
		}

		_, err = ownmapdal.Import2(logger, pbfReader, auxillaryPBFReader, fs, finalStorage)
		if err != nil {
			return errorsx.Wrap(err)
		}

		// _, err = ownmapdal.Import(logger, pbfReader, fs, finalStorage, bounds)
		// if err != nil {
		// 	return errorsx.Wrap(err)
		// }

		logger.Info("import finished in %s", time.Since(startTime))

		finishedChan <- true

		return nil
	})
}

func loadStyle(styleDefinitionPath string) (styling.Style, errorsx.Error) {
	file, err := os.Open(filepath.Join(styleDefinitionPath, "style.json"))
	if err != nil {
		return nil, errorsx.Wrap(err)
	}
	defer file.Close()

	style, err := mapboxglstyle.Parse(file)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	return style, nil
}

func isLocalhost(addr string) bool {
	return addr == "::1" || addr == "127.0.0.1"
}

func createLocalhostMiddleware() func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		fn := func(w http.ResponseWriter, r *http.Request) {
			if isLocalhost(r.RemoteAddr) {
				http.Error(w, "connections only allowed from the same computer the server is running on", http.StatusForbidden)
				return
			}

			next.ServeHTTP(w, r)
		}

		return http.HandlerFunc(fn)
	}
}

const (
	adminPath = "admin"
)

func createServer(dbConns *ownmapdal.DBConnSet, styleSet *styling.StyleSet, pathsConfig *ownmapdal.PathsConfig, logger *logpkg.Logger, ownmapDBFileHandlerLimit uint, shouldProfile bool) (chi.Router, errorsx.Error) {
	var err error

	renderer := ownmaprenderer.NewRasterRenderer(fonts.DefaultFont())

	adminService, err := webservices.NewAdminService(logger, pathsConfig, dbConns, ownmapdal.NewImportQueue(pathsConfig), adminPath, ownmapDBFileHandlerLimit)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	var traceDirPath string
	if pathsConfig == nil {
		traceDirPath, err = ioutil.TempDir("", "")
		if err != nil {
			return nil, errorsx.Wrap(err)
		}
	} else {
		traceDirPath = pathsConfig.TraceDir
	}

	traceFilePath := filepath.Join(traceDirPath, fmt.Sprintf("trace_%s.pbf", time.Now().Format("2006-01-02__03_04_05")))
	logger.Info("tracing at %q", traceFilePath)

	traceFile, err := os.Create(traceFilePath)
	if err != nil {
		return nil, errorsx.Wrap(err)
	}

	tracer := tracing.NewTracer(traceFile)

	router := chi.NewRouter()
	router.Use(middleware.DefaultLogger)
	router.Use(tracing.Middleware(tracer))
	router.Route("/api/", func(r chi.Router) {
		r.Mount("/info", webservices.NewInfoService(logger, dbConns, styleSet))
		r.Mount("/tiles/", webservices.NewTileService(logger, dbConns, renderer, styleSet, shouldProfile))
		r.Mount("/nearby/", webservices.NewNearbyThingsWebService(logger, dbConns))
	})
	router.Route(fmt.Sprintf("/%s/", adminPath), func(r chi.Router) {
		r.Use(createLocalhostMiddleware())
		r.Mount("/", adminService)
	})

	router.Mount("/", http.FileServer(http.Dir("web-client")))

	return router, nil
}

func runLogProgress(pbfReader *ownmapdal.DefaultPBFReader, finishedChan chan bool, totalBytes int64) {
	for {
		time.Sleep(time.Second * 5)
		select {
		case <-finishedChan:
			log.Println("finished scanning the PBF file. Now committing to storage. This make take several minutes...")
			return
		default:
			fullyScannedBytes := pbfReader.FullyScannedBytes()
			log.Printf("scanned bytes so far: %d/%d (%0.02f%%)\n", fullyScannedBytes, totalBytes, float64(fullyScannedBytes)*100/float64(totalBytes))
		}
	}
}

func loadDBConn(dbConfigString string, ownmapDBFileHandlerLimit uint) (ownmapdal.DataSourceConn, errorsx.Error) {
	dbConnConfig, err := ownmapdal.ParseDBConnFilePath(dbConfigString)
	if err != nil {
		return nil, errorsx.Wrap(err, "db file path", dbConfigString)
	}

	switch dbConnConfig.Type {
	case ownmapdal.DBFileTypeMapmakerDB:
		openFileFunc := func() (gofs.File, errorsx.Error) {
			ownmapDBFile, err := os.Open(dbConnConfig.ConnectionPath)
			if err != nil {
				return nil, errorsx.Wrap(err)
			}

			return ownmapDBFile, nil
		}

		return ownmapdb.NewMapmakerDBConn(openFileFunc, filepath.Base(dbConfigString), ownmapDBFileHandlerLimit)
	case ownmapdal.DBFileTypePostgresql:
		return ownmappostgresql.NewDBConn(dbConnConfig.ConnectionPath)
	case ownmapdal.DBFileTypeParquet:
		return parquetdb.NewParquetDatasource(dbConnConfig.ConnectionPath)
	default:
		return nil, errorsx.Errorf("unrecognized db connection type: %q", dbConnConfig.Type)
	}
}
