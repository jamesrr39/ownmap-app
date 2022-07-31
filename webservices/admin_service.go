package webservices

import (
	"fmt"
	"html/template"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-chi/chi"
	"github.com/jamesrr39/goutil/errorsx"
	"github.com/jamesrr39/goutil/gofs"
	"github.com/jamesrr39/goutil/logpkg"
	"github.com/jamesrr39/ownmap-app/ownmapdal"
	"github.com/jamesrr39/ownmap-app/ownmapdal/ownmapdb"
)

const (
	dbPath = "db"
)

type AdminService struct {
	logger                   *logpkg.Logger
	pathsConfig              *ownmapdal.PathsConfig
	dbConnSet                *ownmapdal.DBConnSet
	importQueue              *ownmapdal.ImportQueue
	routerURLBasePath        string
	ownmapDBFileHandlerLimit uint
	chi.Router
}

func NewAdminService(
	logger *logpkg.Logger,
	pathsConfig *ownmapdal.PathsConfig,
	dbConnSet *ownmapdal.DBConnSet,
	importQueue *ownmapdal.ImportQueue,
	routerURLBasePath string,
	ownmapDBFileHandlerLimit uint,
) (*AdminService, errorsx.Error) {

	as := &AdminService{logger, pathsConfig, dbConnSet, importQueue, routerURLBasePath, ownmapDBFileHandlerLimit, chi.NewRouter()}

	as.Router.Get(fmt.Sprintf("/%s/{dbName}/*", dbPath), as.handleDBVisualisation)
	as.Router.Get("/", as.handleGet)
	as.Router.Post("/rawDataFile", as.handlePostRawDataFile)

	return as, nil
}

func (as *AdminService) handleDBVisualisation(w http.ResponseWriter, r *http.Request) {
	dbName := chi.URLParam(r, "dbName")

	for _, dbConn := range as.dbConnSet.GetConns() {
		if dbConn.Name() != dbName {
			continue
		}

		// strip prefix of route
		replacePath := fmt.Sprintf("/%s/%s/%s", as.routerURLBasePath, dbPath, dbConn.Name())
		r.URL.Path = strings.Replace(r.URL.Path, replacePath, "", 1)

		switch t := dbConn.(type) {
		case *ownmapdb.MapmakerDBConn:
			handleFunc := ownmapdb.VisualiseDBHandleFunc(t)
			handleFunc(w, r)
			return
		default:
			errorsx.HTTPError(w, as.logger, errorsx.Errorf("unknown db type: %T (%q)", dbConn, dbConn.Name()), http.StatusInternalServerError)
			return
		}
	}

	errorsx.HTTPError(w, as.logger, errorsx.Errorf("couldn't find db %q", dbName), http.StatusNotFound)
	return
}

func (as *AdminService) handlePostRawDataFile(w http.ResponseWriter, r *http.Request) {
	multipartFile, formData, err := r.FormFile("rawDataFile")
	if err != nil {
		errorsx.HTTPError(w, as.logger, errorsx.Wrap(err), http.StatusBadRequest)
		return
	}
	defer multipartFile.Close()

	dbFileType := r.URL.Query().Get("dbFileType")
	var importFunc ownmapdal.ProcessImportFunc
	switch ownmapdal.DBFileType(dbFileType) {
	case ownmapdal.DBFileTypeMapmakerDB:
		importFunc = func(pbfReader, auxPbfReader ownmapdal.PBFReader) (ownmapdal.DataSourceConn, errorsx.Error) {
			workDirPath := filepath.Join(as.pathsConfig.TempDir, time.Now().Format("import_2006-01-02_15_04_05"))

			fs := gofs.NewOsFs()
			err := fs.MkdirAll(workDirPath, 0700)
			if err != nil {
				return nil, errorsx.Wrap(err)
			}

			pbfHeader, err := pbfReader.Header()
			if err != nil {
				return nil, errorsx.Wrap(err)
			}

			importer, err := ownmapdb.NewFinalStorage(
				as.logger,
				fs,
				workDirPath,
				filepath.Join(as.pathsConfig.DataDir, formData.Filename+".ownmapdb"),
				as.ownmapDBFileHandlerLimit,
				pbfHeader,
				ownmapdb.ImportOptions{},
			)
			if err != nil {
				return nil, errorsx.Wrap(err)
			}

			dbConn, err := ownmapdal.Import2(
				as.logger,
				pbfReader,
				auxPbfReader,
				fs,
				importer,
				ownmapdal.DefaultImporter2Opts(),
			)
			if err != nil {
				return nil, errorsx.Wrap(err)
			}

			return dbConn, nil
		}
	default:
		errorsx.HTTPError(w, as.logger, errorsx.Errorf("unrecognised db file type: %q", dbFileType), http.StatusInternalServerError)
		return
	}
	err = as.importQueue.AddItemToQueue(multipartFile, formData.Filename, importFunc, as.dbConnSet.AddDBConn)
	if err != nil {
		errorsx.HTTPError(w, as.logger, errorsx.Wrap(err), http.StatusInternalServerError)
		return
	}
}

func (as *AdminService) handleGet(w http.ResponseWriter, r *http.Request) {
	var boltvizMountNames []string
	for _, dbConn := range as.dbConnSet.GetConns() {
		boltvizMountNames = append(boltvizMountNames, dbConn.Name())
	}

	data := map[string]interface{}{
		"BoltvizMountNames": boltvizMountNames,
		"RouterURLBasePath": as.routerURLBasePath,
	}

	if as.pathsConfig != nil {
		data["StylesDirImportPath"] = as.pathsConfig.StylesDir
		data["DataDirImportPath"] = as.pathsConfig.DataDir
		data["RawDataImportPath"] = as.pathsConfig.RawDataFilesDir
		data["ImportQueueStatus"] = as.importQueue.GetItems()
	}
	err := adminTmpl.Execute(w, data)
	if err != nil {
		errorsx.HTTPError(w, as.logger, errorsx.Wrap(err), http.StatusInternalServerError)
		return
	}
}

var adminTmpl *template.Template

func init() {
	var err error
	adminTmpl, err = template.New("admin/index.hmtl").Parse(adminTemplate)
	if err != nil {
		panic(err)
	}
}

const adminTemplate = `
<html>
	<head>
		<title>admin</title>
		<style type="text/css">
		div {
			margin: 10px;
			border: 1px solid grey;
			padding: 10px;
		}
		
		</style>
		<script>

		function submitRawDataFile(formEl) {
			const rawDataFile = formEl.elements["rawDataFile"].value;
			
			const formData = new FormData(formEl);

			fetch('/{{.RouterURLBasePath}}/rawDataFile', {method: 'POST', body: formData})
				.then(() => alert('successfully uploaded raw data file. File is queued for processing.'))
				.catch(e => {
					console.error(e);
					alert('failed to upload raw data file: ' + e);
				});
		}
		</script>
	</head>
	<body>
		<h1>Admin settings</h1>
		<div>
			<h2>Already loaded DBs</h2>
			{{range .BoltvizMountNames}}
				<p>
					<a href="db/{{.}}/">{{.}}</a>
				</p>
			{{end}}
		</div>

		<div>
			<h2>Import Queue:</h2>
			<sub>Refresh page for updates</sub>
			{{range .ImportQueueStatus}}
				<h3>{{.RawDataFilePath}}</h3>
				<p>Status: {{.Status}}</p>
				<p>% progress: {{printf "%.2f%%" .ProgressPercent}}</p>
				<p>Time in progress: {{.TimeInProgress}}</p>
			{{end}}
		</div>
		
		<div>
			<h2>Map Data</h2>
			<p>To import map data, download an OpenStreetMap extract, and upload it here to create a MapMaker DB file</p>

			<p>Why? OpenStreetMap extracts are optimised for small file size, but the program needs a file that can has fast data access. So the MapMaker DB file is a larger file, with much faster access to data.</p>

			<p>MapMaker DB files can be copied between different computers</p>

			<div>
				<h3>
					Upload a OpenStreetMap extract
				</h3>
				<form action="javascript:;" method="POST" enctype="multipart/form-data" onsubmit="submitRawDataFile(this)" name="rawDataUploadForm">
					<p>OpenStreetMap extract file (.pbf file).</p>
					<p>This will be copied into <pre>{{.RawDataImportPath}}</pre> and the MapMaker DB file will be created at <pre>{{.DataDirImportPath}}</pre></p>
					<p>
						<label>
							OpenStreetMap extract file (.pbf file)
							<input type="file" name="rawDataFile" />
						</label>
					</p>
					<input type="submit" value="Go!" />
				</form>
			</div>
		</div>
		<div>
			<h3>
				Import a MapMaker DB file
			</h3>
			<form>
				<label>
					MapMaker DB file
					<input type="file" />
				</label>
				<button type="submit">Go!</button>
			</form>
		</div>

		<div>
			<h3>
				Import a style
			</h3>
			<p>Styles are Mapbox GL styles in folders</p>
			<p>They end up in {{.StylesDirImportPath}}</p>
			<form>
				<label>
					Style folder
					<input type="file" />
				</label>
				<button type="submit">Go!</button>
			</form>
		</div>
	</body>
</html>
`
