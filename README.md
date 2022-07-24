# ownmap

**Please note, the status of this project is "pre-alpha testing development". This means that interfaces, file formats, APIs can (and will) change on a regular basis without any notice.**

The aim of this project is to be able to provide a 1-file program that can generate "slippy" maps from openstreetmap PBF files. Ideally it should be able to run (at least serving map tiles) on low-end hard, with a Raspberry Pi-level computer being the target (ideally the Raspberry Pi Zero, but that might be a stretch considering how powerful the hardware is).

The project contains:

- A custom data file format, for storing data for quick retrieval and map tile creation;
- Support for different style types: a custom in-built style and partial support for MapBoxGL styles (with the idea to also support CartoCSS in the future).
- A rasterer: for drawing tile images, given the data and style.
- A web server, for handling a request for fetching a tile, with a given style, and returning the drawn style.

## Dev setup

First you need to download an OSM extract in PBF format. Any compatible files from any website offering them will do. [Geofabrik](https://download.geofabrik.de/) offers some good extracts with small areas you can try out quickly.

Then you should place the downloaded file in `data/sample-pbf-file.pbf`. (Alternatively, you could place a symlink here to another file on the disk.

Then run `make run_dev_import`. This will read the pbf file and create a `ownmapdb` file. This contains information from the pbf file, but also sorts the items and contains an index to find things more efficiently given a geographic area.

You can then run `make run_dev_server__basic_style`. This will start a web server. In the logs you can see the address that it is serving on. Open up a web browser and go to that address. You will see an interactive slippy map with tiles being served from your tileserver.

### parquet

DuckDB is a great tool to debug your parquet files with.

Install it, and then change directory in the terminal to the directory where you have the parquet files. Then run `duckdb` and try out these example queries:

```
-- Look at how duckdb views the files

SELECT * FROM parquet_schema('relations.parquet');
SELECT * FROM parquet_metadata('relations.parquet');

-- Try a query with a where clause on latitude. Extract the key and value of the tags.

SELECT UNNEST(tags)['key'], UNNEST(tags)['value'], id, lat, lon FROM 'nodes.parquet' WHERE lat > 12.34 LIMIT 100;

-- See how duckdb queries our parquet files:

EXPLAIN ANALYZE SELECT id, UNNEST(tags)['key'] FROM 'nodes.parquet' WHERE tags IS NOT NULL AND lat > 12.34 LIMIT 10;

-- Use a Common Table Expression (CTE) to query by tag keys and values

WITH unnested AS (SELECT id, UNNEST(tags)['key'] AS key, UNNEST(tags)['value'] AS value FROM 'ways.parquet' WHERE tags IS NOT NULL)
SELECT * FROM unnested WHERE key = 'highway' AND value IN ('motorway', 'primary');

-- Use `list_has` to query tags by a duckdb `struct`

SELECT * FROM 'ways.parquet' WHERE list_has(tags, {'key': 'highway', 'value': 'motorway'});

```

The following queries were run with version [v0.4.0 da9ee490d](https://github.com/duckdb/duckdb/releases/tag/v0.4.0) of duckdb. If you have any problems, please try at least this or a later version of duckdb. (0.2.9 for example, will not run all the queries successfully.)

### Profiling

go tool pprof --web ownmap-app /path/to/profile/cpu.pprof > profile_out.html

### TODO

- [ ] https://github.com/adjust/parquet_fdw/
- [ ] https://github.com/citusdata/cstore_fdw
- [ ] https://github.com/cyclosm/cyclosm-cartocss-style
