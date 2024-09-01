# ownmap

**Please note, this project is still being developed and hasn't reached a point where APIs are stable yet. This means that for the moment, interfaces, file formats, APIs can change without any notice.**

The aim of this project is to be able to provide a 1-file program that can generate "slippy" map tiles from openstreetmap PBF files. Ideally it should be able to run (at least serving map tiles) on low-end hard, with a Raspberry Pi-level computer being the target (ideally the Raspberry Pi Zero, but that might be a stretch considering how powerful the hardware is).

The project contains:

- A custom data file format, for storing data for quick retrieval and map tile creation. Parquet support is being worked [here](https://github.com/jamesrr39/ownmap-app/tree/parquet).
- Support for different style types: a custom in-built style and partial support for MapBoxGL styles (with the idea to also support CartoCSS in the future).
- A rasterer: for drawing tile images, given the data and style.
- A web server, for handling a request for fetching a tile, with a given style, and returning the drawn style.

## Screenshots

With the default style:

## Limitations

Amongst others:

- Coastlines are not handled.
- Limited support for styling.

## Dev setup

First you need to download an OSM extract in PBF format. Any compatible files from any website offering them will do. [Geofabrik](https://download.geofabrik.de/) offers some good extracts with small areas you can try out quickly.

Then you should place the downloaded file in `data/sample-pbf-file.pbf`. Alternatively, you could place a symlink here to another file on the disk.

Then run `make run_dev_import`. This will read the pbf file and create a `ownmapdb` file. This contains information from the pbf file, but also sorts the items and contains an index to find things more efficiently given a geographic area. The importer has a progress indicator, so you can calculate roughly how long it will take.

You can then run `make run_dev_server__basic_style`. This will start a web server. In the logs you can see the address that it is serving on. Open up a web browser and go to that address. You will see an interactive slippy map with tiles being served from your tileserver.

Here's an example:

```
mkdir -p data
wget -O data/sample-pbf-file.pbf https://download.geofabrik.de/europe/united-kingdom/england/buckinghamshire-latest.osm.pbf
make run_dev_import
run_dev_server__basic_style
# now open your web browser and navigate to http://localhost:9000
```

### Profiling

go tool pprof --web ownmap-app /path/to/profile/cpu.pprof > profile_out.html
