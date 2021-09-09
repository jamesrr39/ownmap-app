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

```
select unnest(tags)['key'] from '*.parquet' where tags is not null limit 10;
```

### Profiling

go tool pprof --web ownmap-app /path/to/profile/cpu.pprof > profile_out.html

