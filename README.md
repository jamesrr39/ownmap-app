# ownmap

The aim of this project is to be able to provide a 1-file program that can generate "slippy" maps from openstreetmap PBF files.

The program should run on hardware ranging from a Raspberry Pi, through to a typical personal laptop (and higher-end machines).

Intermediary data files should be in a size range that can reasonably be expected to fit on a personal computer. The target for the project is to be able to render the "Europe" extract from Geofabrik (although similar size and smaller regions should also work just as well).

## Dev settings

### Profiling

go tool pprof --web ownmap-app /path/to/profile/cpu.pprof > profile_out.html
