// function getTileUrls(map, bounds, tileLayer, zoom) {
//     var min = map.project(bounds.getNorthWest(), zoom).divideBy(256).floor(),
//         max = map.project(bounds.getSouthEast(), zoom).divideBy(256).floor(),
//         urls = [];

//     for (var i = min.x; i <= max.x; i++) {
//         for (var j = min.y; j <= max.y; j++) {
//             var coords = new L.Point(i, j);
//             coords.z = zoom;
//             urls.push(tileLayer.getTileUrl(coords));
//         }
//     }

//     return urls;
// }

function handleError(reason) {
    document.getElementById('mapid').innerText = `failed to get bounds. Reason: "${reason}"`;
}

function getWindowKVPairs() {
    const winHashKVs = {};
    window.location.hash.replace('#', '').split('&').forEach(kvPair => {
        const idxEqual = kvPair.indexOf('=');

        const key = decodeURI(kvPair.substring(0, idxEqual));
        const value = decodeURI(kvPair.substring(idxEqual +1));

        if (!key) {
            // empty string
            return;
        }
        winHashKVs[key] = value;
    })

    return winHashKVs;
}

function getWindowHashLocation() {
    const winKVPairs = getWindowKVPairs()
    if (!winKVPairs.map) {
        return undefined;
    }

    const mapFragments = winKVPairs.map.split('/');
    if (mapFragments.length !== 3) {
        throw new Error('map fragments not 3 items');
    }

    const zoom = parseFloat(mapFragments[0]);
    const lat = parseFloat(mapFragments[1]);
    const lng = parseFloat(mapFragments[2]);
    return {
        zoom,
        lat,
        lng,
    }
}

function setWindowHashLocation(location) {
    if (!location.zoom || !location.lat || !location.lng) {
        console.error('zoom, lat and lng must be set. Location object: ', location);
        return;
    }

    const locationValue = `${location.zoom}/${location.lat.toFixed(4)}/${location.lng.toFixed(4)}`;

    const existingKVPairs = getWindowKVPairs();
    existingKVPairs.map = locationValue;

    const keys = Object.keys(existingKVPairs);
    keys.sort().reverse();
    const kvPairs = keys.map(key => {
        return `${encodeURI(key)}=${encodeURI(existingKVPairs[key])}`;
    });

    window.location.hash = `#${kvPairs.join('&')}`;
}

fetch('/api/info').then(resp => {
    return resp.json().then(info => {
        const optionsEl = info.style.styleIds.map(styleId => {
            const optionEl = document.createElement('option');
            optionEl.selected = styleId == info.style.defaultStyleId;

            optionEl.value = styleId;
            optionEl.innerText = styleId;
            return optionEl;
        });

        const stylePickerEl = document.getElementById('style-picker');
        optionsEl.forEach(optionEl => stylePickerEl.appendChild(optionEl));

        // https://leafletjs.com/reference-1.4.0.html#tilelayer
        const tileLayer = L.tileLayer('/api/tiles/raster/{z}/{x}/{y}', {
            attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
            maxZoom: 21,
        });

        stylePickerEl.addEventListener('change', event => {
            tileLayer.setUrl(`/api/tiles/raster/{z}/{x}/{y}?styleId=${event.target.value}`);
        });

        let maxBounds = [[-90, -180], [90, 180]];
        if (info.datasets.length == 0) {
            throw new Error('No datasets found. Have you imported at least one dataset into a supported database or file?');
        }

        const {minLat, maxLat, minLon: minlng, maxLon: maxlng} = info.datasets[0].bounds;
        maxBounds = [
            [minLat, minlng],
            [maxLat, maxlng],
        ];

        let defaultCoords = [(maxLat + minLat) / 2, (maxlng + minlng) / 2];
        let defaultZoom = 13;

        const windowHash = getWindowHashLocation();
        if (windowHash && windowHash) {
            defaultCoords = [windowHash.lat, windowHash.lng];
            defaultZoom = windowHash.zoom
        }

        var mymap = L.map('mapid', {
            maxBounds,
        }).setView(defaultCoords, defaultZoom);

        mymap.addEventListener('moveend', function(ev) {
            const center = ev.target.getCenter()

            setWindowHashLocation({
                zoom: mymap.getZoom(),
                ...ev.target.getCenter(),
            });
        });

        mymap.addEventListener('zoomend', function(ev) {
            const zoom = ev.target.getZoom();

            setWindowHashLocation({
                zoom,
                ...mymap.getCenter(),
            });
        });

        mymap.on('click', (event) => {
            debugInfoEl = document.getElementById('debug-info');
            debugInfoEl.innerHTML = '';

            const coordsDataEl = document.createElement('p');
            coordsDataEl.innerText = `lat: ${event.latlng.lat}, lng: ${event.latlng.lng}`;
            debugInfoEl.appendChild(coordsDataEl);

            const zoom = mymap.getZoom();
            const latlng = event.latlng;

            var tileCoords = mymap.project(latlng, zoom).divideBy(256).floor();

            const point = new L.Point(tileCoords.x, tileCoords.y);
            
            const tileUrl = (tileLayer.getTileUrl({
                ...point,
                z: zoom,
            }));

            const tileLinkEl = document.createElement('a');
            tileLinkEl.href = tileUrl;
            tileLinkEl.innerText = tileUrl;
            tileLinkEl.style.color = 'white';
            debugInfoEl.appendChild(tileLinkEl);
        })
        
        tileLayer.addTo(mymap);

        window.addEventListener('hashchange', () => {
            const windowHashLocation = getWindowHashLocation()
            mymap.setView([windowHashLocation.lat, windowHashLocation.lng], windowHashLocation.zoom);
        })
    })
}).catch(handleError);
