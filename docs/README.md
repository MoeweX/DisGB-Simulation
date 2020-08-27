# Broker Visualization

A visualization of spatial data from the simulation.

## Info

Several data and Javascript libraries are loaded via CDN. These are

- d3js (version v5) via [https://d3js.org/d3.v5.min.js](https://d3js.org/d3.v5.min.js)
- topojson-client (version 3) via [https://unpkg.com/topojson-client@3](https://unpkg.com/topojson-client@3)
- versor (version 0.1.2) via [https://unpkg.com/versor@0.1.2](https://unpkg.com/versor@0.1.2)
- FileSaver.js (version 2.0.2) via [https://cdn.jsdelivr.net/npm/file-saver@2.0.2/dist/FileSaver.min.js](https://cdn.jsdelivr.net/npm/file-saver@2.0.2/dist/FileSaver.min.js)
- world-atlas (version 2) via [https://cdn.jsdelivr.net/npm/world-atlas@2](https://cdn.jsdelivr.net/npm/world-atlas@2)

The CSV file that is visualized needs to be semicolon-delimited. This corresponds to the output of the simulation.

## Prerequisites

Some data has to be loaded locally (input.csv in CSV folder). Because of security restrictions, only files from a web server
can be loaded. For the current configuration to work, a web server has to be started on port 8080 and be available via localhost.
This can be achieved using the following command, if Python 3 is available.

```bash
cd your/clone/of/this/repo/browser_visualization
python3 -m http.server 8080
```

Then navigate to [http://localhost:8080/visualization.html](http://localhost:8080/visualization.html) to see the visualization.

## Notes on Projections

Implemented all projections from [https://github.com/d3/d3-geo](https://github.com/d3/d3-geo) to be selectable via dropdown menu
(except for composite and Albers, since they do not make sense, and Conic Conformal, which behaves strangely).

More possible projections are listed at [https://github.com/d3/d3-geo-projection](https://github.com/d3/d3-geo-projection).
If any of these are to be implemented, an additional module from d3js (d3-geo-projection) has to be loaded inside of the HTML page.

## Notes on SVG Rendering

The visualization is rendered inside the browser as an SVG that can be downloaded thanks to FileSaver.js. The way the SVG is rendered
is not optimal however (it leaves emtpy paths for hidden elements), since it makes the code a little bit cleaner and saner.
You are advised to use other tools to further process or edit the SVG (like [Inkscape](https://inkscape.org/)) or use
an optimizer (like [svgcleaner](https://github.com/RazrFalcon/svgcleaner) or [SVGO](https://github.com/svg/svgo)).
