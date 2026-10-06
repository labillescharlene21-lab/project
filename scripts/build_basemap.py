"""Build docs/data/basemap.js: the dark-slate map base for the GitHub Pages site.

Natural Earth (public domain) 1:50m countries and rivers, simplified and rounded so the whole base map
is small enough to ship with the site (no map-tile service needed, so the page works offline).

    python scripts/build_basemap.py            # downloads the two GeoJSON files, writes docs/data/basemap.js

Requires geopandas + shapely (already in requirements.txt). Run once; the output is committed.
"""
from __future__ import annotations

import json
import sys
import tempfile
import urllib.request
from pathlib import Path

import geopandas as gpd
import shapely

BASE = "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/"
FILES = {"countries": "ne_50m_admin_0_countries.geojson", "rivers": "ne_50m_rivers_lake_centerlines.geojson"}
TOLERANCE = {"countries": 0.04, "rivers": 0.03}      # degrees (~4 km): invisible at the zoom levels the map allows
GRID = 0.01                                          # coordinate precision in degrees (~1 km)
OUT = Path(__file__).resolve().parents[1] / "docs" / "data" / "basemap.js"


def build(name: str, src: Path) -> dict:
    gdf = gpd.read_file(src)
    if name == "countries":
        gdf = gdf[gdf["ADMIN"] != "Antarctica"]
    geoms = []
    for g in gdf.geometry:
        g = shapely.set_precision(g.simplify(TOLERANCE[name], preserve_topology=True), GRID)
        if not g.is_empty:
            geoms.append(g)
    return {"type": "GeometryCollection", "geometries": [json.loads(shapely.to_geojson(g)) for g in geoms]}


def main() -> None:
    out = {}
    with tempfile.TemporaryDirectory() as tmp:
        for name, fname in FILES.items():
            src = Path(sys.argv[1]) / fname if len(sys.argv) > 1 else Path(tmp) / fname
            if not src.exists():
                urllib.request.urlretrieve(BASE + fname, src)
            out[name] = build(name, src)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text("window.BASEMAP = " + json.dumps(out, separators=(",", ":")) + ";\n", encoding="utf-8")
    print(f"{OUT} ({OUT.stat().st_size / 1024:.0f} KB)")


if __name__ == "__main__":
    main()
