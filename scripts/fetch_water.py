"""
Download TIGER AREAWATER shapefiles for every county in the study area,
union into a single water mask, and save as data/water_mask.geojson.
"""

import zipfile
from pathlib import Path
import requests
import geopandas as gpd
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
DATA.mkdir(exist_ok=True)

COUNTIES = [
    ("36", "005"), ("36", "047"), ("36", "061"), ("36", "081"), ("36", "085"),
    ("36", "119"), ("36", "059"),
    ("34", "003"), ("34", "017"), ("34", "039"), ("34", "023"),
]

def fetch(state, county):
    url = f"https://www2.census.gov/geo/tiger/TIGER2020/AREAWATER/tl_2020_{state}{county}_areawater.zip"
    cache = DATA / f"areawater_{state}{county}.zip"
    if not cache.exists():
        print(f"  downloading {url}")
        r = requests.get(url, timeout=120)
        r.raise_for_status()
        cache.write_bytes(r.content)
    return cache

def main():
    frames = []
    for state, county in COUNTIES:
        z = fetch(state, county)
        g = gpd.read_file(f"zip://{z}")
        frames.append(g[["geometry"]])
    all_water = gpd.GeoDataFrame(pd.concat(frames, ignore_index=True), crs=frames[0].crs)
    print(f"total water polygons: {len(all_water)}")
    # Dissolve to a single multipolygon for efficient clipping
    dissolved = all_water.dissolve()
    dissolved = dissolved.to_crs(epsg=4326)
    out = DATA / "water_mask.geojson"
    dissolved.to_file(out, driver="GeoJSON")
    print(f"wrote {out}")

if __name__ == "__main__":
    main()
