"""
Download TIGER AREALM (area landmarks) for every county in the study area,
filter to non-residential land (parks, cemeteries, airports, military),
union with the water mask, and save as data/clip_mask.geojson.

MTFCC codes kept (non-residential land):
  K2180-K2190  Parks / forest / recreation areas (all jurisdictional levels)
  K2191        Cemetery
  K2193        Airport polygon
  K2167        Military installation
"""

import zipfile
from pathlib import Path
import requests
import geopandas as gpd
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"

COUNTIES = [
    ("36", "005"), ("36", "047"), ("36", "061"), ("36", "081"), ("36", "085"),
    ("36", "119"), ("36", "059"),
    ("34", "003"), ("34", "017"), ("34", "039"), ("34", "023"),
]

KEEP_MTFCC = {
    "K2180", "K2181", "K2182", "K2183", "K2184",
    "K2185", "K2186", "K2187", "K2188", "K2189", "K2190",
    "K2191",  # Cemetery
    "K2193",  # Airport polygon
    "K2167",  # Military installation
}


def fetch_arealm(state, county):
    # TIGER 2020 publishes AREALM at the state level, not county.
    url = f"https://www2.census.gov/geo/tiger/TIGER2020/AREALM/tl_2020_{state}_arealm.zip"
    cache = DATA / f"arealm_{state}.zip"
    if not cache.exists():
        print(f"  downloading {url}")
        r = requests.get(url, timeout=120)
        if r.status_code != 200:
            print(f"    HTTP {r.status_code} - skipping {state}{county}")
            return None
        cache.write_bytes(r.content)
    return cache


def main():
    frames = []
    seen_states = set()
    for state, county in COUNTIES:
        if state in seen_states:
            continue
        seen_states.add(state)
        z = fetch_arealm(state, county)
        if z is None:
            continue
        try:
            g = gpd.read_file(f"zip://{z}")
        except Exception as e:
            print(f"  couldn't read {z}: {e}")
            continue
        if "MTFCC" not in g.columns:
            continue
        g = g[g["MTFCC"].isin(KEEP_MTFCC)].copy()
        if len(g) == 0:
            continue
        # Repair any invalid geometries before we union.
        g["geometry"] = g.geometry.buffer(0)
        g = g[~g.geometry.is_empty & g.geometry.is_valid]
        print(f"  state {state}: {len(g)} landmark polygons")
        frames.append(g[["MTFCC", "geometry"]])

    if not frames:
        print("no landmark polygons found")
        return
    landmarks = gpd.GeoDataFrame(pd.concat(frames, ignore_index=True), crs=frames[0].crs)

    # Also pull in the water mask
    water = gpd.read_file(DATA / "water_mask.geojson").to_crs(landmarks.crs)

    # Union everything. Clean invalid geometries first or shapely can throw
    # "unable to assign free hole to a shell" on duplicate/self-intersecting polys.
    landmarks["geometry"] = landmarks.geometry.buffer(0)
    water["geometry"] = water.geometry.buffer(0)
    combined = pd.concat([landmarks[["geometry"]], water[["geometry"]]], ignore_index=True)
    combined = gpd.GeoDataFrame(combined, crs=landmarks.crs)
    combined = combined[~combined.geometry.is_empty & combined.geometry.is_valid]
    dissolved = combined.dissolve().to_crs(epsg=4326)
    dissolved["geometry"] = dissolved.geometry.buffer(0)

    out = DATA / "clip_mask.geojson"
    dissolved.to_file(out, driver="GeoJSON")
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
