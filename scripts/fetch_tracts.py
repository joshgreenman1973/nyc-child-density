"""
Download cartographic boundary tract shapefiles for NYC for 2000, 2010, 2020,
clip to NYC counties, compute land-area (sq mi), and emit per-year GeoJSON
joined with the under-18 counts we already pulled.

Output: web/tracts_{year}.geojson, each feature with
  geoid, under18, land_sqmi, density (kids per sq mi)
"""

import io
import json
import zipfile
from pathlib import Path

import requests
import geopandas as gpd
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
WEB = ROOT / "web"
WEB.mkdir(exist_ok=True)

# NYC + bordering counties, keyed by state FIPS.
STATE_COUNTIES = {
    "36": {"005", "047", "061", "081", "085", "119", "059"},
    "34": {"003", "017", "039", "023"},
}

# Cartographic boundary shapefiles per state, per vintage.
URLS = {
    2000: {
        "36": "https://www2.census.gov/geo/tiger/PREVGENZ/tr/tr00shp/tr36_d00_shp.zip",
        "34": "https://www2.census.gov/geo/tiger/PREVGENZ/tr/tr00shp/tr34_d00_shp.zip",
    },
    2010: {
        "36": "https://www2.census.gov/geo/tiger/GENZ2010/gz_2010_36_140_00_500k.zip",
        "34": "https://www2.census.gov/geo/tiger/GENZ2010/gz_2010_34_140_00_500k.zip",
    },
    2020: {
        "36": "https://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_36_tract_500k.zip",
        "34": "https://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_34_tract_500k.zip",
    },
}


def download_shp(url, cache_name):
    cache = DATA / cache_name
    if cache.exists():
        return cache
    print(f"  downloading {url}")
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    cache.write_bytes(r.content)
    return cache


def load_tracts(year):
    frames = []
    for state, url in URLS[year].items():
        zpath = download_shp(url, f"tracts_{year}_{state}.zip")
        frames.append(_load_one(zpath, year, state))
    out = gpd.GeoDataFrame(pd.concat(frames, ignore_index=True), crs=frames[0].crs)
    return out


def _load_one(zpath, year, state):
    gdf = gpd.read_file(f"zip://{zpath}")
    cols = {c.upper(): c for c in gdf.columns}
    if year == 2000:
        # 2000 PREVGENZ: TRACT is 4-char (tract with no decimal, e.g. "0297")
        # or 6-char (tract.decimal packed, e.g. "024302" = 243.02).
        # API returns 6-char form. Pad 4-char entries with "00".
        gdf["STATEFP"] = gdf[cols["STATE"]]
        gdf["COUNTYFP"] = gdf[cols["COUNTY"]]
        tract = gdf[cols["TRACT"]].astype(str).str.replace(".", "", regex=False)
        tract = tract.where(tract.str.len() == 6, tract + "00")
        gdf["TRACTCE"] = tract
    elif year == 2010:
        # GENZ2010: STATE, COUNTY, TRACT
        gdf["STATEFP"] = gdf[cols["STATE"]]
        gdf["COUNTYFP"] = gdf[cols["COUNTY"]]
        gdf["TRACTCE"] = gdf[cols["TRACT"]].astype(str).str.zfill(6)
    else:  # 2020
        gdf["STATEFP"] = gdf["STATEFP"]
        gdf["COUNTYFP"] = gdf["COUNTYFP"]
        gdf["TRACTCE"] = gdf["TRACTCE"]
    wanted = STATE_COUNTIES[state]
    gdf = gdf[(gdf["STATEFP"] == state) & (gdf["COUNTYFP"].isin(wanted))].copy()
    gdf["geoid"] = gdf["STATEFP"] + gdf["COUNTYFP"] + gdf["TRACTCE"]
    if gdf.crs is None:
        gdf = gdf.set_crs(epsg=4269)
    gdf = gdf.dissolve(by="geoid", as_index=False)
    return gdf[["geoid", "geometry"]]


def main():
    for year in [2000, 2010, 2020]:
        print(f"{year}...")
        gdf = load_tracts(year)
        # land area in sq miles (project to NY State Plane Long Island, ft)
        gdf_proj = gdf.to_crs(epsg=2263)
        gdf["land_sqmi"] = gdf_proj.geometry.area / 27_878_400  # sqft -> sqmi

        # Join under-18 counts
        counts = pd.read_json(DATA / f"under18_{year}.json")
        counts["geoid"] = counts["geoid"].astype(str)
        merged = gdf.merge(counts[["geoid", "under18"]], on="geoid", how="left")
        merged["under18"] = merged["under18"].fillna(0).astype(int)
        merged["density"] = merged.apply(
            lambda r: round(r["under18"] / r["land_sqmi"], 1) if r["land_sqmi"] > 0.005 else 0,
            axis=1,
        )

        # Simplify geometry for web
        merged_wgs = merged.to_crs(epsg=4326)
        merged_wgs["geometry"] = merged_wgs.geometry.simplify(0.0001, preserve_topology=True)

        out = WEB / f"tracts_{year}.geojson"
        merged_wgs[["geoid", "under18", "land_sqmi", "density", "geometry"]].to_file(
            out, driver="GeoJSON"
        )
        total = merged_wgs["under18"].sum()
        print(f"  wrote {out.name}  tracts={len(merged_wgs)}  children={total:,}")


if __name__ == "__main__":
    main()
