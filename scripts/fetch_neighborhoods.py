"""
Build a tract -> neighborhood-name lookup for the study area.

Sources:
  - NYC 5 boroughs: NYC Planning NTA 2020 boundaries (~260 neighborhoods,
    e.g. "Williamsburg", "Washington Heights South").
  - Non-NYC counties (Westchester, Nassau, Bergen, Hudson, Union, Middlesex):
    TIGER PLACE 2020 (incorporated + census-designated places,
    e.g. "Hoboken city", "Yonkers city", "White Plains city").

Output: docs/neighborhoods.json  { gisjoin2010 -> "Williamsburg" }
"""

import json
import zipfile
from pathlib import Path

import requests
import geopandas as gpd
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
WEB = ROOT / "docs"

NYC_COUNTIES = {"36005", "36047", "36061", "36081", "36085"}
ALL_COUNTIES = NYC_COUNTIES | {"36119", "36059", "34003", "34017", "34039", "34023"}

NTA_URL = "https://data.cityofnewyork.us/api/geospatial/9nt8-h7nd?method=export&format=GeoJSON"


def load_tracts():
    frames = []
    for s in ["tracts_2010_36.zip", "tracts_2010_34.zip"]:
        g = gpd.read_file(f"zip://{DATA/s}")
        if g.crs is None:
            g = g.set_crs(epsg=4269)
        cols = {c.upper(): c for c in g.columns}
        g["STATEFP"] = g[cols["STATE"]]
        g["COUNTYFP"] = g[cols["COUNTY"]]
        g["TRACTCE"] = g[cols["TRACT"]].astype(str).str.zfill(6)
        g["gisjoin"] = "G" + g["STATEFP"] + "0" + g["COUNTYFP"] + "0" + g["TRACTCE"]
        g["geoid"] = g["STATEFP"] + g["COUNTYFP"] + g["TRACTCE"]
        g = g[(g["STATEFP"] + g["COUNTYFP"]).isin(ALL_COUNTIES)]
        g = g.dissolve(by="gisjoin", as_index=False)
        frames.append(g[["gisjoin", "geoid", "STATEFP", "COUNTYFP", "geometry"]])
    return gpd.GeoDataFrame(pd.concat(frames, ignore_index=True), crs=frames[0].crs)


def load_nta():
    cache = DATA / "nyc_nta_2020.geojson"
    if not cache.exists():
        print("  downloading NYC NTA 2020...")
        r = requests.get(NTA_URL, timeout=120)
        r.raise_for_status()
        cache.write_bytes(r.content)
    g = gpd.read_file(cache)
    # The NTA dataset uses lower-case field names. Find the name column.
    name_col = None
    for c in g.columns:
        if c.lower() in ("ntaname", "nta_name", "ntaname2020"):
            name_col = c; break
    if name_col is None:
        # Fall back: first string column that isn't a code
        for c in g.columns:
            if g[c].dtype == object and "name" in c.lower():
                name_col = c; break
    g = g[[name_col, "geometry"]].rename(columns={name_col: "nbhd"})
    return g


def load_places():
    """TIGER 2020 PLACE for NY and NJ."""
    frames = []
    for state in ["36", "34"]:
        cache = DATA / f"tl_2020_{state}_place.zip"
        if not cache.exists():
            url = f"https://www2.census.gov/geo/tiger/TIGER2020/PLACE/tl_2020_{state}_place.zip"
            print(f"  downloading {url}")
            r = requests.get(url, timeout=180)
            r.raise_for_status()
            cache.write_bytes(r.content)
        g = gpd.read_file(f"zip://{cache}")
        cols = {c.upper(): c for c in g.columns}
        name = cols.get("NAME") or cols.get("NAMELSAD")
        g = g[[name, "geometry"]].rename(columns={name: "nbhd"})
        frames.append(g)
    return gpd.GeoDataFrame(pd.concat(frames, ignore_index=True), crs=frames[0].crs)


def main():
    print("loading tracts...")
    tracts = load_tracts()
    print(f"  {len(tracts)} tracts")

    # Use centroids for point-in-polygon join.
    cent = tracts.copy()
    cent["geometry"] = cent.geometry.representative_point()

    print("loading NYC NTAs...")
    nta = load_nta().to_crs(tracts.crs)

    print("loading TIGER places (NY + NJ)...")
    places = load_places().to_crs(tracts.crs)

    # NYC tracts -> NTA
    nyc_mask = (cent["STATEFP"] + cent["COUNTYFP"]).isin(NYC_COUNTIES)
    nyc_cent = cent[nyc_mask].copy()
    non_cent = cent[~nyc_mask].copy()

    nyc_join = gpd.sjoin(nyc_cent[["gisjoin", "geometry"]], nta[["nbhd", "geometry"]],
                         how="left", predicate="within")
    non_join = gpd.sjoin(non_cent[["gisjoin", "geometry"]], places[["nbhd", "geometry"]],
                         how="left", predicate="within")

    out = {}
    for df in (nyc_join, non_join):
        for _, r in df.iterrows():
            gj = r["gisjoin"]
            name = r.get("nbhd")
            if pd.isna(name) or not name:
                continue
            # Prefer the first hit if a centroid lies on a boundary.
            if gj in out:
                continue
            # Clean trailing "city/township/borough" noise from places a little.
            clean = str(name).replace(" city", "").replace(" township", "") \
                             .replace(" borough", "").replace(" town", "").strip()
            out[gj] = clean

    (WEB / "neighborhoods.json").write_text(json.dumps(out, separators=(",", ":")))
    print(f"wrote {WEB/'neighborhoods.json'} ({len(out)} tracts labeled)")
    missing = [t["gisjoin"] for _, t in tracts.iterrows() if t["gisjoin"] not in out]
    print(f"  {len(missing)} tracts have no neighborhood match")


if __name__ == "__main__":
    main()
