"""
Build area-weighted crosswalks from historical tract geometries (1970, 1980,
1990, 2000) to 2010 census tracts, then redistribute NHGIS D08 child counts
through those crosswalks so every decade has a value expressed on 2010
boundaries.

Output:
  data/normalized_counts.json
    { "1970": {gisjoin2010 -> count}, "1980": ..., ... }

Method
------
For each source decade and each source tract:
    for each 2010 tract it overlaps:
        weight = overlap_area / source_tract_area
        allocated_count = source_tract_count * weight
Sum across source tracts -> 2010-tract-indexed value.

This is AREA-weighted, not population-weighted (which would use a 2010 block
population layer to do better in tracts with uneven density). Area weighting
is a standard approximation that works well for tracts of similar population
density but can err when a tract was split unequally. Labeled as such in the
methodology panel.
"""

import json
import zipfile
from pathlib import Path

import pandas as pd
import geopandas as gpd

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
SHAPES_DIR = DATA / "nhgis_shapes"

WANTED_STATES = {"36", "34"}
WANTED_COUNTIES = {
    "36005", "36047", "36061", "36081", "36085", "36119", "36059",
    "34003", "34017", "34039", "34023",
}

DECADES = [1970, 1980, 1990, 2000]  # 2010 and 2020 are already on target grid


def find_shape(year):
    """Locate the NHGIS unzipped shapefile for a given year's US_tract file."""
    # Files look like: US_tract_1970_conflated.shp or similar. Find by year.
    candidates = list(SHAPES_DIR.rglob(f"*tract_{year}*.shp"))
    if not candidates:
        candidates = list(SHAPES_DIR.rglob(f"*{year}*tract*.shp"))
    if not candidates:
        candidates = list(SHAPES_DIR.rglob(f"US_tract_{year}*.shp"))
    if not candidates:
        # Some NHGIS extracts unzip an intermediate zip per year.
        inner = list(SHAPES_DIR.rglob(f"*{year}*.zip"))
        for iz in inner:
            with zipfile.ZipFile(iz) as z:
                z.extractall(iz.parent)
        candidates = list(SHAPES_DIR.rglob(f"*tract_{year}*.shp"))
    if not candidates:
        raise FileNotFoundError(f"no shapefile found for {year}")
    return candidates[0]


def load_source(year):
    path = find_shape(year)
    g = gpd.read_file(path)
    if g.crs is None:
        g = g.set_crs(epsg=4326)
    # Normalize GISJOIN column
    cols = {c.upper(): c for c in g.columns}
    gjcol = cols.get("GISJOIN") or cols.get("GISJOIN1") or cols.get(f"GISJOIN{year}")
    if gjcol:
        g["GISJOIN"] = g[gjcol]
    elif "STATE" in cols:
        # Fabricate GISJOIN
        g["GISJOIN"] = ("G" + g[cols["STATE"]].astype(str)
                        + "0" + g[cols["COUNTY"]].astype(str).str.zfill(3)
                        + "0" + g[cols["TRACT"]].astype(str).str.replace(".","",regex=False).str.zfill(6))
    # Filter to our states
    if "STATE" in cols:
        g = g[g[cols["STATE"]].isin(WANTED_STATES)]
    elif "STATEFP" in cols:
        g = g[g[cols["STATEFP"]].isin(WANTED_STATES)]
    else:
        # Use GISJOIN prefix
        g = g[g["GISJOIN"].str[1:3].isin(WANTED_STATES)]
    # Further filter to our counties
    g = g[g["GISJOIN"].str[1:3] + g["GISJOIN"].str[4:7]].isin(WANTED_COUNTIES) if False else \
        g[(g["GISJOIN"].str[1:3] + g["GISJOIN"].str[4:7]).isin(WANTED_COUNTIES)]
    return g[["GISJOIN", "geometry"]].to_crs(epsg=2263)


def load_target_2010():
    """Load 2010 tract geometries already downloaded (state-level cb files)."""
    frames = []
    for s in ["tracts_2010_36.zip", "tracts_2010_34.zip"]:
        g = gpd.read_file(f"zip://{DATA/s}")
        if g.crs is None:
            g = g.set_crs(epsg=4269)
        cols = {c.upper(): c for c in g.columns}
        g["STATEFP"] = g[cols["STATE"]]
        g["COUNTYFP"] = g[cols["COUNTY"]]
        g["TRACTCE"] = g[cols["TRACT"]].astype(str).str.zfill(6)
        g["GISJOIN"] = "G" + g["STATEFP"] + "0" + g["COUNTYFP"] + "0" + g["TRACTCE"]
        g = g[(g["STATEFP"] + g["COUNTYFP"]).isin(WANTED_COUNTIES)]
        g = g.dissolve(by="GISJOIN", as_index=False)
        frames.append(g[["GISJOIN", "geometry"]])
    out = gpd.GeoDataFrame(pd.concat(frames, ignore_index=True), crs=frames[0].crs)
    return out.to_crs(epsg=2263)


def load_nhgis_counts():
    """Load the already-downloaded D08 CSV, keyed by each decade's native GISJOIN."""
    df = pd.read_csv(
        DATA / "nhgis_unpacked/nhgis0001_csv/nhgis0001_ts_nominal_tract.csv",
        dtype=str,
    )
    # Each row is a tract history. Use each decade's native GISJOIN.
    out = {}
    for d in DECADES + [2010, 2020]:
        m = {}
        for _, r in df.iterrows():
            gj = r.get(f"GJOIN{d}")
            if gj and pd.notna(gj):
                v = pd.to_numeric(r[f"D08AA{d}"], errors="coerce")
                if pd.notna(v):
                    m[gj] = float(v)
        out[d] = m
    return out


def build_crosswalk(source_gdf, target_gdf):
    """Area-weighted crosswalk. Returns list of (src_gj, tgt_gj, weight)."""
    # Intersect sjoin on geometry
    source = source_gdf.copy()
    source["src_area"] = source.geometry.area
    # Use overlay for accurate intersection areas
    inter = gpd.overlay(source, target_gdf, how="intersection", keep_geom_type=False)
    inter["int_area"] = inter.geometry.area
    inter["weight"] = inter["int_area"] / inter["src_area"]
    inter = inter[["GISJOIN_1", "GISJOIN_2", "weight"]].rename(
        columns={"GISJOIN_1": "src", "GISJOIN_2": "tgt"}
    )
    return inter


def main():
    print("loading 2010 targets...")
    tgt = load_target_2010()
    print(f"  {len(tgt)} 2010 tracts")

    print("loading D08 counts...")
    counts = load_nhgis_counts()
    for d in DECADES + [2010, 2020]:
        print(f"  {d}: {len(counts[d])} tracts with data")

    normalized = {}
    for d in DECADES:
        print(f"\n== {d} ==")
        src = load_source(d)
        print(f"  source tracts: {len(src)}")
        cw = build_crosswalk(src, tgt)
        print(f"  crosswalk rows: {len(cw)}")
        # Redistribute
        tgt_totals = {}
        for _, row in cw.iterrows():
            c = counts[d].get(row["src"])
            if c is None:
                continue
            tgt_totals[row["tgt"]] = tgt_totals.get(row["tgt"], 0) + c * row["weight"]
        normalized[d] = tgt_totals
        print(f"  normalized to {len(tgt_totals)} 2010 tracts, total = {sum(tgt_totals.values()):,.0f}")

    # 2010, 2020 are natively on 2010/2020 boundaries. For 2010 we use as-is.
    # For 2020, attempt a nominal join (most GEOIDs unchanged) - leave in counts dict.
    normalized[2010] = counts[2010]
    normalized[2020] = counts[2020]

    # Write output
    out = {str(d): v for d, v in normalized.items()}
    (DATA / "normalized_counts.json").write_text(json.dumps(out))
    print("\nwrote data/normalized_counts.json")


if __name__ == "__main__":
    main()
