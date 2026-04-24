"""
Build the web-ready time series: 1970-2023, yearly, one value per 2010-tract.

Sources
-------
- 1970, 1980, 1990, 2000, 2010, 2020 : NHGIS D08AA (nominal tract join)
- 2011-2023                          : ACS 5-year B01001 sums, exact GEOID match
                                       (2011-2019 use 2010 tracts; 2020-2023 use
                                       2020 tracts - we match where GEOID is
                                       unchanged, otherwise fall back to
                                       interpolation/decennial)

Between years that have real measurements, we linearly interpolate.
Post-2010 years where we have ACS become real anchors; years without become
interpolated between ACS + decennial.
"""

import json
from pathlib import Path

import pandas as pd
import geopandas as gpd

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
WEB = ROOT / "web"

WANTED_COUNTIES = {
    ("36", "005"), ("36", "047"), ("36", "061"), ("36", "081"), ("36", "085"),
    ("36", "119"), ("36", "059"),
    ("34", "003"), ("34", "017"), ("34", "039"), ("34", "023"),
}

YEAR_MIN = 1970
YEAR_MAX = 2023


def load_nhgis():
    """Returns: {gisjoin_2010 : {1970: v, 1980: v, ...}}"""
    df = pd.read_csv(
        DATA / "nhgis_unpacked/nhgis0001_csv/nhgis0001_ts_nominal_tract.csv",
        dtype=str,
    )
    df = df[df.apply(lambda r: (r["STATEFP"], r["COUNTYFP"]) in WANTED_COUNTIES, axis=1)].copy()
    out = {}
    for _, r in df.iterrows():
        gj = r.get("GJOIN2010") or r.get("GJOIN2020")
        if not gj:
            continue
        vals = {}
        for d in [1970, 1980, 1990, 2000, 2010, 2020]:
            v = pd.to_numeric(r[f"D08AA{d}"], errors="coerce")
            if pd.notna(v):
                vals[d] = float(v)
        if vals:
            out[gj] = vals
    return out


def load_acs():
    """Returns: {geoid11 : {endyear: under18}}"""
    out = {}
    for ey in range(2011, 2024):
        path = DATA / f"acs5_under18_{ey}.json"
        rows = json.loads(path.read_text())
        for r in rows:
            if r["under18"] is None:
                continue
            out.setdefault(r["geoid"], {})[ey] = r["under18"]
    return out


def load_geom():
    frames = []
    for state_zip in ["tracts_2010_36.zip", "tracts_2010_34.zip"]:
        g = gpd.read_file(f"zip://{DATA / state_zip}")
        if g.crs is None:
            g = g.set_crs(epsg=4269)
        cols = {c.upper(): c for c in g.columns}
        g["STATEFP"] = g[cols["STATE"]]
        g["COUNTYFP"] = g[cols["COUNTY"]]
        g["TRACTCE"] = g[cols["TRACT"]].astype(str).str.zfill(6)
        g["geoid"] = g["STATEFP"] + g["COUNTYFP"] + g["TRACTCE"]
        g["gisjoin"] = "G" + g["STATEFP"] + "0" + g["COUNTYFP"] + "0" + g["TRACTCE"]
        g = g[g.apply(lambda r: (r["STATEFP"], r["COUNTYFP"]) in WANTED_COUNTIES, axis=1)].copy()
        g = g.dissolve(by="gisjoin", as_index=False)
        frames.append(g[["gisjoin", "geoid", "geometry"]])
    out = gpd.GeoDataFrame(pd.concat(frames, ignore_index=True), crs=frames[0].crs)
    proj = out.to_crs(epsg=2263)
    out["land_sqmi"] = proj.geometry.area / 27_878_400
    return out


def interpolate(year_values):
    """
    year_values: dict {year : value}  (sparse anchors)
    returns list of values from YEAR_MIN to YEAR_MAX with:
      - exact value at each anchor
      - linear interpolation between consecutive anchors
      - extrapolation held constant at the nearest anchor past the edges
    Returns None if no anchors.
    """
    if not year_values:
        return None
    years_sorted = sorted(year_values.keys())
    out = []
    for y in range(YEAR_MIN, YEAR_MAX + 1):
        if y in year_values:
            out.append(year_values[y])
            continue
        # find neighbors
        lo = [a for a in years_sorted if a < y]
        hi = [a for a in years_sorted if a > y]
        if lo and hi:
            y0 = lo[-1]; y1 = hi[0]
            v0 = year_values[y0]; v1 = year_values[y1]
            t = (y - y0) / (y1 - y0)
            out.append(v0 + (v1 - v0) * t)
        elif lo:
            out.append(year_values[lo[-1]])
        else:
            out.append(year_values[hi[0]])
    return out


def main():
    print("loading NHGIS anchors...")
    nh = load_nhgis()
    print(f"  {len(nh)} tracts with NHGIS data")

    print("loading ACS annual data...")
    acs = load_acs()
    print(f"  {len(acs)} tracts with ACS data")

    print("loading 2010 geometry...")
    geom = load_geom()
    print(f"  {len(geom)} 2010 tracts")

    print("merging anchors per tract...")
    tracts_series = {}
    features = []
    real_anchors = {}  # for each tract, record which years are REAL (not interpolated)

    for _, row in geom.iterrows():
        gj = row["gisjoin"]
        geoid = row["geoid"]
        area = row["land_sqmi"]
        if area <= 0.005:
            continue

        anchors = {}
        # Decennial anchors from NHGIS
        if gj in nh:
            anchors.update(nh[gj])
        # ACS annual (use GEOID11 - same as our 2010 geoid)
        if geoid in acs:
            for ey, v in acs[geoid].items():
                anchors[ey] = float(v)

        if not anchors:
            continue

        series = interpolate(anchors)
        if series is None:
            continue

        tracts_series[gj] = [round(v, 1) for v in series]
        real_anchors[gj] = sorted(anchors.keys())

        features.append({
            "type": "Feature",
            "geometry": json.loads(
                gpd.GeoSeries([row["geometry"]], crs=geom.crs)
                .to_crs(epsg=4326)
                .simplify(0.0001, preserve_topology=True)
                .to_json()
            )["features"][0]["geometry"],
            "properties": {
                "gisjoin": gj,
                "geoid": geoid,
                "land_sqmi": round(area, 3),
            },
        })

    print(f"  {len(features)} tracts with complete time series")

    # Write base geom
    (WEB / "tracts_base.geojson").write_text(
        json.dumps({"type": "FeatureCollection", "features": features})
    )

    # Write density time series (density = count / area)
    areas = geom.set_index("gisjoin")["land_sqmi"].to_dict()
    density_ts = {}
    for gj, counts in tracts_series.items():
        a = areas[gj]
        density_ts[gj] = [round(c / a, 1) for c in counts]

    years = list(range(YEAR_MIN, YEAR_MAX + 1))
    (WEB / "density_timeseries.json").write_text(json.dumps({
        "years": years,
        "tracts": density_ts,
    }, separators=(",", ":")))

    # Also the raw counts for delta mode (we compute density in JS too)
    (WEB / "counts_timeseries.json").write_text(json.dumps({
        "years": years,
        "tracts": tracts_series,
    }, separators=(",", ":")))

    # Summary
    summary_total = {}
    summary_peak = {}
    real_years_set = set()
    for gj in tracts_series:
        real_years_set.update(real_anchors[gj])

    for i, yr in enumerate(years):
        total = 0.0; peak = 0.0
        for gj, counts in tracts_series.items():
            total += counts[i]
            d = counts[i] / areas[gj]
            if d > peak: peak = d
        summary_total[yr] = int(round(total))
        summary_peak[yr] = int(round(peak))

    (WEB / "summary.json").write_text(json.dumps({
        "total": summary_total,
        "peak_density": summary_peak,
        "real_anchor_years": sorted(real_years_set),
    }))

    print("decade totals (sum of tract series):")
    for d in [1970, 1980, 1990, 2000, 2010, 2015, 2020, 2023]:
        if d in summary_total:
            print(f"  {d}: {summary_total[d]:,}")

    print("done.")


if __name__ == "__main__":
    main()
