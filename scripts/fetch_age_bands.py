"""
Fetch child population broken into 4 age bands by tract, for every year
between 2000 and 2023 where the data supports it.

Bands: under 5, 5-9, 10-14, 15-17.

Sources:
  - 2000 decennial SF1, table P012 (male 003-006, female 027-030)
  - 2010 decennial SF1, table P012 (same layout)
  - 2020 decennial DHC, table P12 (renamed to P12_003N etc.)
  - 2011-2023 ACS 5-year, table B01001 (male 003-006, female 027-030)

Output: docs/age_bands.json
  {
    "years": [2000, 2010, 2020, 2011, 2012, ..., 2023],
    "bands": ["u5", "5_9", "10_14", "15_17"],
    "tracts": {
      "<geoid>": {
        "u5":    {year: count, ...},
        "5_9":   {...},
        "10_14": {...},
        "15_17": {...}
      }
    }
  }

This is joined in the frontend against the existing under-18 time series.
"""

import json, time
from pathlib import Path
import requests

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
WEB = ROOT / "docs"

STATE_COUNTIES = {
    "36": ["005", "047", "061", "081", "085", "119", "059"],
    "34": ["003", "017", "039", "023"],
}

BANDS = ["u5", "5_9", "10_14", "15_17"]

# Decennial P012: male index [0..3] = 003-006; female [0..3] = 027-030
P012_MALE   = ["003", "004", "005", "006"]
P012_FEMALE = ["027", "028", "029", "030"]

# ACS B01001 uses the same underscore-zero-padded indices.
ACS_MALE    = [f"B01001_{n}E" for n in P012_MALE]
ACS_FEMALE  = [f"B01001_{n}E" for n in P012_FEMALE]


def dec_vars(year):
    if year == 2020:
        # DHC: P12_003N ... P12_030N
        male   = [f"P12_{n}N" for n in P012_MALE]
        female = [f"P12_{n}N" for n in P012_FEMALE]
    else:
        # SF1 2000/2010: P012003 etc.
        male   = [f"P012{n}" for n in P012_MALE]
        female = [f"P012{n}" for n in P012_FEMALE]
    return male, female


def fetch_decennial(year):
    base = {
        2000: "https://api.census.gov/data/2000/dec/sf1",
        2010: "https://api.census.gov/data/2010/dec/sf1",
        2020: "https://api.census.gov/data/2020/dec/dhc",
    }[year]
    male, female = dec_vars(year)
    all_vars = male + female
    get = ",".join(["NAME"] + all_vars)
    out = {}
    for state, counties in STATE_COUNTIES.items():
        for county in counties:
            params = {"get": get, "for": "tract:*",
                      "in": f"state:{state} county:{county}"}
            r = requests.get(base, params=params, timeout=60)
            r.raise_for_status()
            header, *body = r.json()
            for row in body:
                d = dict(zip(header, row))
                tract = d["tract"]
                if len(tract) == 4:
                    tract = tract + "00"
                geoid = d["state"] + d["county"] + tract
                try:
                    bvals = [int(d[male[i]]) + int(d[female[i]]) for i in range(4)]
                except (ValueError, TypeError):
                    continue
                out[geoid] = dict(zip(BANDS, bvals))
            time.sleep(0.1)
    return out


def fetch_acs(endyear):
    base = f"https://api.census.gov/data/{endyear}/acs/acs5"
    all_vars = ACS_MALE + ACS_FEMALE
    get = ",".join(["NAME"] + all_vars)
    out = {}
    for state, counties in STATE_COUNTIES.items():
        for county in counties:
            params = {"get": get, "for": "tract:*",
                      "in": f"state:{state} county:{county}"}
            r = requests.get(base, params=params, timeout=60)
            if r.status_code != 200:
                print(f"   {endyear} {state}{county}: HTTP {r.status_code}")
                continue
            header, *body = r.json()
            for row in body:
                d = dict(zip(header, row))
                tract = d["tract"]
                if len(tract) == 4:
                    tract = tract + "00"
                geoid = d["state"] + d["county"] + tract
                try:
                    bvals = [int(d[ACS_MALE[i]]) + int(d[ACS_FEMALE[i]])
                             for i in range(4)]
                except (ValueError, TypeError):
                    continue
                out[geoid] = dict(zip(BANDS, bvals))
            time.sleep(0.1)
    return out


def main():
    # Load the existing base features so we know which GEOIDs we render.
    base = json.loads((WEB / "tracts_base.geojson").read_text())
    wanted_geoids = set(f["properties"]["geoid"] for f in base["features"])
    print(f"  {len(wanted_geoids)} geoids in base")

    per_year = {}

    for year in [2000, 2010, 2020]:
        cache = DATA / f"age_bands_{year}.json"
        if cache.exists():
            print(f"  {year}: cached")
            per_year[year] = json.loads(cache.read_text())
        else:
            print(f"  fetching decennial {year}...")
            per_year[year] = fetch_decennial(year)
            cache.write_text(json.dumps(per_year[year]))

    for ey in range(2011, 2024):
        cache = DATA / f"age_bands_acs_{ey}.json"
        if cache.exists():
            print(f"  ACS {ey}: cached")
            per_year[ey] = json.loads(cache.read_text())
        else:
            print(f"  fetching ACS {ey}...")
            per_year[ey] = fetch_acs(ey)
            cache.write_text(json.dumps(per_year[ey]))

    # Pivot: tracts -> band -> year -> count
    tracts = {}
    years = sorted(per_year.keys())
    for geoid in wanted_geoids:
        rec = {b: {} for b in BANDS}
        has_any = False
        for y in years:
            m = per_year[y].get(geoid)
            if not m:
                continue
            for b in BANDS:
                rec[b][y] = m[b]
                has_any = True
        if has_any:
            tracts[geoid] = rec

    out = {"years": years, "bands": BANDS, "tracts": tracts}
    (WEB / "age_bands.json").write_text(json.dumps(out, separators=(",", ":")))
    print(f"  wrote {WEB/'age_bands.json'} ({len(tracts)} tracts)")

    # Sanity: print a band total for the earliest and latest complete years.
    for y in [2000, 2010, 2020, 2023]:
        if y not in years:
            continue
        totals = {b: 0 for b in BANDS}
        for rec in tracts.values():
            for b in BANDS:
                if y in rec[b]:
                    totals[b] += rec[b][y]
        print(f"  {y} totals: " + ", ".join(f"{b}={totals[b]:,}" for b in BANDS)
              + f"  sum={sum(totals.values()):,}")


if __name__ == "__main__":
    main()
