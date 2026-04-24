"""
Fetch total-population (all ages) by census tract, for the same years the
under-18 pipeline uses:
  - Decennial 2000 SF1  : P012001
  - Decennial 2010 SF1  : P012001
  - Decennial 2020 DHC  : P12_001N
  - ACS 5-year 2011..2023: B01001_001E

Output (cached per year):
  data/total_2000.json  [{"geoid":..., "total":...}, ...]
  data/total_2010.json
  data/total_2020.json
  data/acs5_total_2011.json
  ...
  data/acs5_total_2023.json

These feed build_timeseries.py, which joins them onto 2010 tracts and produces
docs/totals_timeseries.json — the denominator for the "share of population
under 18" map view.
"""

import json
import time
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
DATA.mkdir(exist_ok=True)

STATE_COUNTIES = {
    "36": ["005", "047", "061", "081", "085", "119", "059"],
    "34": ["003", "017", "039", "023"],
}


def fetch_decennial(year):
    if year == 2020:
        base = "https://api.census.gov/data/2020/dec/dhc"
        var = "P12_001N"
    else:
        base = f"https://api.census.gov/data/{year}/dec/sf1"
        var = "P012001"
    out = []
    for state, counties in STATE_COUNTIES.items():
        for county in counties:
            params = {
                "get": f"NAME,{var}",
                "for": "tract:*",
                "in": f"state:{state} county:{county}",
            }
            r = requests.get(base, params=params, timeout=60)
            r.raise_for_status()
            header, *body = r.json()
            for row in body:
                d = dict(zip(header, row))
                try:
                    total = int(d[var])
                except (ValueError, TypeError):
                    total = None
                t = d["tract"]
                if len(t) == 4:
                    t = t + "00"
                out.append({
                    "geoid": d["state"] + d["county"] + t,
                    "total": total,
                })
            time.sleep(0.1)
    return out


def fetch_acs(endyear):
    base = f"https://api.census.gov/data/{endyear}/acs/acs5"
    var = "B01001_001E"
    out = []
    for state, counties in STATE_COUNTIES.items():
        for county in counties:
            params = {
                "get": f"NAME,{var}",
                "for": "tract:*",
                "in": f"state:{state} county:{county}",
            }
            r = requests.get(base, params=params, timeout=60)
            if r.status_code != 200:
                print(f"   {endyear} {state}{county}: HTTP {r.status_code}")
                continue
            header, *body = r.json()
            for row in body:
                d = dict(zip(header, row))
                try:
                    total = int(d[var])
                except (ValueError, TypeError):
                    total = None
                t = d["tract"]
                if len(t) == 4:
                    t = t + "00"
                out.append({
                    "geoid": d["state"] + d["county"] + t,
                    "total": total,
                })
            time.sleep(0.1)
    return out


def main():
    for year in [2000, 2010, 2020]:
        path = DATA / f"total_{year}.json"
        if path.exists():
            print(f"skip decennial {year} (cached)")
            continue
        print(f"fetching decennial {year}...")
        rows = fetch_decennial(year)
        path.write_text(json.dumps(rows))
        total = sum((r["total"] or 0) for r in rows)
        print(f"  {len(rows)} tracts, total pop {total:,}")

    for ey in range(2011, 2024):
        path = DATA / f"acs5_total_{ey}.json"
        if path.exists():
            print(f"skip ACS {ey} (cached)")
            continue
        print(f"fetching ACS {ey}...")
        rows = fetch_acs(ey)
        path.write_text(json.dumps(rows))
        total = sum((r["total"] or 0) for r in rows)
        print(f"  {len(rows)} tracts, total pop {total:,}")


if __name__ == "__main__":
    main()
