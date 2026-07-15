"""
Fetch ACS 5-year estimates of under-18 population by census tract,
for endyears 2011 through 2023, across the 11-county study area.

Uses table B01001 (sex by age). Under-18 is:
  male under 18   = B01001_003E + B01001_004E + B01001_005E + B01001_006E
  female under 18 = B01001_027E + B01001_028E + B01001_029E + B01001_030E

ACS tracts before 2020 endyear use 2010 boundaries; 2020+ use 2020 boundaries.
For pre-2020 endyears we can use directly. For 2020-2023 endyears, the GEOID
is on 2020 tracts - we handle that below by writing GEOID as-is and letting
the build step crosswalk.
"""

import json, os, time
from pathlib import Path
import requests

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
DATA.mkdir(exist_ok=True)

# Census API now requires a key; keyless calls 302-redirect and fail.
# Free key: https://api.census.gov/data/key_signup.html
KEY = os.environ.get("CENSUS_API_KEY", "")

STATE_COUNTIES = {
    "36": ["005", "047", "061", "081", "085", "119", "059"],
    "34": ["003", "017", "039", "023"],
}

MALE = ["B01001_003E", "B01001_004E", "B01001_005E", "B01001_006E"]
FEMALE = ["B01001_027E", "B01001_028E", "B01001_029E", "B01001_030E"]
VARS = MALE + FEMALE

# ACS 5-year endyears. 2009 is earliest; 2011-2023 gives us real data that
# overlaps and extends past the decennial 2010 anchor. EARLIEST is fixed;
# the upper bound is auto-detected each run so a new Census release picks up
# automatically without editing this file.
EARLIEST_ENDYEAR = 2011


def latest_available_endyear(start_probe, key):
    """Probe api.census.gov upward from start_probe to find the newest
    published ACS5 endyear (Census usually releases the next one in December).
    Returns the last endyear that responded successfully."""
    year = start_probe
    latest = start_probe - 1
    while True:
        url = f"https://api.census.gov/data/{year}/acs/acs5"
        try:
            r = requests.get(url, params={"get": "NAME", "for": "state:36", "key": key}, timeout=30)
        except requests.RequestException:
            break
        if r.status_code != 200:
            break
        latest = year
        year += 1
    return latest


ENDYEARS = None  # resolved in main() once KEY is known


def fetch_year(endyear):
    if not KEY:
        raise SystemExit("Set CENSUS_API_KEY (free: https://api.census.gov/data/key_signup.html). Keyless Census API calls now fail.")
    base = f"https://api.census.gov/data/{endyear}/acs/acs5"
    get = ",".join(["NAME"] + VARS)
    out = []
    for state, counties in STATE_COUNTIES.items():
        for county in counties:
            params = {
                "get": get,
                "for": "tract:*",
                "in": f"state:{state} county:{county}",
                "key": KEY,
            }
            r = requests.get(base, params=params, timeout=60)
            if r.status_code != 200:
                print(f"   {endyear} {state}{county}: HTTP {r.status_code}: {r.text[:120]}")
                continue
            header, *body = r.json()
            for row in body:
                d = dict(zip(header, row))
                try:
                    total = sum(int(d[v]) for v in VARS)
                except (ValueError, TypeError):
                    total = None
                t = d["tract"]
                if len(t) == 4:
                    t = t + "00"
                out.append({
                    "geoid": d["state"] + d["county"] + t,
                    "under18": total,
                })
            time.sleep(0.1)
    return out


def main():
    if not KEY:
        raise SystemExit("Set CENSUS_API_KEY (free: https://api.census.gov/data/key_signup.html). Keyless Census API calls now fail.")
    cached_years = sorted(
        int(p.stem.rsplit("_", 1)[1]) for p in DATA.glob("acs5_under18_*.json")
    )
    probe_start = (cached_years[-1] + 1) if cached_years else EARLIEST_ENDYEAR
    newest = latest_available_endyear(probe_start, KEY)
    endyears = list(range(EARLIEST_ENDYEAR, max(newest, probe_start - 1) + 1))
    print(f"Fetching endyears {endyears[0]}-{endyears[-1]} (newest published: {newest})")
    for ey in endyears:
        outpath = DATA / f"acs5_under18_{ey}.json"
        if outpath.exists():
            print(f"skip {ey} (cached)")
            continue
        rows = fetch_year(ey)
        outpath.write_text(json.dumps(rows))
        total = sum((r["under18"] or 0) for r in rows)
        print(f"  {ey}: {len(rows)} tracts, {total:,} children")


if __name__ == "__main__":
    main()
