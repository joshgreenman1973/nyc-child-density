"""
Build docs/components_of_change.json: cumulative natural change (births minus
deaths) and net migration for each county in the study area, 2011 to the
newest published Census vintage.

Sources (downloaded on demand, so this is reproducible from a clean checkout):
  - Vintage 2019 county estimates for 2011-2019 : NATURALINC<year>, NETMIG<year>
  - Newest county vintage for 2020-onward       : NATURALCHG<year>, NETMIG<year>

The two vintages sit on different population bases (Vintage 2019 was never
revised to the 2020 census), so the 2019/2020 seam is a real discontinuity.
It is disclosed on the page; splicing is still the only way to get a continuous
series out of published Census files.

The post-2020 half is taken from the NEWEST published vintage, detected at
runtime by census_vintage.latest_vintage — not the vintage matching the map's
ACS endyear. Each vintage revises the previous one, sometimes materially:
Vintage 2025 moved the national under-18 count for 2024 down by roughly 580,000
from what Vintage 2024 reported.
"""

import csv
import io
import json
import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
import census_http
from census_vintage import COUNTY_URL, latest_vintage

ROOT = Path(__file__).resolve().parent.parent
WEB = ROOT / "docs"

V2019_URL = ("https://www2.census.gov/programs-surveys/popest/datasets/"
             "2010-2019/counties/totals/co-est2019-alldata.csv")

NYC_FIPS = {"36005", "36047", "36061", "36081", "36085"}

STUDY = [
    ("36", "005", "Bronx"), ("36", "047", "Brooklyn (Kings)"),
    ("36", "061", "Manhattan (NY)"), ("36", "081", "Queens"),
    ("36", "085", "Staten Is. (Richmond)"), ("36", "059", "Nassau"),
    ("36", "119", "Westchester"), ("34", "003", "Bergen"),
    ("34", "017", "Hudson"), ("34", "023", "Middlesex"), ("34", "039", "Union"),
]

# FIRST_YEAR and SEAM are fixed by the source layout. LAST_YEAR is whatever the
# newest county vintage reaches, resolved at runtime.
FIRST_YEAR, SEAM = 2011, 2020


def fetch_csv(url):
    """Download a Census estimates CSV. Fails loud: an empty or truncated
    response must not quietly produce a zeroed-out chart."""
    r = census_http.get(url, timeout=180)
    r.raise_for_status()
    if len(r.content) < 100_000:
        raise SystemExit(f"{url} returned only {len(r.content)} bytes — refusing "
                         "to build components from a truncated file.")
    return list(csv.DictReader(io.StringIO(r.content.decode("latin-1"))))


def main():
    vintage, county_url = latest_vintage(COUNTY_URL)
    last_year = vintage
    print(f"newest county vintage: {vintage} — building {FIRST_YEAR}-{last_year}")

    out = {
        name: {
            "fips": st + co,
            "group": "nyc" if st + co in NYC_FIPS else "suburb",
            "years": {},
        }
        for st, co, name in STUDY
    }
    by_fips = {st + co: name for st, co, name in STUDY}

    for url, years, nat_col in [
        (V2019_URL, range(FIRST_YEAR, SEAM), "NATURALINC"),
        (county_url, range(SEAM, last_year + 1), "NATURALCHG"),
    ]:
        print(f"fetching {url.rsplit('/', 1)[1]}...")
        seen = set()
        for row in fetch_csv(url):
            fips = row["STATE"] + row["COUNTY"]
            name = by_fips.get(fips)
            if not name:
                continue
            seen.add(fips)
            for y in years:
                out[name]["years"][str(y)] = {
                    "natural": int(row[f"{nat_col}{y}"]),
                    "netmig": int(row[f"NETMIG{y}"]),
                }
        missing = set(by_fips) - seen
        if missing:
            raise SystemExit(f"counties missing from {url}: {sorted(missing)}")

    for name, d in out.items():
        nat = sum(d["years"][str(y)]["natural"] for y in range(FIRST_YEAR, last_year + 1))
        mig = sum(d["years"][str(y)]["netmig"] for y in range(FIRST_YEAR, last_year + 1))
        d["cum"] = {"natural": nat, "netmig": mig, "total": nat + mig}

    out["_meta"] = {"vintage": vintage, "first_year": FIRST_YEAR, "last_year": last_year}
    (WEB / "components_of_change.json").write_text(json.dumps(out, indent=2))
    print(f"wrote {WEB / 'components_of_change.json'} ({len(out) - 1} counties, through {last_year})")

    for grp in ["nyc", "suburb"]:
        nat = sum(v["cum"]["natural"] for v in out.values() if v.get("group") == grp)
        mig = sum(v["cum"]["netmig"] for v in out.values() if v.get("group") == grp)
        print(f"  {grp:7} natural {nat:+,}  netmig {mig:+,}  net {nat + mig:+,}")


if __name__ == "__main__":
    main()
