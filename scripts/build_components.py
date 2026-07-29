"""
Build docs/components_of_change.json: cumulative natural change (births minus
deaths) and net migration for each county in the study area, 2011-2025.

Sources (downloaded on demand, so this is reproducible from a clean checkout):
  - Vintage 2019 county estimates for 2011-2019 : NATURALINC<year>, NETMIG<year>
  - Vintage 2025 county estimates for 2020-2025 : NATURALCHG<year>, NETMIG<year>

The two vintages sit on different population bases (Vintage 2019 was never
revised to the 2020 census), so the 2019/2020 seam is a real discontinuity.
It is disclosed on the page; splicing is still the only way to get a continuous
series out of published Census files.

Always take the post-2020 half from the NEWEST published vintage, not the one
that happens to match the map's ACS endyear. Each vintage revises the previous
one, sometimes materially: Vintage 2025 moved the national under-18 count for
2024 down by roughly 580,000 from what Vintage 2024 reported.
"""

import csv
import io
import json
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent
WEB = ROOT / "docs"

V2019_URL = ("https://www2.census.gov/programs-surveys/popest/datasets/"
             "2010-2019/counties/totals/co-est2019-alldata.csv")
V2025_URL = ("https://www2.census.gov/programs-surveys/popest/datasets/"
             "2020-2025/counties/totals/co-est2025-alldata.csv")

NYC_FIPS = {"36005", "36047", "36061", "36081", "36085"}

STUDY = [
    ("36", "005", "Bronx"), ("36", "047", "Brooklyn (Kings)"),
    ("36", "061", "Manhattan (NY)"), ("36", "081", "Queens"),
    ("36", "085", "Staten Is. (Richmond)"), ("36", "059", "Nassau"),
    ("36", "119", "Westchester"), ("34", "003", "Bergen"),
    ("34", "017", "Hudson"), ("34", "023", "Middlesex"), ("34", "039", "Union"),
]

FIRST_YEAR, SEAM, LAST_YEAR = 2011, 2020, 2025


def fetch_csv(url):
    """Download a Census estimates CSV. Fails loud: an empty or truncated
    response must not quietly produce a zeroed-out chart."""
    r = requests.get(url, timeout=180)
    r.raise_for_status()
    if len(r.content) < 100_000:
        raise SystemExit(f"{url} returned only {len(r.content)} bytes — refusing "
                         "to build components from a truncated file.")
    return list(csv.DictReader(io.StringIO(r.content.decode("latin-1"))))


def main():
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
        (V2025_URL, range(SEAM, LAST_YEAR + 1), "NATURALCHG"),
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
        nat = sum(d["years"][str(y)]["natural"] for y in range(FIRST_YEAR, LAST_YEAR + 1))
        mig = sum(d["years"][str(y)]["netmig"] for y in range(FIRST_YEAR, LAST_YEAR + 1))
        d["cum"] = {"natural": nat, "netmig": mig, "total": nat + mig}

    (WEB / "components_of_change.json").write_text(json.dumps(out, indent=2))
    print(f"wrote {WEB / 'components_of_change.json'} ({len(out)} counties)")

    for grp in ["nyc", "suburb"]:
        nat = sum(v["cum"]["natural"] for v in out.values() if v["group"] == grp)
        mig = sum(v["cum"]["netmig"] for v in out.values() if v["group"] == grp)
        print(f"  {grp:7} natural {nat:+,}  netmig {mig:+,}  net {nat + mig:+,}")


if __name__ == "__main__":
    main()
