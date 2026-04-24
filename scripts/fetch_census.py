"""
Fetch NYC under-18 population by census tract, for as many years as possible.

Sources used (no API key — stays under anonymous 500/day limit):
- Decennial 2000 SF1, variable P012003..P012025 (male <18) + P012027..P012049 (female <18)
  Simpler: P012003 + P012004 + P012005 + P012006 = males 0-17 by age group
  Even simpler: table P014 (sex by single year of age under 20) — or just use age-grouped P012.
  Cleanest: Decennial 2000 table P013 is "Median age by sex" — no.
  Use PCT012 or just compute from P012.
  Actually Decennial 2000 SF1 has P014 "Sex by age for the population under 20 years"
  but easiest: use SF1 P012 and sum the under-18 age bands.
  There is a simpler variable: P016001 "POPULATION UNDER 18 YEARS" — verify.
  In 2000 SF1: P016 is "POPULATION UNDER 18 YEARS BY HOUSEHOLDER RELATIONSHIP" total=P016001.

- Decennial 2010 SF1: P014001 = total under 18 (actually P014 is sex by single year under 20
  in 2010 too; use P012 sum or simpler P016001).
  Cleaner: DP1 in 2010 has a variable for under-18 count.
  Most reliable: sum P012003..P012006 (male 0-4,5-9,10-14,15-17) + P012027..P012030 (female).

- Decennial 2020 DHC: P12 variables same structure.

- ACS 5yr 2009-2023: B01001 (sex by age) — sum under-18 bands.

For a first pass, I'll fetch Decennial 2000, 2010, 2020 using P012 sums.
"""

import json
import time
from pathlib import Path
import requests

OUT = Path(__file__).resolve().parent.parent / "data"
OUT.mkdir(exist_ok=True)

# NYC + counties that share a land or water border with NYC.
# Keyed by state FIPS.
STATE_COUNTIES = {
    "36": [  # New York
        "005", "047", "061", "081", "085",  # Bronx, Kings, New York, Queens, Richmond
        "119",  # Westchester
        "059",  # Nassau
    ],
    "34": [  # New Jersey
        "003",  # Bergen
        "017",  # Hudson
        "039",  # Union
        "023",  # Middlesex
    ],
}

# P012 male under-18 age bands: 003(<5), 004(5-9), 005(10-14), 006(15-17)
# P012 female under-18 age bands: 027(<5), 028(5-9), 029(10-14), 030(15-17)
MALE_U18 = ["P012003", "P012004", "P012005", "P012006"]
FEMALE_U18 = ["P012027", "P012028", "P012029", "P012030"]

# Dataset endpoints by year
DATASETS = {
    2000: "https://api.census.gov/data/2000/dec/sf1",
    2010: "https://api.census.gov/data/2010/dec/sf1",
    # 2020 uses DHC (Demographic and Housing Characteristics) — variables renamed P12_003N etc.
    2020: "https://api.census.gov/data/2020/dec/dhc",
}

# 2020 DHC uses "P12_003N" style
def vars_for_year(year):
    if year == 2020:
        male = [f"P12_{n[-3:]}N" for n in MALE_U18]
        female = [f"P12_{n[-3:]}N" for n in FEMALE_U18]
        return male + female
    return MALE_U18 + FEMALE_U18


def fetch(year):
    base = DATASETS[year]
    variables = vars_for_year(year)
    get_clause = ",".join(["NAME"] + variables)
    rows = []
    for state, counties in STATE_COUNTIES.items():
        for county in counties:
            params = {
                "get": get_clause,
                "for": "tract:*",
                "in": f"state:{state} county:{county}",
            }
            r = requests.get(base, params=params, timeout=60)
            r.raise_for_status()
            data = r.json()
            header, *body = data
            for row in body:
                d = dict(zip(header, row))
                try:
                    total = sum(int(d[v]) for v in variables)
                except (ValueError, TypeError):
                    total = None
                tract = d["tract"]
                if len(tract) == 4:
                    tract = tract + "00"
                rows.append({
                    "geoid": d["state"] + d["county"] + tract,
                    "name": d["NAME"],
                    "under18": total,
                })
            time.sleep(0.2)
    return rows


def main():
    for year in [2000, 2010, 2020]:
        print(f"Fetching {year}...")
        rows = fetch(year)
        out = OUT / f"under18_{year}.json"
        out.write_text(json.dumps(rows, indent=2))
        total = sum(r["under18"] or 0 for r in rows)
        print(f"  {len(rows)} tracts, {total:,} children under 18")


if __name__ == "__main__":
    main()
