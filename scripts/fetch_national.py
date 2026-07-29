"""
Build docs/national.json: the U.S. under-18 population figures the page quotes
as national context, pulled from the newest published Census vintage rather
than hardcoded into the prose.

Why this exists: the "up only N% since 1970" line in the dek was hand-written
against Vintage 2024 and went stale the moment Vintage 2025 published, which
revised July 1, 2024 down by ~584,000. Anything hardcoded here drifts silently.

Output:
  {
    "vintage": 2025,
    "source_file": "NC-EST2025-AGESEX",
    "base_1970": 69600000,
    "years": {"2020": ..., "2021": ..., ..., "2025": ...}
  }

The 1970 baseline is a fixed historical census count, not an estimate, so it is
a constant here: 69.6 million, from the 1970 decennial census (Characteristics
of the Population, U.S. Summary, table 52). Other vintages of that same year
put it at 69.7-69.8M depending on whether the figure is the April 1 census
count or a July 1 estimate.
"""

import csv
import io
import json
import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
import census_http
from census_vintage import NATIONAL_URL, latest_vintage

ROOT = Path(__file__).resolve().parent.parent
WEB = ROOT / "docs"

BASE_1970 = 69_600_000


def main():
    vintage, url = latest_vintage(NATIONAL_URL)
    print(f"newest national vintage: {vintage}")
    r = census_http.get(url, timeout=180)
    r.raise_for_status()
    if len(r.content) < 5_000:
        raise SystemExit(f"{url} returned only {len(r.content)} bytes — refusing "
                         "to publish national figures from a truncated file.")

    rows = list(csv.DictReader(io.StringIO(r.content.decode("latin-1"))))
    year_cols = sorted(c for c in rows[0] if c.startswith("POPESTIMATE"))
    if not year_cols:
        raise SystemExit(f"no POPESTIMATE columns in {url}")

    years = {}
    for col in year_cols:
        total = sum(int(row[col]) for row in rows
                    if row["SEX"] == "0" and int(row["AGE"]) <= 17)
        if total <= 0:
            raise SystemExit(f"{col} summed to {total} — bad parse, refusing to write.")
        years[col.replace("POPESTIMATE", "")] = total

    out = {
        "vintage": vintage,
        "source_file": f"NC-EST{vintage}-AGESEX",
        "base_1970": BASE_1970,
        "years": years,
    }
    (WEB / "national.json").write_text(json.dumps(out, indent=2))
    print(f"wrote {WEB / 'national.json'}")
    for y, v in sorted(years.items()):
        pct = (v - BASE_1970) / BASE_1970 * 100
        print(f"  {y}: {v:,}  ({pct:+.1f}% vs 1970)")


if __name__ == "__main__":
    main()
