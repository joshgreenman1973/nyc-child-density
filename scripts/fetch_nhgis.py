"""
Fetch decennial under-18 population counts for 1970-2020 from NHGIS via its
data API, normalized to 2010 census tract boundaries (so a single tract has a
continuous value across all decades, which makes smooth animation possible).

Requires an IPUMS / NHGIS API key. Register at:
  https://account.ipums.org/registration/new
  https://account.ipums.org/api_keys

Then either:
  export NHGIS_API_KEY=yourkey
or place the key in ~/.nhgis_api_key

NHGIS time-series tables relevant here:
  - B57 : "Persons Under 18 Years" (1970, 1980, 1990, 2000, 2010)
    normalized to 2010 boundaries (nominal_geog_unit = "NT4")
  - For 2020 we pull directly from DHC P12 table, already fetched separately.

This script writes data/nhgis_under18_normalized.csv with columns:
  gisjoin, geoid2010, year, under18
"""

import os
import io
import json
import time
import zipfile
from pathlib import Path
import requests

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
DATA.mkdir(exist_ok=True)

API = "https://api.ipums.org/extracts"
COLLECTION = "nhgis"
VERSION = "v2"

NY_NJ_COUNTIES_2010 = {
    # 5 char state+county FIPS (our 11-county study area)
    "36005", "36047", "36061", "36081", "36085",   # NYC
    "36119", "36059",                              # Westchester, Nassau
    "34003", "34017", "34039", "34023",            # Bergen, Hudson, Union, Middlesex
}


def get_api_key():
    env = os.environ.get("NHGIS_API_KEY")
    if env:
        return env.strip()
    p = Path.home() / ".nhgis_api_key"
    if p.exists():
        return p.read_text().strip()
    raise SystemExit(
        "No NHGIS API key found. Set NHGIS_API_KEY env var or write ~/.nhgis_api_key"
    )


def submit_extract(api_key):
    """Request a normalized time-series extract for B57 (persons under 18)."""
    body = {
        "datasets": [],
        "timeSeriesTables": [
            {
                "name": "B57",   # Persons Under 18 Years
                "geogLevels": ["tract"],
                # Choose the "nominal" integration — normalizes to each period's own boundaries.
                # For cross-time consistency we want standardized to 2010.
                # NHGIS geographic integration: "2010" = standardize to 2010 boundaries.
                "geographicIntegration": "standardized_to_2010",
            }
        ],
        "description": "NYC-area under-18, 1970-2010 normalized to 2010 tracts",
        "dataFormat": "csv_no_header",
    }
    r = requests.post(
        f"{API}?collection={COLLECTION}&version={VERSION}",
        headers={"Authorization": api_key, "Content-Type": "application/json"},
        json=body,
        timeout=60,
    )
    r.raise_for_status()
    return r.json()["number"]


def wait_for_extract(api_key, number):
    url = f"{API}/{number}?collection={COLLECTION}&version={VERSION}"
    while True:
        r = requests.get(url, headers={"Authorization": api_key}, timeout=60)
        r.raise_for_status()
        status = r.json()["status"]
        print(f"  extract {number} status: {status}")
        if status == "completed":
            return r.json()
        if status == "failed":
            raise SystemExit(f"extract failed: {r.json()}")
        time.sleep(15)


def download_extract(api_key, extract):
    url = extract["downloadLinks"]["tableData"]["url"]
    r = requests.get(url, headers={"Authorization": api_key}, timeout=300)
    r.raise_for_status()
    out = DATA / "nhgis_b57.zip"
    out.write_bytes(r.content)
    return out


def main():
    key = get_api_key()
    print("Submitting NHGIS extract...")
    num = submit_extract(key)
    print(f"  extract number: {num}")
    ext = wait_for_extract(key, num)
    zpath = download_extract(key, ext)
    print(f"  downloaded {zpath}")
    # Unpack and filter to our study area
    import pandas as pd
    with zipfile.ZipFile(zpath) as z:
        csvs = [n for n in z.namelist() if n.endswith(".csv")]
        print("  files:", csvs)
        # We'll post-process once we see the structure.
        for n in csvs:
            z.extract(n, DATA / "nhgis_unpacked")


if __name__ == "__main__":
    main()
