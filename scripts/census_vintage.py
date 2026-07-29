"""
Find the newest published Census population-estimates vintage.

The estimates run on a different clock from the ACS: county and national
vintages publish earlier, and each one REVISES the last. Vintage 2025 moved the
national under-18 count for July 1, 2024 down by about 584,000 from what
Vintage 2024 reported. So the rule for anything built off estimates is to take
the newest vintage available, not the one whose year matches the map's ACS
endyear.

Both consumers (build_components.py, fetch_national.py) probe through here.
"""

import requests

COUNTY_URL = ("https://www2.census.gov/programs-surveys/popest/datasets/"
              "2020-{v}/counties/totals/co-est{v}-alldata.csv")
NATIONAL_URL = ("https://www2.census.gov/programs-surveys/popest/datasets/"
                "2020-{v}/national/asrh/nc-est{v}-agesex-res.csv")

# First vintage published on the 2020-census base. Nothing earlier is usable
# here without also changing the file layout.
EARLIEST = 2021


def latest_vintage(url_template, start=EARLIEST, ceiling=12):
    """Probe upward for the newest vintage whose file actually exists.

    Returns (vintage_year, url). Raises if not even `start` is reachable —
    a silent fall back to a stale vintage is worse than a loud failure.
    """
    found = None
    v = start
    for _ in range(ceiling):
        url = url_template.format(v=v)
        try:
            r = requests.head(url, timeout=60, allow_redirects=True)
        except requests.RequestException:
            break
        if r.status_code != 200:
            break
        found = v
        v += 1
    if found is None:
        raise SystemExit(
            f"No Census estimates vintage found at or after {start} for "
            f"{url_template.format(v=start)} — refusing to build from nothing.")
    return found, url_template.format(v=found)
