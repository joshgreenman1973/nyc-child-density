"""
Take the per-year decennial counts (on 2010-normalized tracts) and produce a
year-by-year table from 1970 to 2020 via linear interpolation.

Output: web/density_timeseries.json
  { "years": [1970, 1971, ..., 2020],
    "tracts": {
      "<geoid2010>": { "areas_sqmi": 1.23, "density": [d1970, d1971, ...] },
      ...
    }
  }

The frontend uses this for frame-by-frame animation with smooth tweening.
"""

import json
from pathlib import Path
import pandas as pd
import geopandas as gpd

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
WEB = ROOT / "docs"

DECADES = [1970, 1980, 1990, 2000, 2010, 2020]


def load_decennial_panel():
    """Return DataFrame: geoid2010 -> under18 counts for each decade."""
    # Placeholder: will be filled in after NHGIS extract lands.
    # Expected inputs:
    #   data/nhgis_under18_normalized.csv with columns geoid2010, year, under18
    #   data/under18_2020_on_2010.json    (2020 DHC reaggregated to 2010 tracts)
    raise NotImplementedError("Run fetch_nhgis.py first, then reaggregate 2020.")


def interpolate(panel):
    years = list(range(1970, 2021))
    out = {}
    for geoid, row in panel.iterrows():
        dvals = [row[d] for d in DECADES]
        series = []
        for y in years:
            # Linear interp between surrounding decade anchors.
            i = min((y - 1970) // 10, len(DECADES) - 2)
            y0, y1 = DECADES[i], DECADES[i + 1]
            v0, v1 = dvals[i], dvals[i + 1]
            t = (y - y0) / (y1 - y0)
            series.append(round(v0 + (v1 - v0) * t, 1))
        out[geoid] = series
    return years, out


if __name__ == "__main__":
    print("This script runs after NHGIS data is in hand.")
