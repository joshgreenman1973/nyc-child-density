#!/bin/bash
# Refresh whatever the Census has newly published, rebuild the site inputs, and
# push if anything changed. Cheap no-op when nothing new is out.
#
# The page runs on TWO Census clocks and they do not tick together:
#
#   ACS 5-year tract data  -> the map itself. New endyear each December.
#   Population estimates   -> the national under-18 context figure and the
#                             components-of-change chart. New vintage lands
#                             earlier in the year, and each vintage REVISES the
#                             previous one, sometimes by hundreds of thousands.
#
# So this runs on both a December and a March schedule. Every script here
# auto-detects the newest data it can find, so running the whole thing on
# either date is safe: the half with nothing new simply reports "cached".
set -euo pipefail
cd "$(dirname "$0")/.."

# Prefer an already-set env var (GitHub Actions injects CENSUS_API_KEY from a
# repo secret); fall back to the local login Keychain when run by hand/launchd.
export CENSUS_API_KEY="${CENSUS_API_KEY:-$(security find-generic-password -s CENSUS_API_KEY -w 2>/dev/null || true)}"
if [ -z "$CENSUS_API_KEY" ]; then
  echo "CENSUS_API_KEY not set. In CI add it as a repo secret; locally add it once with:" >&2
  echo '  security add-generic-password -s CENSUS_API_KEY -a joshgreenman -w "<key>"' >&2
  exit 1
fi

# --- ACS half: the map (needs the API key) --------------------------------
python3 scripts/fetch_acs.py
python3 scripts/fetch_totals.py
python3 scripts/build_timeseries.py
# Age bands are a separate output (docs/age_bands.json) and must run AFTER
# build_timeseries.py, which writes the tracts_base.geojson they join against.
# Without this the age-group chips stay a year behind the All view.
python3 scripts/fetch_age_bands.py

# --- Estimates half: national context + components chart ------------------
# These read static files from www2.census.gov and need no API key. Both probe
# for the newest vintage rather than a pinned year, so the March run picks up
# new data without anyone editing a constant.
python3 scripts/fetch_national.py
python3 scripts/build_components.py

if git diff --quiet -- docs/; then
  echo "No changes to docs/ — nothing new published."
  exit 0
fi

git config user.name "Josh Greenman"
git config user.email "josh.greenman@gmail.com"
git add docs/
git commit -m "Auto-refresh: newest Census data as of $(date +%Y-%m-%d)"
git push origin HEAD
echo "Pushed refreshed child-density data."
