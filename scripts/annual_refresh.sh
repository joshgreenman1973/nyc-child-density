#!/bin/bash
# Annual refresh: pull the newest ACS 5-year estimates (fetch_acs.py and
# fetch_totals.py both auto-detect the latest published endyear), rebuild the
# web time series, and push if anything changed. Meant to run via launchd
# once a year in December, when the Census Bureau typically releases the next
# ACS 5-year endyear. Cheap no-op if no new endyear is published yet.
set -euo pipefail
cd "$(dirname "$0")/.."

export CENSUS_API_KEY="$(security find-generic-password -s CENSUS_API_KEY -w 2>/dev/null || true)"
if [ -z "$CENSUS_API_KEY" ]; then
  echo "CENSUS_API_KEY not found in keychain. Add it once with:" >&2
  echo '  security add-generic-password -s CENSUS_API_KEY -a joshgreenman -w "<key>"' >&2
  exit 1
fi

python3 scripts/fetch_acs.py
python3 scripts/fetch_totals.py
python3 scripts/build_timeseries.py

if git diff --quiet -- docs/; then
  echo "No changes to docs/ — nothing new published."
  exit 0
fi

git config user.name "Josh Greenman"
git config user.email "josh.greenman@gmail.com"
git add docs/
git commit -m "Auto-refresh: new ACS endyear $(date +%Y-%m-%d)"
git push origin HEAD
echo "Pushed refreshed child-density data."
