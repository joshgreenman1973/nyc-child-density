# NYC-area child density, 1970 to 2023

Interactive animated map of children under 18 per square mile, by census tract,
across New York City and every bordering county in New York and New Jersey.

**Live:** https://joshgreenman1973.github.io/nyc-child-density/

## What it shows

- Decennial census anchors for 1970, 1980, 1990, 2000, 2010, 2020 from IPUMS NHGIS
  (time series table D08, "Persons by Age: Children and Adults").
- Annual ACS 5-year estimates for endyears 2011 through 2023, table B01001.
- Two view modes:
  - **Density** (absolute) — children per sq mi on a fixed color scale.
  - **Change from baseline** — diverging red/blue showing % change from a chosen
    decennial baseline year.

## Study area

Five boroughs of NYC plus: Westchester, Nassau (NY); Bergen, Hudson, Union,
Middlesex (NJ).

## Methodology

Full methodology and limitations are in an expandable panel on the live page.
Short version:

- Geometry is on 2010 census tract boundaries (TIGER cartographic 500k).
- Between decennial anchors before 2011, annual values are linearly interpolated.
- From 2011 on, every year is a real ACS measurement.
- NHGIS decennial values join 2010 tracts by nominal tract code; true
  population-weighted normalization via NHGIS crosswalks is a future enhancement.

## Rebuilding the data

```sh
# 1. Decennial 2000/2010/2020 direct from Census API + cartographic boundaries
python3 scripts/fetch_census.py
python3 scripts/fetch_tracts.py

# 2. NHGIS 1970-2020 time series (needs free IPUMS API key in ~/.nhgis_api_key)
python3 scripts/fetch_nhgis.py
python3 scripts/poll_nhgis.py

# 3. ACS 5-year 2011-2023
python3 scripts/fetch_acs.py

# 4. Merge everything into web/ inputs
python3 scripts/build_timeseries.py
```

Output in `web/` is a static site (HTML + three JSON files). Serve any way you like.

## Data sources

- U.S. Census Bureau decennial census (SF1 for 2000/2010, DHC for 2020) and
  American Community Survey 5-year estimates, via api.census.gov.
- IPUMS NHGIS, University of Minnesota. Schroeder, J., Van Riper, D., Manson, S.,
  et al. IPUMS National Historical Geographic Information System: Version 20.0
  [dataset]. Minneapolis, MN: IPUMS. 2025. http://doi.org/10.18128/D050.V20.0
