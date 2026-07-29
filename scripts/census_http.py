"""
One retrying GET for every Census API call in this project.

Why: a full refresh makes roughly 300 sequential requests to api.census.gov
(11 counties x ~14 endyears x several scripts). The API resets connections
under sustained load, and a single ConnectionResetError two thirds of the way
through used to abort the whole GitHub Actions run — once a year, unwatched.
A blip is not a reason to fail; exhausting retries is.

This deliberately does NOT swallow errors. After the last attempt it re-raises,
so a genuine outage or a bad request still fails loud rather than writing an
empty file.
"""

import time

import requests

# Retry on transport-level failures and on the status codes that mean
# "try again", not on 4xx, which means the request itself is wrong.
RETRY_STATUS = {429, 500, 502, 503, 504}
MAX_ATTEMPTS = 5
BACKOFF_BASE = 2.0


def get(url, params=None, timeout=60, max_attempts=MAX_ATTEMPTS):
    """GET with exponential backoff. Returns the Response.

    Raises the last exception if every attempt fails at the transport level.
    A response with a non-retryable status is returned as-is, so callers keep
    their existing status-code handling.
    """
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        try:
            r = requests.get(url, params=params, timeout=timeout)
        except requests.RequestException as e:
            last_exc = e
            if attempt == max_attempts:
                raise
            wait = BACKOFF_BASE ** attempt
            print(f"    {type(e).__name__} on attempt {attempt}/{max_attempts}; "
                  f"retrying in {wait:.0f}s")
            time.sleep(wait)
            continue

        if r.status_code in RETRY_STATUS and attempt < max_attempts:
            wait = BACKOFF_BASE ** attempt
            print(f"    HTTP {r.status_code} on attempt {attempt}/{max_attempts}; "
                  f"retrying in {wait:.0f}s")
            time.sleep(wait)
            continue

        return r

    raise last_exc  # pragma: no cover - loop above always returns or raises
