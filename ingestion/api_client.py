"""Thin client for the CollegeFootballData (CFBD) API.

Every ingestion script imports from here, so authentication and request logic
live in exactly one place. This module does not fetch or save any specific
dataset — it only knows how to make one authenticated call and return JSON.
"""

import os

import requests
from dotenv import load_dotenv

BASE_URL = "https://api.collegefootballdata.com"

# Load variables from .env into the environment once, when this module is imported.
load_dotenv()


def get(endpoint, params=None):
    """Make an authenticated GET request to the CFBD API and return parsed JSON.

    endpoint: path such as "/teams/fbs" (a leading slash is optional).
    params:   optional dict of query parameters, e.g. {"year": 2024}.
    """
    api_key = os.getenv("CFBD_API_KEY")
    if not api_key:
        raise RuntimeError("CFBD_API_KEY not found — add it to your .env file.")

    url = f"{BASE_URL}/{endpoint.lstrip('/')}"
    headers = {"Authorization": f"Bearer {api_key}"}

    response = requests.get(url, headers=headers, params=params, timeout=30)
    response.raise_for_status()  # turn an HTTP error (401, 429, 500...) into an exception
    return response.json()


if __name__ == "__main__":
    # Smoke test: run `python ingestion/api_client.py` to confirm auth works.
    teams = get("/teams/fbs", {"year": 2024})
    print(f"OK — fetched {len(teams)} FBS teams for 2024")
