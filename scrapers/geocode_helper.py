"""Geocode provider addresses using the Google Maps Geocoding API.

Runs all uncached addresses concurrently (bounded by GEOCODE_CONCURRENCY,
default 10) so a full pull of ~23k addresses finishes in minutes rather than
hours.  Results are cached in geocode_cache.json to avoid re-billing on
subsequent runs.
"""
import asyncio
import json
import os
from typing import Optional, Tuple

import aiohttp
from dotenv import load_dotenv

load_dotenv()

CACHE_FILE = "geocode_cache.json"
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY", "")
GEOCODE_CONCURRENCY = int(os.getenv("GEOCODE_CONCURRENCY", "5"))
# Minimum seconds between requests per slot — keeps burst well under Google's 50 QPS cap
_REQUEST_DELAY = float(os.getenv("GEOCODE_REQUEST_DELAY", "0.25"))

_GEOCODE_URL = "https://maps.googleapis.com/maps/api/geocode/json"


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------

def load_cache() -> dict:
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r", encoding="utf-8-sig") as f:  # utf-8-sig strips BOM if present
            return json.load(f)
    return {}


def save_cache(cache: dict) -> None:
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2)


def address_key(street_address: str, city: str, state: str, postal_code: str) -> str:
    return (
        f"{street_address.strip().lower()}|"
        f"{city.strip().lower()}|"
        f"{state.strip().upper()}|"
        f"{str(postal_code).strip()}"
    )


# ---------------------------------------------------------------------------
# Single-address geocoding (Google Maps Geocoding API)
# ---------------------------------------------------------------------------

async def geocode_address(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    street_address: str,
    city: str,
    state: str,
    postal_code: str,
) -> Tuple[Optional[float], Optional[float]]:
    query = f"{street_address}, {city}, {state} {postal_code}, USA"
    params = {"address": query, "key": GOOGLE_MAPS_API_KEY}

    async with semaphore:
        try:
            async with session.get(
                _GEOCODE_URL,
                params=params,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status != 200:
                    print(f"Geocoding HTTP {resp.status} for {city}, {state}")
                    return None, None
                data = await resp.json()
                status = data.get("status", "")
                results = data.get("results", [])
                if results:
                    loc = results[0]["geometry"]["location"]
                    return float(loc["lat"]), float(loc["lng"])
                if status not in ("ZERO_RESULTS", "OK"):
                    print(f"Geocoding API status={status} for {city}, {state}")
        except Exception as exc:
            print(f"Geocoding error for {city}, {state}: {exc}")
        finally:
            # Rate-limit: hold the slot briefly to stay under Google's 50 QPS cap
            await asyncio.sleep(_REQUEST_DELAY)

    return None, None


# ---------------------------------------------------------------------------
# Batch geocoding (public API)
# ---------------------------------------------------------------------------

async def geocode_locations(locations: list) -> list:
    """Geocode a list of provider dicts in-place.  Returns the same list."""
    if not GOOGLE_MAPS_API_KEY:
        print("GOOGLE_MAPS_API_KEY not set — skipping geocoding")
        return locations

    cache = load_cache()
    to_geocode: list[tuple[int, dict, str]] = []

    for i, loc in enumerate(locations):
        key = address_key(
            loc.get("street_address", ""),
            loc.get("city", ""),
            loc.get("state", ""),
            str(loc.get("zip", "")),
        )
        if key in cache:
            loc["latitude"] = cache[key].get("latitude")
            loc["longitude"] = cache[key].get("longitude")
            loc["geocode_status"] = "ok"
        else:
            loc["geocode_status"] = "pending"
            to_geocode.append((i, loc, key))

    cached_count = len(locations) - len(to_geocode)
    print(
        f"Geocoding: {len(to_geocode)} new addresses "
        f"({cached_count} served from cache) — "
        f"concurrency={GEOCODE_CONCURRENCY}"
    )

    if not to_geocode:
        print("All geocodes served from cache")
        return locations

    semaphore = asyncio.Semaphore(GEOCODE_CONCURRENCY)
    connector = aiohttp.TCPConnector(limit=GEOCODE_CONCURRENCY + 4)
    completed = 0

    async with aiohttp.ClientSession(connector=connector) as session:

        async def _geocode_one(idx: int, loc: dict, key: str) -> None:
            nonlocal completed
            lat, lon = await geocode_address(
                session,
                semaphore,
                loc.get("street_address", ""),
                loc.get("city", ""),
                loc.get("state", ""),
                str(loc.get("zip", "")),
            )
            loc["latitude"] = lat
            loc["longitude"] = lon
            if lat is not None and lon is not None:
                cache[key] = {"latitude": lat, "longitude": lon}
                loc["geocode_status"] = "ok"
            else:
                loc["geocode_status"] = "failed"
            completed += 1
            if completed % 500 == 0:
                print(f"  Geocoded {completed}/{len(to_geocode)} new addresses...")
                save_cache(cache)

        await asyncio.gather(*[_geocode_one(i, loc, key) for i, loc, key in to_geocode])

    save_cache(cache)
    failed = [loc for loc in locations if loc.get("geocode_status") == "failed"]
    succeeded = completed - len(failed)
    print(f"Geocoding complete: {succeeded} succeeded, {len(failed)} failed (will retry next run)")
    if failed:
        print(f"  Failed addresses (billing/API issue — not bad addresses):")
        for loc in failed[:5]:
            print(f"    {loc.get('provider_name')} | {loc.get('street_address')}, {loc.get('city')} {loc.get('state')} {loc.get('zip')}")
        if len(failed) > 5:
            print(f"    ... and {len(failed) - 5} more. See geocode_failures.json for full list.")
        import json as _json
        with open("geocode_failures.json", "w", encoding="utf-8") as _f:
            _json.dump([{
                "provider_name": l.get("provider_name"),
                "street_address": l.get("street_address"),
                "city": l.get("city"),
                "state": l.get("state"),
                "zip": l.get("zip"),
                "achc_company_id": l.get("achc_company_id"),
                "source_url": l.get("source_url"),
            } for l in failed], _f, indent=2)
    return locations
