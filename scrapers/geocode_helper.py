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

def group_by_address(locations: list) -> "dict[str, list[dict]]":
    """Group provider records by normalized physical address.

    The same physical location can appear multiple times in `locations`
    (e.g. one row per accredited program/service the institution offers).
    Geocoding must key off this grouping, not the raw list, or duplicate
    addresses get billed once per occurrence instead of once per address.
    """
    groups: "dict[str, list[dict]]" = {}
    for loc in locations:
        key = address_key(
            loc.get("street_address", ""),
            loc.get("city", ""),
            loc.get("state", ""),
            str(loc.get("zip", "")),
        )
        groups.setdefault(key, []).append(loc)
    return groups


def print_dedup_report(locations: list) -> "dict[str, list[dict]]":
    """Print raw provider count vs. unique-address count. Free — no API calls."""
    groups = group_by_address(locations)
    duplicate_groups = {k: v for k, v in groups.items() if len(v) > 1}
    if duplicate_groups:
        dup_providers = sum(len(v) for v in duplicate_groups.values())
        print(
            f"Address dedup: {len(locations)} providers -> {len(groups)} unique addresses "
            f"({len(duplicate_groups)} addresses shared by {dup_providers} providers total — "
            f"each will be geocoded once, not once per provider)"
        )
        for key, group in list(duplicate_groups.items())[:10]:
            sample = group[0]
            names = [g.get("provider_name") for g in group]
            print(f"    {sample.get('street_address')}, {sample.get('city')} {sample.get('state')}: {names}")
        if len(duplicate_groups) > 10:
            print(f"    ... and {len(duplicate_groups) - 10} more duplicate-address groups")
    else:
        print(f"Address dedup: {len(locations)} providers -> {len(groups)} unique addresses (no duplicates)")
    return groups


async def geocode_locations(locations: list) -> list:
    """Geocode a list of provider dicts in-place.  Returns the same list.

    Geocodes by unique physical address (see group_by_address), not by
    provider record — a location listed multiple times (once per accredited
    program/service) is billed exactly once, with the result broadcast to
    every provider record sharing that address.
    """
    if not GOOGLE_MAPS_API_KEY:
        print("GOOGLE_MAPS_API_KEY not set — skipping geocoding")
        return locations

    cache = load_cache()
    groups = print_dedup_report(locations)

    to_geocode: list[tuple[str, list[dict]]] = []
    for key, group in groups.items():
        if key in cache:
            lat = cache[key].get("latitude")
            lon = cache[key].get("longitude")
            for loc in group:
                loc["latitude"] = lat
                loc["longitude"] = lon
                loc["geocode_status"] = "ok"
        else:
            for loc in group:
                loc["geocode_status"] = "pending"
            to_geocode.append((key, group))

    cached_count = len(groups) - len(to_geocode)
    print(
        f"Geocoding: {len(to_geocode)} new unique addresses to look up "
        f"({cached_count} addresses served from cache) — "
        f"concurrency={GEOCODE_CONCURRENCY}"
    )

    if not to_geocode:
        print("All geocodes served from cache")
        return locations

    semaphore = asyncio.Semaphore(GEOCODE_CONCURRENCY)
    connector = aiohttp.TCPConnector(limit=GEOCODE_CONCURRENCY + 4)
    completed = 0

    async with aiohttp.ClientSession(connector=connector) as session:

        async def _geocode_one(key: str, group: list) -> None:
            nonlocal completed
            sample = group[0]
            lat, lon = await geocode_address(
                session,
                semaphore,
                sample.get("street_address", ""),
                sample.get("city", ""),
                sample.get("state", ""),
                str(sample.get("zip", "")),
            )
            status = "ok" if lat is not None and lon is not None else "failed"
            if status == "ok":
                cache[key] = {"latitude": lat, "longitude": lon}
            for loc in group:
                loc["latitude"] = lat
                loc["longitude"] = lon
                loc["geocode_status"] = status
            completed += 1
            if completed % 500 == 0:
                print(f"  Geocoded {completed}/{len(to_geocode)} new addresses...")
                save_cache(cache)

        await asyncio.gather(*[_geocode_one(key, group) for key, group in to_geocode])

    save_cache(cache)
    failed = [loc for loc in locations if loc.get("geocode_status") == "failed"]
    ok = [loc for loc in locations if loc.get("geocode_status") == "ok"]
    print(
        f"Geocoding complete: {len(to_geocode)} API calls made for {len(to_geocode)} unique addresses — "
        f"{len(ok)} providers resolved, {len(failed)} providers failed (will retry next run)"
    )
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
