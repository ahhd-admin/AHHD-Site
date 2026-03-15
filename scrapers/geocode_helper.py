import asyncio
import json
import os
from typing import Optional, Tuple

import aiohttp

CACHE_FILE = "geocode_cache.json"


def load_cache() -> dict:
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_cache(cache: dict):
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2)


def address_key(address: str, city: str, state: str, postal_code: str) -> str:
    return f"{address.strip().lower()}|{city.strip().lower()}|{state.strip().upper()}|{postal_code.strip()}"


async def geocode_address(session, address: str, city: str, state: str, postal_code: str) -> Tuple[Optional[float], Optional[float]]:
    query = f"{address}, {city}, {state} {postal_code}, USA"

    url = "https://nominatim.openstreetmap.org/search"
    params = {
        "format": "json",
        "q": query,
        "limit": 1,
        "countrycodes": "us"
    }

    headers = {
        "User-Agent": "AHHD-directory-research/1.0 (+https://accreditedhomehealthcare.directory)"
    }

    try:
        async with session.get(url, params=params, headers=headers, timeout=15) as response:
            if response.status == 200:
                data = await response.json()
                if data:
                    return float(data[0]["lat"]), float(data[0]["lon"])
    except Exception as e:
        print(f"Geocoding error for {city}, {state}: {e}")

    return None, None


async def geocode_locations(locations: list) -> list:
    cache = load_cache()
    updated = 0

    connector = aiohttp.TCPConnector(limit=1)
    async with aiohttp.ClientSession(connector=connector) as session:
        for i, loc in enumerate(locations, start=1):
            key = address_key(loc["address"], loc["city"], loc["state"], loc["zip"])

            if key in cache:
                loc["latitude"] = cache[key].get("latitude")
                loc["longitude"] = cache[key].get("longitude")
                continue

            lat, lon = await geocode_address(
                session,
                loc["address"],
                loc["city"],
                loc["state"],
                loc["zip"]
            )

            loc["latitude"] = lat
            loc["longitude"] = lon

            cache[key] = {
                "latitude": lat,
                "longitude": lon
            }
            updated += 1

            if i % 25 == 0:
                print(f"Geocoded {i}/{len(locations)} locations")

            await asyncio.sleep(1.1)

    save_cache(cache)
    print(f"Added {updated} new geocodes to cache")

    return locations
