import asyncio
import json
import os
from typing import Optional

import aiohttp

PLACES_CACHE_FILE = "places_cache.json"
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY", "")
PLACES_CONCURRENCY = int(os.getenv("PLACES_CONCURRENCY", "8"))
PLACES_CACHE_SAVE_INTERVAL = int(os.getenv("PLACES_CACHE_SAVE_INTERVAL", "100"))


def load_places_cache() -> dict:
    if os.path.exists(PLACES_CACHE_FILE):
        with open(PLACES_CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_places_cache(cache: dict):
    with open(PLACES_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2)


def places_cache_key(name: str, city: str, state: str) -> str:
    return f"{name.strip().lower()}|{city.strip().lower()}|{state.strip().upper()}"


async def lookup_website(
    session: aiohttp.ClientSession,
    name: str,
    street_address: str,
    city: str,
    state: str,
    zip_code: str,
) -> Optional[str]:
    query = f"{name}, {street_address}, {city}, {state} {zip_code}"
    url = "https://places.googleapis.com/v1/places:searchText"
    headers = {
        "X-Goog-Api-Key": GOOGLE_MAPS_API_KEY,
        "X-Goog-FieldMask": "places.websiteUri,places.displayName",
        "Content-Type": "application/json",
    }
    body = {"textQuery": query}

    try:
        async with session.post(url, json=body, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status == 200:
                data = await resp.json()
                places = data.get("places", [])
                if places:
                    website = places[0].get("websiteUri")
                    return website
            else:
                text = await resp.text()
                print(f"  Places API error {resp.status} for {name}: {text[:120]}")
    except Exception as e:
        print(f"  Places lookup error for {name}: {e}")

    return None


async def enrich_websites(locations: list) -> list:
    if not GOOGLE_MAPS_API_KEY:
        print("GOOGLE_MAPS_API_KEY not set -- skipping website enrichment")
        return locations

    cache = load_places_cache()
    looked_up = 0
    from_cache = 0
    found = 0
    sem = asyncio.Semaphore(PLACES_CONCURRENCY)

    # Apply cache hits first
    to_lookup = []
    for loc in locations:
        key = places_cache_key(loc.get("provider_name", ""), loc.get("city", ""), loc.get("state", ""))
        if key in cache:
            loc["website"] = cache[key]
            from_cache += 1
            if cache[key]:
                found += 1
        else:
            to_lookup.append(loc)

    print(f"Website enrichment: {from_cache} from cache, {len(to_lookup)} need API lookup (concurrency={PLACES_CONCURRENCY})")

    async def _lookup_one(loc: dict, idx: int) -> None:
        nonlocal looked_up, found
        name = loc.get("provider_name", "")
        city = loc.get("city", "")
        state = loc.get("state", "")
        street = loc.get("street_address", "")
        zip_code = loc.get("zip", "")
        key = places_cache_key(name, city, state)

        async with sem:
            website = await lookup_website(session, name, street, city, state, zip_code)
            loc["website"] = website or ""
            cache[key] = website or ""
            looked_up += 1
            if website:
                found += 1
            # Periodic cache save to survive crashes
            if looked_up % PLACES_CACHE_SAVE_INTERVAL == 0:
                save_places_cache(cache)
                print(f"  Places progress: {looked_up}/{len(to_lookup)} looked up, {found} found (cache saved)")

    async with aiohttp.ClientSession() as session:
        await asyncio.gather(*[_lookup_one(loc, i) for i, loc in enumerate(to_lookup)])

    save_places_cache(cache)
    print(
        f"Website enrichment complete: {from_cache} from cache, {looked_up} new lookups, "
        f"{found} websites found out of {len(locations)} providers"
    )
    return locations
