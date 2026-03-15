import asyncio
import aiohttp
from typing import Optional, Tuple

async def geocode_address(address: str, city: str, state: str, postal_code: str) -> Tuple[Optional[float], Optional[float]]:
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
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, headers=headers, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    if data and len(data) > 0:
                        lat = float(data[0]["lat"])
                        lon = float(data[0]["lon"])
                        return (lat, lon)
    except Exception as e:
        print(f"Geocoding error for {city}, {state}: {e}")

    return (None, None)

async def geocode_locations(locations: list) -> list:
    tasks = []
    for loc in locations:
        tasks.append(geocode_address(
            loc["address"],
            loc["city"],
            loc["state"],
            loc["zip"]
        ))

    results = await asyncio.gather(*tasks)

    for loc, (lat, lon) in zip(locations, results):
        loc["latitude"] = lat
        loc["longitude"] = lon

    return locations
