"""One-time manual population: geocode the already-pulled MVP dataset and
write it directly to Supabase. Reuses the same geocode_helper/achc_scraper
functions the real pipeline uses -- this is not a separate code path."""
import asyncio
import json

from geocode_helper import geocode_locations
from achc_scraper import write_to_supabase_direct

DUMP_PATH = r"C:\Users\Jay Fox\achcscraper\data\normalized_dump.json"


async def main():
    with open(DUMP_PATH, encoding="utf-8") as f:
        data = json.load(f)
    print(f"Loaded {len(data)} normalized MVP-scope records")

    data = await geocode_locations(data)

    ok = sum(1 for r in data if r.get("geocode_status") == "ok")
    failed = sum(1 for r in data if r.get("geocode_status") == "failed")
    pending = sum(1 for r in data if r.get("geocode_status") == "pending")
    print(f"Geocoding done: {ok} ok, {failed} failed, {pending} pending")

    print("Writing to Supabase...")
    result = await write_to_supabase_direct(data)
    print(f"Supabase write complete: {result}")


if __name__ == "__main__":
    asyncio.run(main())
