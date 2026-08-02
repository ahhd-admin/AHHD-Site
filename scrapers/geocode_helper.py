"""Geocode provider addresses using the Google Maps Geocoding API.

Runs all uncached addresses concurrently (bounded by GEOCODE_CONCURRENCY,
default 10) so a full pull of ~23k addresses finishes in minutes rather than
hours.  Results are cached in geocode_cache.json to avoid re-billing on
subsequent runs.
"""
import asyncio
import json
import os
from datetime import datetime, timezone
from typing import Optional, Tuple

import aiohttp
from dotenv import load_dotenv

load_dotenv()

CACHE_FILE = "geocode_cache.json"
# One-time seed: every address already geocoded in Supabase (from the
# original populate_mvp_supabase.py pass), exported so the FIRST live run
# of this pipeline doesn't start from an empty cache and waste budget
# re-geocoding ~23k addresses that already have real coordinates on file.
# The GitHub Actions cache for CACHE_FILE only gets populated once a real
# run actually calls save_cache() below -- before that, this is the only
# thing standing between a fresh cache and a very expensive first run.
# Safe to leave in place permanently: once CACHE_FILE exists (which it
# will, from here on), load_cache() below prefers it and never touches
# this seed again.
SEED_CACHE_FILE = "geocode_cache_seed.json"
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY", "")
GEOCODE_CONCURRENCY = int(os.getenv("GEOCODE_CONCURRENCY", "5"))
# Optional extra cap on new (uncached) API calls in a single run, independent
# of the monthly budget below. 0 = no per-run cap (a run may use as much of
# the remaining monthly budget as there is backlog for).
GEOCODE_MAX_NEW_PER_RUN = int(os.getenv("GEOCODE_MAX_NEW_PER_RUN", "0"))
# Minimum seconds between requests per slot — keeps burst well under Google's 50 QPS cap
_REQUEST_DELAY = float(os.getenv("GEOCODE_REQUEST_DELAY", "0.25"))

_GEOCODE_URL = "https://maps.googleapis.com/maps/api/geocode/json"

# ---------------------------------------------------------------------------
# Monthly budget tracking — Google's Geocoding API free tier resets monthly
# (10,000 calls/month as of this writing), not daily or per-run. The budget
# period here is deliberately calendar-month to match that reset cadence;
# if Google's actual billing-cycle reset day differs from the 1st, adjust
# _current_period() accordingly. Default budget is set slightly below the
# stated free tier as a safety margin.
# ---------------------------------------------------------------------------

BUDGET_FILE = "geocode_monthly_budget.json"
GEOCODE_MONTHLY_BUDGET = int(os.getenv("GEOCODE_MONTHLY_BUDGET", "9500"))


def _current_period() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m")


def load_monthly_budget_state() -> dict:
    period = _current_period()
    if os.path.exists(BUDGET_FILE):
        with open(BUDGET_FILE, "r", encoding="utf-8-sig") as f:
            state = json.load(f)
        if state.get("period") == period:
            return state
    # No file yet, or the calendar month rolled over — fresh budget.
    return {"period": period, "calls_made": 0}


def save_monthly_budget_state(state: dict) -> None:
    with open(BUDGET_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def remaining_monthly_budget() -> int:
    state = load_monthly_budget_state()
    return max(0, GEOCODE_MONTHLY_BUDGET - state.get("calls_made", 0))


def record_monthly_budget_usage(calls_made: int) -> None:
    if calls_made <= 0:
        return
    state = load_monthly_budget_state()
    state["calls_made"] = state.get("calls_made", 0) + calls_made
    save_monthly_budget_state(state)


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------

def load_cache() -> dict:
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r", encoding="utf-8-sig") as f:  # utf-8-sig strips BOM if present
            return json.load(f)
    if os.path.exists(SEED_CACHE_FILE):
        with open(SEED_CACHE_FILE, "r", encoding="utf-8-sig") as f:
            seed = json.load(f)
        print(f"No persisted geocode_cache.json yet -- starting from the {len(seed)}-address seed instead of empty.")
        return seed
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

    # Monthly budget is the real cap (matches Google's actual free-tier reset
    # cadence). GEOCODE_MAX_NEW_PER_RUN is an optional additional per-run
    # throttle on top of it. Sorted by address key so, since completed
    # addresses drop out of `to_geocode` in future runs (already cached),
    # each run picks up where the previous one left off instead of repeatedly
    # attempting the same addresses.
    remaining = remaining_monthly_budget()
    effective_cap = remaining if GEOCODE_MAX_NEW_PER_RUN <= 0 else min(remaining, GEOCODE_MAX_NEW_PER_RUN)
    print(f"Monthly geocode budget: {remaining}/{GEOCODE_MONTHLY_BUDGET} remaining for {_current_period()}")

    if effective_cap <= 0:
        for _, group in to_geocode:
            for loc in group:
                loc["geocode_status"] = "pending"
        print(
            f"Monthly geocode budget exhausted — 0 API calls this run, "
            f"{len(to_geocode)} addresses deferred to next month (geocode_status=pending)"
        )
        return locations

    if len(to_geocode) > effective_cap:
        to_geocode.sort(key=lambda kv: kv[0])
        deferred = len(to_geocode) - effective_cap
        to_geocode = to_geocode[:effective_cap]
        print(
            f"Budget cap {effective_cap} — geocoding {len(to_geocode)} addresses this run, "
            f"deferring {deferred} (left as geocode_status=pending)"
        )

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
    # Record conservatively: every attempted call counts against the monthly
    # budget regardless of success/failure, so a run of denied/errored calls
    # can't silently blow past the free tier.
    record_monthly_budget_usage(len(to_geocode))
    failed = [loc for loc in locations if loc.get("geocode_status") == "failed"]
    ok = [loc for loc in locations if loc.get("geocode_status") == "ok"]
    remaining_after = remaining_monthly_budget()
    print(
        f"Geocoding complete: {len(to_geocode)} API calls made this run — "
        f"{len(ok)} providers resolved, {len(failed)} providers failed (will retry next run). "
        f"Monthly budget remaining: {remaining_after}/{GEOCODE_MONTHLY_BUDGET}"
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
                "issue": "Geocoding failed -- no lat/lng, won't appear on the map or in state/city searches until resolved",
                "provider_name": l.get("provider_name"),
                "dba_name": l.get("dba_name"),
                "street_address": l.get("street_address"),
                "city": l.get("city"),
                "state": l.get("state"),
                "zip": l.get("zip"),
                "phone": l.get("phone"),
                "website": l.get("website"),
                "achc_company_id": l.get("achc_company_id"),
                "source_url": l.get("source_url"),
            } for l in failed], _f, indent=2)
    return locations
