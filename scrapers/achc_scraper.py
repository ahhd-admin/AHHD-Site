import asyncio
import os
import random
import re
from datetime import datetime
from typing import List, Tuple

import aiohttp
from dotenv import load_dotenv
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

from geocode_helper import geocode_locations

load_dotenv()

AMS_URL = "https://ams.achc.org/accredited_organizations.aspx"

PROGRAMS = ["Home Care", "Home Health", "Hospice"]

# These are the labels used to SEARCH the ACHC tool
SEARCH_STATES = [
    "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut", "Delaware",
    "District of Columbia", "Florida", "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa",
    "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota",
    "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada", "New Hampshire", "New Jersey",
    "New Mexico", "New York", "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon",
    "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah",
    "Vermont", "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming"
]

# Standardized display labels for output
STATE_DISPLAY_MAP = {
    "Alabama": "Alabama",
    "Alaska": "Alaska",
    "Arizona": "Arizona",
    "Arkansas": "Arkansas",
    "California": "California",
    "Colorado": "Colorado",
    "Connecticut": "Connecticut",
    "Delaware": "Delaware",
    "District of Columbia": "Washington, D.C.",
    "Washington, D.C.": "Washington, D.C.",
    "DC": "Washington, D.C.",
    "Florida": "Florida",
    "Georgia": "Georgia",
    "Hawaii": "Hawaii",
    "Idaho": "Idaho",
    "Illinois": "Illinois",
    "Indiana": "Indiana",
    "Iowa": "Iowa",
    "Kansas": "Kansas",
    "Kentucky": "Kentucky",
    "Louisiana": "Louisiana",
    "Maine": "Maine",
    "Maryland": "Maryland",
    "Massachusetts": "Massachusetts",
    "Michigan": "Michigan",
    "Minnesota": "Minnesota",
    "Mississippi": "Mississippi",
    "Missouri": "Missouri",
    "Montana": "Montana",
    "Nebraska": "Nebraska",
    "Nevada": "Nevada",
    "New Hampshire": "New Hampshire",
    "New Jersey": "New Jersey",
    "New Mexico": "New Mexico",
    "New York": "New York",
    "North Carolina": "North Carolina",
    "North Dakota": "North Dakota",
    "Ohio": "Ohio",
    "Oklahoma": "Oklahoma",
    "Oregon": "Oregon",
    "Pennsylvania": "Pennsylvania",
    "Rhode Island": "Rhode Island",
    "South Carolina": "South Carolina",
    "South Dakota": "South Dakota",
    "Tennessee": "Tennessee",
    "Texas": "Texas",
    "Utah": "Utah",
    "Vermont": "Vermont",
    "Virginia": "Virginia",
    "Washington": "Washington",
    "West Virginia": "West Virginia",
    "Wisconsin": "Wisconsin",
    "Wyoming": "Wyoming"
}

STATE_ABBR_MAP = {
    "Alabama": "AL",
    "Alaska": "AK",
    "Arizona": "AZ",
    "Arkansas": "AR",
    "California": "CA",
    "Colorado": "CO",
    "Connecticut": "CT",
    "Delaware": "DE",
    "District of Columbia": "DC",
    "Washington, D.C.": "DC",
    "DC": "DC",
    "Florida": "FL",
    "Georgia": "GA",
    "Hawaii": "HI",
    "Idaho": "ID",
    "Illinois": "IL",
    "Indiana": "IN",
    "Iowa": "IA",
    "Kansas": "KS",
    "Kentucky": "KY",
    "Louisiana": "LA",
    "Maine": "ME",
    "Maryland": "MD",
    "Massachusetts": "MA",
    "Michigan": "MI",
    "Minnesota": "MN",
    "Mississippi": "MS",
    "Missouri": "MO",
    "Montana": "MT",
    "Nebraska": "NE",
    "Nevada": "NV",
    "New Hampshire": "NH",
    "New Jersey": "NJ",
    "New Mexico": "NM",
    "New York": "NY",
    "North Carolina": "NC",
    "North Dakota": "ND",
    "Ohio": "OH",
    "Oklahoma": "OK",
    "Oregon": "OR",
    "Pennsylvania": "PA",
    "Rhode Island": "RI",
    "South Carolina": "SC",
    "South Dakota": "SD",
    "Tennessee": "TN",
    "Texas": "TX",
    "Utah": "UT",
    "Vermont": "VT",
    "Virginia": "VA",
    "Washington": "WA",
    "West Virginia": "WV",
    "Wisconsin": "WI",
    "Wyoming": "WY"
}

LIMIT_LOCATIONS = int(os.getenv("LIMIT_LOCATIONS", "0"))
GOOGLE_SHEETS_URL = os.getenv("GOOGLE_SHEETS_WEB_APP_URL")

if not GOOGLE_SHEETS_URL:
    raise ValueError("Missing GOOGLE_SHEETS_WEB_APP_URL in environment variables")


async def polite_pause(min_seconds: float = 1.2, max_seconds: float = 2.2):
    await asyncio.sleep(random.uniform(min_seconds, max_seconds))


def split_city_state_zip(text: str) -> Tuple[str, str, str]:
    if not text:
        return ("", "", "")

    # Expects two-letter state from scraped address line, e.g. "Chicago, IL 60601"
    m = re.match(r"^(.*?),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)$", text.strip())
    if not m:
        return ("", "", "")

    return m.group(1), m.group(2), m.group(3)


def normalize_state_display(search_label: str, scraped_state_abbr: str = "") -> str:
    if scraped_state_abbr:
        # Reverse lookup from abbreviation to display name
        for state_name, abbr in STATE_ABBR_MAP.items():
            if abbr == scraped_state_abbr and state_name in STATE_DISPLAY_MAP:
                return STATE_DISPLAY_MAP[state_name]

    return STATE_DISPLAY_MAP.get(search_label, search_label)


def normalize_state_abbr(search_label: str, scraped_state_abbr: str = "") -> str:
    if scraped_state_abbr:
        return scraped_state_abbr
    return STATE_ABBR_MAP.get(search_label, "")


async def find_select_with_programs(page):
    selects = page.locator("select")
    for i in range(await selects.count()):
        sel = selects.nth(i)
        opts = [t.strip() for t in await sel.locator("option").all_inner_texts()]
        if all(any(p in o for o in opts) for p in ["Home Care", "Home Health", "Hospice"]):
            return sel
    return None


async def find_select_with_states(page):
    states_set = set(SEARCH_STATES)
    selects = page.locator("select")
    for i in range(await selects.count()):
        sel = selects.nth(i)
        opts = [t.strip() for t in await sel.locator("option").all_inner_texts()]
        hits = sum(1 for o in opts if o in states_set)
        if hits > 30:
            return sel
    return None


async def click_search(page):
    candidates = [
        "input[value='Search']",
        "#MainContent_btnSearch",
        "input[type='submit']",
        "button:has-text('Search')",
        "button:has-text('Submit')"
    ]
    for sel in candidates:
        if await page.locator(sel).count():
            await page.locator(sel).first.click()
            return
    await page.keyboard.press("Enter")


async def wait_for_results_or_no_results(page):
    candidates = [
        "table",
        "text=No records",
        "text=No results",
        "text=0 results",
        "#MainContent_UpdatePanel1",
        "body"
    ]

    for selector in candidates:
        try:
            await page.locator(selector).first.wait_for(timeout=5000)
            return
        except PlaywrightTimeoutError:
            continue


async def scrape_current_page_rows(page, program: str, state_label: str) -> List[dict]:
    rows = []
    tables = page.locator("table")
    table_count = await tables.count()

    for i in range(table_count):
        table = tables.nth(i)

        try:
            trs = await table.locator("tr").all()
        except Exception:
            continue

        if len(trs) < 2:
            continue

        parsed_any = False

        for tr in trs[1:]:
            tds = [td.strip() for td in await tr.locator("td").all_inner_texts()]

            if len(tds) < 3:
                continue

            name = tds[0]
            addr1 = tds[1]
            csz = tds[2]
            phone = tds[3] if len(tds) > 3 else ""

            city, scraped_state_abbr, zipc = split_city_state_zip(csz)

            if not name or not addr1:
                continue

            state_display = normalize_state_display(state_label, scraped_state_abbr)
            state_abbr = normalize_state_abbr(state_label, scraped_state_abbr)

            rows.append({
                "organization": name,
                "program": program,
                "address": addr1,
                "city": city,
                "state": state_display,
                "state_abbr": state_abbr,
                "zip": zipc,
                "phone": phone,
                "latitude": "",
                "longitude": "",
                "source_url": AMS_URL,
                "last_seen": datetime.utcnow().isoformat()
            })
            parsed_any = True

        if parsed_any:
            break

    return rows


async def save_debug_artifacts(page, label: str):
    os.makedirs("debug", exist_ok=True)
    safe_label = re.sub(r"[^a-zA-Z0-9_-]", "_", label)
    await page.screenshot(path=f"debug/{safe_label}.png", full_page=True)
    html = await page.content()
    with open(f"debug/{safe_label}.html", "w", encoding="utf-8") as f:
        f.write(html)


def deduplicate_rows(rows: List[dict]) -> List[dict]:
    seen = set()
    unique = []

    for row in rows:
        key = (
            row["organization"].strip().lower(),
            row["address"].strip().lower(),
            row["city"].strip().lower(),
            row["state_abbr"].strip().upper(),
            row["zip"].strip()
        )
        if key not in seen:
            seen.add(key)
            unique.append(row)

    return unique


async def run_scrape() -> List[dict]:
    all_rows = []
    zero_result_checks = 0

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122 Safari/537.36"
        )
        page = await context.new_page()

        for program in PROGRAMS:
            for state_label in SEARCH_STATES:
                if LIMIT_LOCATIONS > 0 and len(all_rows) >= LIMIT_LOCATIONS:
                    print(f"Reached limit of {LIMIT_LOCATIONS} locations. Stopping.")
                    all_rows = all_rows[:LIMIT_LOCATIONS]
                    break

                print(f"Fetching: {program} / {state_label}")

                await page.goto(AMS_URL, timeout=90000)
                await page.wait_for_load_state("domcontentloaded")

                prog_select = await find_select_with_programs(page)
                state_select = await find_select_with_states(page)

                if prog_select is None or state_select is None:
                    print("Could not locate dropdowns. Saving debug artifact.")
                    await save_debug_artifacts(page, f"missing_dropdowns_{program}_{state_label}")
                    continue

                await prog_select.select_option(label=program)
                await state_select.select_option(label=state_label)
                await polite_pause(0.6, 1.0)

                await click_search(page)
                await wait_for_results_or_no_results(page)
                await polite_pause()

                page_rows = await scrape_current_page_rows(page, program, state_label)
                print(f"  Found {len(page_rows)} rows on first page")

                if not page_rows:
                    zero_result_checks += 1
                    if zero_result_checks <= 3:
                        await save_debug_artifacts(page, f"no_rows_{program}_{state_label}")
                else:
                    zero_result_checks = 0

                all_rows.extend(page_rows)

                while page_rows:
                    if LIMIT_LOCATIONS > 0 and len(all_rows) >= LIMIT_LOCATIONS:
                        all_rows = all_rows[:LIMIT_LOCATIONS]
                        break

                    next_link = page.locator("a:has-text('Next'), a[title*='Next']")
                    if await next_link.count() == 0:
                        break

                    await next_link.first.click()
                    await wait_for_results_or_no_results(page)
                    await polite_pause(1.0, 1.8)

                    page_rows = await scrape_current_page_rows(page, program, state_label)
                    print(f"  Found {len(page_rows)} rows on next page")

                    if not page_rows:
                        break

                    all_rows.extend(page_rows)

                await polite_pause(1.0, 1.8)

            if LIMIT_LOCATIONS > 0 and len(all_rows) >= LIMIT_LOCATIONS:
                break

        await browser.close()

    return all_rows


async def write_to_google_sheets(rows: List[dict]):
    payload = {
        "action": "replace_all",
        "rows": rows
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(
            GOOGLE_SHEETS_URL,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=aiohttp.ClientTimeout(total=120)
        ) as response:
            text = await response.text()
            print(f"Google Sheets response status: {response.status}")
            print(f"Google Sheets response body: {text[:500]}")

            if response.status != 200:
                raise Exception(f"Google Sheets write failed with HTTP {response.status}: {text}")


async def main():
    print("Starting ACHC scraper...")

    rows = await run_scrape()

    if not rows:
        raise Exception("No rows scraped")

    unique_rows = deduplicate_rows(rows)
    print(f"Scraped {len(rows)} total rows, {len(unique_rows)} unique")

    #print("Geocoding locations...")
    #geocoded_rows = await geocode_locations(unique_rows)

    #geocoded_count = sum(1 for r in geocoded_rows if r.get("latitude") is not None)
    #print(f"Geocoded {geocoded_count}/{len(geocoded_rows)} locations")

    await write_to_google_sheets(geocoded_rows)

    print("Scraper completed successfully")
    print("Data written to Google Sheets")


if __name__ == "__main__":
    asyncio.run(main())
