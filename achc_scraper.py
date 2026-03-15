import asyncio
import re
import os
import json
from datetime import datetime
from typing import List, Tuple, Optional
from playwright.async_api import async_playwright
import aiohttp
from dotenv import load_dotenv
from geocode_helper import geocode_locations

load_dotenv()

AMS_URL = "https://ams.achc.org/accredited_organizations.aspx"

PROGRAMS = ["Home Care", "Home Health", "Hospice"]
STATES = [
    "Alabama","Alaska","Arizona","Arkansas","California","Colorado","Connecticut","Delaware",
    "District of Columbia","Florida","Georgia","Hawaii","Idaho","Illinois","Indiana","Iowa",
    "Kansas","Kentucky","Louisiana","Maine","Maryland","Massachusetts","Michigan","Minnesota",
    "Mississippi","Missouri","Montana","Nebraska","Nevada","New Hampshire","New Jersey",
    "New Mexico","New York","North Carolina","North Dakota","Ohio","Oklahoma","Oregon",
    "Pennsylvania","Rhode Island","South Carolina","South Dakota","Tennessee","Texas","Utah",
    "Vermont","Virginia","Washington","West Virginia","Wisconsin","Wyoming"
]

LIMIT_LOCATIONS = int(os.getenv("LIMIT_LOCATIONS", "0"))

GOOGLE_SHEETS_URL = os.getenv("GOOGLE_SHEETS_WEB_APP_URL")

if not GOOGLE_SHEETS_URL:
    raise ValueError("Missing GOOGLE_SHEETS_WEB_APP_URL in .env")

def split_city_state_zip(text: str) -> Tuple[str, str, str]:
    if not text:
        return ("", "", "")
    m = re.match(r"^(.*?),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)$", text.strip())
    if not m:
        return ("", "", "")
    return m.group(1), m.group(2), m.group(3)

def normalize_org_name(name: str) -> str:
    cleaned = re.sub(r'[^a-z0-9\s-]', '', name.lower())
    cleaned = re.sub(r'\s+', '-', cleaned.strip())
    return cleaned[:100]

async def find_select_with_programs(page):
    selects = page.locator("select")
    for i in range(await selects.count()):
        sel = selects.nth(i)
        opts = [t.strip() for t in await sel.locator("option").all_inner_texts()]
        if all(any(p in o for o in opts) for p in ["Home Care","Home Health","Hospice"]):
            return sel
    return None

async def find_select_with_states(page):
    states_set = set(STATES)
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
            await page.click(sel)
            return
    await page.keyboard.press("Enter")

async def scrape_current_page_rows(page, program: str, state_label: str) -> List[dict]:
    rows = []
    tables = page.locator("table")
    for i in range(await tables.count()):
        table = tables.nth(i)
        if await table.locator("th").count() == 0:
            continue
        trs = await table.locator("tr").all()
        if len(trs) < 2:
            continue
        for tr in trs[1:]:
            tds = await tr.locator("td").all_inner_texts()
            if len(tds) < 3:
                continue
            name = tds[0].strip()
            addr1 = tds[1].strip()
            csz = tds[2].strip()
            phone = tds[3].strip() if len(tds) > 3 else ""
            city, st, zipc = split_city_state_zip(csz)
            rows.append({
                "organization": name,
                "program": program,
                "address": addr1,
                "city": city,
                "state": st if st else state_label,
                "zip": zipc,
                "phone": phone,
                "source_url": AMS_URL,
                "last_seen": datetime.utcnow().isoformat()
            })
        if rows:
            break
    return rows

async def run_scrape() -> List[dict]:
    all_rows = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="AHHD-directory-research/1.0 (+https://accreditedhomehealthcare.directory)"
        )
        page = await context.new_page()

        for program in PROGRAMS:
            for state_label in STATES:
                if LIMIT_LOCATIONS > 0 and len(all_rows) >= LIMIT_LOCATIONS:
                    print(f"Reached limit of {LIMIT_LOCATIONS} locations. Stopping.")
                    break

                print(f"Fetching: {program} / {state_label}")
                await page.goto(AMS_URL, timeout=90000)
                prog_select = await find_select_with_programs(page)
                state_select = await find_select_with_states(page)
                if prog_select is None or state_select is None:
                    print("  ! Could not locate dropdowns. Skipping.")
                    continue

                await prog_select.select_option(label=program)
                await state_select.select_option(label=state_label)
                await click_search(page)
                await page.wait_for_load_state("networkidle")

                while True:
                    page_rows = await scrape_current_page_rows(page, program, state_label)
                    all_rows.extend(page_rows)

                    if LIMIT_LOCATIONS > 0 and len(all_rows) >= LIMIT_LOCATIONS:
                        break

                    next_link = page.locator("a:has-text('Next'), a[title*='Next']")
                    if await next_link.count() == 0:
                        break
                    await next_link.first.click()
                    await page.wait_for_load_state("networkidle")
                    await page.wait_for_timeout(500)

                await page.wait_for_timeout(2500)

            if LIMIT_LOCATIONS > 0 and len(all_rows) >= LIMIT_LOCATIONS:
                break

        await browser.close()

    return all_rows

def deduplicate_rows(rows: List[dict]) -> List[dict]:
    seen = set()
    unique = []
    for row in rows:
        key = (row["organization"], row["address"], row["city"], row["state"], row["zip"])
        if key not in seen:
            seen.add(key)
            unique.append(row)
    return unique

async def write_to_google_sheets(rows: List[dict]):
    async with aiohttp.ClientSession() as session:
        async with session.post(
            GOOGLE_SHEETS_URL,
            json=rows,
            headers={'Content-Type': 'application/json'}
        ) as response:
            result = await response.json()
            if result.get('success'):
                print(f"Wrote {result.get('count', len(rows))} rows to Google Sheets")
            else:
                raise Exception(f"Failed to write to Google Sheets: {result}")

async def main():
    print("Starting ACHC scraper...")

    try:
        rows = await run_scrape()

        if not rows:
            raise Exception("No rows scraped")

        unique_rows = deduplicate_rows(rows)
        print(f"Scraped {len(rows)} total rows, {len(unique_rows)} unique")

        print("Geocoding locations...")
        geocoded_rows = await geocode_locations(unique_rows)
        geocoded_count = sum(1 for r in geocoded_rows if r.get("latitude"))
        print(f"Geocoded {geocoded_count}/{len(unique_rows)} locations")

        await write_to_google_sheets(geocoded_rows)

        print(f"Scraper completed successfully!")
        print(f"Data written to Google Sheets")

    except Exception as e:
        print(f"Error during scraping: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
