import asyncio
import os
import random
import re
from datetime import datetime
from typing import List

import aiohttp
from dotenv import load_dotenv
from playwright.async_api import async_playwright

load_dotenv()

AMS_URL = "https://ams.achc.org/accredited_organizations.aspx"

DEFAULT_PROGRAMS = ["Home Health"]
DEFAULT_STATES = ["Texas"]

LIMIT_LOCATIONS = int(os.getenv("LIMIT_LOCATIONS", "25"))
GOOGLE_SHEETS_URL = os.getenv("GOOGLE_SHEETS_WEB_APP_URL")
TEST_MODE = os.getenv("TEST_MODE", "false").lower() == "true"

TEST_PROGRAMS_ENV = os.getenv("TEST_PROGRAMS", "").strip()
TEST_STATES_ENV = os.getenv("TEST_STATES", "").strip()
NO_STATE_FILTER = os.getenv("NO_STATE_FILTER", "false").lower() == "true"

PROGRAMS = [p.strip() for p in TEST_PROGRAMS_ENV.split(",") if p.strip()] if TEST_PROGRAMS_ENV else DEFAULT_PROGRAMS

if NO_STATE_FILTER:
    SEARCH_STATES = [None]
else:
    SEARCH_STATES = [s.strip() for s in TEST_STATES_ENV.split(",") if s.strip()] if TEST_STATES_ENV else DEFAULT_STATES

if not GOOGLE_SHEETS_URL:
    raise ValueError("Missing GOOGLE_SHEETS_WEB_APP_URL in environment variables")

COUNTRY_PREFERRED_LABELS = [
    "USA",
    "United States",
    "United States of America",
    "U.S.A.",
    "U.S.",
    "US"
]


async def polite_pause(min_seconds: float = 0.12, max_seconds: float = 0.28):
    await asyncio.sleep(random.uniform(min_seconds, max_seconds))


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip().lower())


async def find_select_with_programs(page):
    selects = page.locator("select")
    for i in range(await selects.count()):
        sel = selects.nth(i)
        opts = [t.strip() for t in await sel.locator("option").all_inner_texts()]
        if "Home Care" in opts and "Home Health" in opts and "Hospice" in opts:
            return sel
    return None


async def find_select_with_states(page):
    state_markers = {
        "Illinois", "California", "Texas", "Florida", "New York", "District of Columbia"
    }
    selects = page.locator("select")
    for i in range(await selects.count()):
        sel = selects.nth(i)
        opts = [t.strip() for t in await sel.locator("option").all_inner_texts()]
        hits = sum(1 for o in opts if o in state_markers)
        if hits >= 4:
            return sel
    return None


async def find_country_select_and_value(page):
    selects = page.locator("select")

    for i in range(await selects.count()):
        sel = selects.nth(i)
        option_locator = sel.locator("option")
        option_count = await option_locator.count()

        options = []
        for j in range(option_count):
            opt = option_locator.nth(j)
            label = (await opt.inner_text()).strip()
            value = (await opt.get_attribute("value")) or ""
            options.append({"label": label, "value": value})

        normalized_labels = [normalize_text(o["label"]) for o in options]
        joined = " ".join(normalized_labels)

        if "usa" not in joined and "united states" not in joined:
            continue

        for preferred in COUNTRY_PREFERRED_LABELS:
            pref_norm = normalize_text(preferred)
            for option in options:
                if normalize_text(option["label"]) == pref_norm:
                    return sel, option["value"], option["label"]

        for option in options:
            label_norm = normalize_text(option["label"])
            if "united states" in label_norm or label_norm in {"usa", "us"}:
                return sel, option["value"], option["label"]

    return None, None, None


async def click_search(page):
    selectors = [
        "input[value='Find']",
        "input[value='Search']",
        "button:has-text('Find')",
        "button:has-text('Search')"
    ]
    for sel in selectors:
        if await page.locator(sel).count():
            await page.locator(sel).first.click()
            return
    await page.keyboard.press("Enter")


async def wait_for_results(page):
    await page.wait_for_timeout(1800)


async def scrape_raw_rows(page, searched_program: str, searched_state_label: str) -> List[dict]:
    """
    Raw-only extraction.
    Finds all detail links in the DOM and stores the surrounding text block as raw text.
    """
    rows = []

    detail_links = page.locator("a:has-text('Show/Hide Accreditation Details')")
    link_count = await detail_links.count()
    print(f"  Detail links found in DOM: {link_count}")

    for i in range(link_count):
        link = detail_links.nth(i)

        candidate_locators = [
            link.locator("xpath=ancestor::tr[1]"),
            link.locator("xpath=ancestor::div[1]")
        ]

        raw_text = ""
        container_type = ""

        for idx, candidate in enumerate(candidate_locators):
            try:
                candidate_text = await candidate.inner_text()
                if candidate_text and "Show/Hide Accreditation Details" in candidate_text:
                    raw_text = candidate_text.strip()
                    container_type = "tr" if idx == 0 else "div"
                    break
            except Exception:
                continue

        if not raw_text:
            continue

        rows.append({
            "raw_index": i + 1,
            "container_type": container_type,
            "searched_program_type": searched_program,
            "searched_state": searched_state_label,
            "raw_text": raw_text,
            "source_url": AMS_URL,
            "last_seen": datetime.utcnow().isoformat()
        })

        if LIMIT_LOCATIONS > 0 and len(rows) >= LIMIT_LOCATIONS:
            break

    return rows


async def scrape_program_state(page, program: str, state_label: str) -> List[dict]:
    state_log = state_label if state_label else "NO STATE FILTER"
    print(f"Fetching: {program} / {state_log}")

    await page.goto(AMS_URL, timeout=60000)
    await page.wait_for_load_state("domcontentloaded")

    prog_select = await find_select_with_programs(page)
    state_select = await find_select_with_states(page)
    country_select, country_value, country_label = await find_country_select_and_value(page)

    if not prog_select or not state_select or not country_select or not country_value:
        raise Exception("Could not locate one or more dropdowns")

    await country_select.select_option(value=country_value)
    print(f"  Selected country option: {country_label}")

    await prog_select.select_option(label=program)
    
    if state_label:
        await state_select.select_option(label=state_label)
        print(f"  Selected state: {state_label}")
    else:
        print("  No state filter selected")
    
    await polite_pause()

    await click_search(page)
    await wait_for_results(page)

    rows = await scrape_raw_rows(page, program, state_label)
    print(f"  Found {len(rows)} raw rows")

    return rows


async def run_scrape() -> List[dict]:
    all_rows = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--disable-dev-shm-usage", "--no-sandbox"]
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122 Safari/537.36"
        )
        page = await context.new_page()

        for program in PROGRAMS:
            for state_label in SEARCH_STATES:
                rows = await scrape_program_state(page, program, state_label)
                all_rows.extend(rows)

                if LIMIT_LOCATIONS > 0 and len(all_rows) >= LIMIT_LOCATIONS:
                    all_rows = all_rows[:LIMIT_LOCATIONS]
                    break

            if LIMIT_LOCATIONS > 0 and len(all_rows) >= LIMIT_LOCATIONS:
                break

        await browser.close()

    return all_rows


async def write_to_google_sheets(raw_rows: List[dict]):
    payload = {
        "action": "replace_raw_only",
        "test_mode": TEST_MODE,
        "raw_rows": raw_rows
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(
            GOOGLE_SHEETS_URL,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=aiohttp.ClientTimeout(total=180)
        ) as response:
            text = await response.text()
            print(f"Google Sheets response status: {response.status}")
            print(f"Google Sheets response body: {text[:500]}")
            if response.status != 200:
                raise Exception(f"Google Sheets write failed with HTTP {response.status}: {text}")


async def main():
    print("Starting ACHC raw dump...")
    print(f"TEST_MODE: {TEST_MODE}")
    print(f"PROGRAMS: {PROGRAMS}")
    print(f"SEARCH_STATES: {SEARCH_STATES}")

    raw_rows = await run_scrape()

    if not raw_rows:
        raise Exception("No rows scraped")

    print(f"Raw rows captured: {len(raw_rows)}")
    print("Sample raw rows:")
    for row in raw_rows[:3]:
        print({
            "raw_index": row["raw_index"],
            "searched_program_type": row["searched_program_type"],
            "searched_state": row["searched_state"],
            "container_type": row["container_type"],
            "raw_text_preview": row["raw_text"][:200]
        })

    await write_to_google_sheets(raw_rows)

    print("Raw data written to Google Sheets")
    print("Raw-only test completed successfully")


if __name__ == "__main__":
    asyncio.run(main())
