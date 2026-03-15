import asyncio
import os
import random
import re
from datetime import datetime
from typing import List, Tuple

import aiohttp
from dotenv import load_dotenv
from playwright.async_api import async_playwright

load_dotenv()

AMS_URL = "https://ams.achc.org/accredited_organizations.aspx"

PROGRAMS = ["Home Care", "Home Health", "Hospice"]

# Single-state test
SEARCH_STATES = ["Illinois"]

LIMIT_LOCATIONS = int(os.getenv("LIMIT_LOCATIONS", "0"))
GOOGLE_SHEETS_URL = os.getenv("GOOGLE_SHEETS_WEB_APP_URL")

if not GOOGLE_SHEETS_URL:
    raise ValueError("Missing GOOGLE_SHEETS_WEB_APP_URL in environment variables")

# -----------------------------
# State normalization
# -----------------------------

STATE_DISPLAY_MAP = {
    "District of Columbia": "Washington, D.C.",
    "DC": "Washington, D.C."
}

STATE_ABBR_MAP = {
    "Illinois": "IL",
    "District of Columbia": "DC"
}

# -----------------------------
# Country helpers
# -----------------------------

COUNTRY_PREFERRED_LABELS = [
    "USA",
    "United States",
    "United States of America",
    "U.S.A.",
    "U.S.",
    "US"
]

COUNTRY_PARTIAL_MATCHES = [
    "united states",
    "usa",
    "u.s.a",
    "u.s.",
    " us "
]


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip().lower())


# -----------------------------
# Parsing helpers
# -----------------------------

def split_city_state_zip(text: str) -> Tuple[str, str, str]:

    if not text:
        return "", "", ""

    match = re.match(r"^(.*?),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)$", text.strip())

    if not match:
        return "", "", ""

    return match.group(1), match.group(2), match.group(3)


def normalize_state_display(search_label: str, scraped_state_abbr: str = "") -> str:

    if scraped_state_abbr:
        for k, v in STATE_ABBR_MAP.items():
            if v == scraped_state_abbr:
                return k

    return STATE_DISPLAY_MAP.get(search_label, search_label)


def normalize_state_abbr(search_label: str, scraped_state_abbr: str = "") -> str:

    if scraped_state_abbr:
        return scraped_state_abbr

    return STATE_ABBR_MAP.get(search_label, "")


# -----------------------------
# Dropdown detection
# -----------------------------

async def find_select_with_programs(page):

    selects = page.locator("select")

    for i in range(await selects.count()):

        sel = selects.nth(i)
        opts = [t.strip() for t in await sel.locator("option").all_inner_texts()]

        if "Home Care" in opts and "Home Health" in opts:
            return sel

    return None


async def find_select_with_states(page):

    state_markers = {
        "Illinois",
        "California",
        "Texas",
        "Florida",
        "New York",
        "District of Columbia"
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

            options.append({
                "label": label,
                "value": value
            })

        normalized_labels = [normalize_text(o["label"]) for o in options]

        if not any(x in " ".join(normalized_labels) for x in ["usa", "united states"]):
            continue

        for preferred in COUNTRY_PREFERRED_LABELS:

            pref_norm = normalize_text(preferred)

            for option in options:
                if normalize_text(option["label"]) == pref_norm:
                    return sel, option["value"], option["label"]

        for option in options:

            label_norm = normalize_text(option["label"])

            if "united states" in label_norm or label_norm in ["usa", "us"]:
                return sel, option["value"], option["label"]

    return None, None, None


# -----------------------------
# Search submission
# -----------------------------

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

    await page.wait_for_load_state("networkidle")
    await page.wait_for_timeout(2500)


# -----------------------------
# RESULT PARSER
# -----------------------------

async def scrape_current_page_rows(page, program: str, state_label: str):

    rows = []

    blocks = page.locator("text=Show/Hide Accreditation Details").locator(
        "xpath=ancestor::tr"
    )

    block_count = await blocks.count()

    for i in range(block_count):

        block = blocks.nth(i)

        text = await block.inner_text()

        lines = [l.strip() for l in text.split("\n") if l.strip()]

        if len(lines) < 3:
            continue

        name = lines[0]
        address = lines[1]
        city_state_zip = lines[2]

        city, scraped_state_abbr, zipc = split_city_state_zip(city_state_zip)

        state_display = normalize_state_display(state_label, scraped_state_abbr)
        state_abbr = normalize_state_abbr(state_label, scraped_state_abbr)

        rows.append({
            "organization": name,
            "program": program,
            "address": address,
            "city": city,
            "state": state_display,
            "state_abbr": state_abbr,
            "zip": zipc,
            "phone": "",
            "latitude": "",
            "longitude": "",
            "source_url": AMS_URL,
            "last_seen": datetime.utcnow().isoformat()
        })

    return rows


# -----------------------------
# Debug helpers
# -----------------------------

async def save_debug_artifacts(page, label: str):

    os.makedirs("debug", exist_ok=True)

    safe = re.sub(r"[^a-zA-Z0-9_-]", "_", label)

    await page.screenshot(path=f"debug/{safe}.png", full_page=True)

    html = await page.content()

    with open(f"debug/{safe}.html", "w", encoding="utf-8") as f:
        f.write(html)


# -----------------------------
# Deduplication
# -----------------------------

def deduplicate_rows(rows: List[dict]) -> List[dict]:

    seen = set()
    unique = []

    for row in rows:

        key = (
            row["organization"].lower(),
            row["address"].lower(),
            row["city"].lower(),
            row["zip"]
        )

        if key not in seen:
            seen.add(key)
            unique.append(row)

    return unique


# -----------------------------
# Scraper driver
# -----------------------------

async def run_scrape():

    all_rows = []
    debug_saved = 0

    async with async_playwright() as p:

        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        for program in PROGRAMS:
            for state_label in SEARCH_STATES:

                print(f"Fetching: {program} / {state_label}")

                await page.goto(AMS_URL)

                prog_select = await find_select_with_programs(page)
                state_select = await find_select_with_states(page)

                country_select, country_value, country_label = \
                    await find_country_select_and_value(page)

                if not prog_select or not state_select or not country_select:
                    print("Could not find dropdowns")

                    if debug_saved < 2:
                        await save_debug_artifacts(page, "dropdown_failure")
                        debug_saved += 1

                    continue

                await country_select.select_option(value=country_value)
                print("Selected country:", country_label)

                await prog_select.select_option(label=program)
                await state_select.select_option(label=state_label)

                await click_search(page)

                await wait_for_results(page)

                page_rows = await scrape_current_page_rows(page, program, state_label)

                print(f"Found {len(page_rows)} rows")

                if not page_rows and debug_saved < 3:

                    await save_debug_artifacts(page, f"no_rows_{program}_{state_label}")
                    debug_saved += 1

                all_rows.extend(page_rows)

        await browser.close()

    return all_rows


# -----------------------------
# Google Sheets writer
# -----------------------------

async def write_to_google_sheets(rows: List[dict]):

    payload = {
        "action": "replace_all",
        "rows": rows
    }

    async with aiohttp.ClientSession() as session:

        async with session.post(
            GOOGLE_SHEETS_URL,
            json=payload,
            headers={"Content-Type": "application/json"}
        ) as response:

            text = await response.text()

            print("Google Sheets response:", response.status)

            if response.status != 200:
                raise Exception(text)


# -----------------------------
# Main
# -----------------------------

async def main():

    print("Starting ACHC scraper...")

    rows = await run_scrape()

    if not rows:
        raise Exception("No rows scraped")

    unique_rows = deduplicate_rows(rows)

    print("Total rows:", len(unique_rows))

    await write_to_google_sheets(unique_rows)

    print("Scraper completed successfully")


if __name__ == "__main__":
    asyncio.run(main())
