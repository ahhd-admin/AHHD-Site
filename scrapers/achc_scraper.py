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

DEFAULT_PROGRAMS = ["Home Care", "Home Health", "Hospice"]
DEFAULT_TRIGGER_STATE = "Texas"

LIMIT_LOCATIONS = int(os.getenv("LIMIT_LOCATIONS", "25"))
GOOGLE_SHEETS_URL = os.getenv("GOOGLE_SHEETS_WEB_APP_URL")
TEST_MODE = os.getenv("TEST_MODE", "false").lower() == "true"

TEST_PROGRAMS_ENV = os.getenv("TEST_PROGRAMS", "").strip()
TRIGGER_STATE = os.getenv("TRIGGER_STATE", DEFAULT_TRIGGER_STATE).strip()

PROGRAMS = [p.strip() for p in TEST_PROGRAMS_ENV.split(",") if p.strip()] if TEST_PROGRAMS_ENV else DEFAULT_PROGRAMS

if not GOOGLE_SHEETS_URL:
    raise ValueError("Missing GOOGLE_SHEETS_WEB_APP_URL in environment variables")

STATE_ABBR_MAP = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR", "California": "CA",
    "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE", "District of Columbia": "DC",
    "Florida": "FL", "Georgia": "GA", "Hawaii": "HI", "Idaho": "ID", "Illinois": "IL",
    "Indiana": "IN", "Iowa": "IA", "Kansas": "KS", "Kentucky": "KY", "Louisiana": "LA",
    "Maine": "ME", "Maryland": "MD", "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN",
    "Mississippi": "MS", "Missouri": "MO", "Montana": "MT", "Nebraska": "NE", "Nevada": "NV",
    "New Hampshire": "NH", "New Jersey": "NJ", "New Mexico": "NM", "New York": "NY",
    "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH", "Oklahoma": "OK", "Oregon": "OR",
    "Pennsylvania": "PA", "Rhode Island": "RI", "South Carolina": "SC", "South Dakota": "SD",
    "Tennessee": "TN", "Texas": "TX", "Utah": "UT", "Vermont": "VT", "Virginia": "VA",
    "Washington": "WA", "West Virginia": "WV", "Wisconsin": "WI", "Wyoming": "WY"
}

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


def split_city_state_zip(text: str) -> Tuple[str, str, str]:
    if not text:
        return "", "", ""
    match = re.match(r"^(.*?),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)$", text.strip())
    if not match:
        return "", "", ""
    return match.group(1), match.group(2), match.group(3)


def parse_raw_block(raw_text: str) -> Tuple[str, str, str]:
    """
    Returns:
    - raw_name_line
    - raw_address_block
    - parsed_state_abbr
    """
    lines = [line.strip() for line in raw_text.split("\n") if line.strip()]
    lines = [line for line in lines if "Show/Hide Accreditation Details" not in line]

    if not lines:
        return "", "", ""

    raw_name_line = lines[0]
    city_state_zip = ""
    address_lines = []

    for line in lines[1:]:
        if re.search(r",\s*[A-Z]{2}\s+\d{5}(?:-\d{4})?$", line):
            city_state_zip = line
            break
        address_lines.append(line)

    raw_address_block = " | ".join(address_lines)
    _, parsed_state_abbr, _ = split_city_state_zip(city_state_zip)

    return raw_name_line, raw_address_block, parsed_state_abbr


async def find_select_with_programs(page):
    selects = page.locator("select")
    for i in range(await selects.count()):
        sel = selects.nth(i)
        opts = [t.strip() for t in await sel.locator("option").all_inner_texts()]
        if "Home Care" in opts and "Home Health" in opts and "Hospice" in opts:
            return sel
    return None


async def find_select_with_states(page):
    state_markers = {"Illinois", "California", "Texas", "Florida", "New York", "District of Columbia"}
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


async def scrape_raw_rows(page, searched_program: str, trigger_state: str) -> List[dict]:
    rows = []

    detail_links = page.locator("a:has-text('Show/Hide Accreditation Details')")
    link_count = await detail_links.count()
    print(f"  Detail links found in DOM: {link_count}")

    trigger_state_abbr = STATE_ABBR_MAP.get(trigger_state, "")

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

        raw_name_line, raw_address_block, parsed_state_abbr = parse_raw_block(raw_text)
        matches_trigger_state = parsed_state_abbr == trigger_state_abbr if parsed_state_abbr and trigger_state_abbr else False

        rows.append({
            "raw_index": i + 1,
            "container_type": container_type,
            "searched_program_type": searched_program,
            "search_trigger_state": trigger_state,
            "result_scope": f"Multi-state raw ACHC pull; trigger state = {trigger_state}",
            "raw_name_line": raw_name_line,
            "raw_address_block": raw_address_block,
            "parsed_state_abbr": parsed_state_abbr,
            "matches_trigger_state": matches_trigger_state,
            "raw_text": raw_text,
            "source_url": AMS_URL,
            "last_seen": datetime.utcnow().isoformat()
        })

        if LIMIT_LOCATIONS > 0 and len(rows) >= LIMIT_LOCATIONS:
            break

    return rows


async def scrape_program(page, program: str, trigger_state: str) -> List[dict]:
    print(f"Fetching: {program} / trigger state {trigger_state}")

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
    await state_select.select_option(label=trigger_state)
    print(f"  Selected trigger state: {trigger_state}")
    await polite_pause()

    await click_search(page)
    await wait_for_results(page)

    rows = await scrape_raw_rows(page, program, trigger_state)
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
            rows = await scrape_program(page, program, TRIGGER_STATE)
            all_rows.extend(rows)

            if LIMIT_LOCATIONS > 0 and len(all_rows) >= LIMIT_LOCATIONS:
                all_rows = all_rows[:LIMIT_LOCATIONS]
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
    print(f"TRIGGER_STATE: {TRIGGER_STATE}")

    raw_rows = await run_scrape()

    if not raw_rows:
        raise Exception("No rows scraped")

    print(f"Raw rows captured: {len(raw_rows)}")
    print("Sample raw rows:")
    for row in raw_rows[:3]:
        print({
            "raw_index": row["raw_index"],
            "searched_program_type": row["searched_program_type"],
            "search_trigger_state": row["search_trigger_state"],
            "parsed_state_abbr": row["parsed_state_abbr"],
            "matches_trigger_state": row["matches_trigger_state"],
            "raw_name_line": row["raw_name_line"]
        })

    await write_to_google_sheets(raw_rows)

    print("Raw data written to Google Sheets")
    print("Raw-only test completed successfully")


if __name__ == "__main__":
    asyncio.run(main())
