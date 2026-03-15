import asyncio
import os
import random
import re
import time
from datetime import datetime
from typing import List, Tuple, Dict, Any

import aiohttp
from dotenv import load_dotenv
from playwright.async_api import async_playwright

load_dotenv()

AMS_URL = "https://ams.achc.org/accredited_organizations.aspx"

DEFAULT_PROGRAMS = ["Home Care", "Home Health", "Hospice"]
DEFAULT_STATES = [
    "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut", "Delaware",
    "District of Columbia", "Florida", "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa",
    "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota",
    "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada", "New Hampshire", "New Jersey",
    "New Mexico", "New York", "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon",
    "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah",
    "Vermont", "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming"
]

LIMIT_LOCATIONS = int(os.getenv("LIMIT_LOCATIONS", "0"))
GOOGLE_SHEETS_URL = os.getenv("GOOGLE_SHEETS_WEB_APP_URL")
TEST_MODE = os.getenv("TEST_MODE", "false").lower() == "true"

TEST_PROGRAMS_ENV = os.getenv("TEST_PROGRAMS", "").strip()
TEST_STATES_ENV = os.getenv("TEST_STATES", "").strip()

PROGRAMS = [p.strip() for p in TEST_PROGRAMS_ENV.split(",") if p.strip()] if TEST_PROGRAMS_ENV else DEFAULT_PROGRAMS
SEARCH_STATES = [s.strip() for s in TEST_STATES_ENV.split(",") if s.strip()] if TEST_STATES_ENV else DEFAULT_STATES

if not GOOGLE_SHEETS_URL:
    raise ValueError("Missing GOOGLE_SHEETS_WEB_APP_URL in environment variables")

STATE_DISPLAY_MAP = {
    "District of Columbia": "Washington, D.C.",
    "Washington, D.C.": "Washington, D.C.",
    "DC": "Washington, D.C."
}

STATE_ABBR_MAP = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR", "California": "CA",
    "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE", "District of Columbia": "DC",
    "Washington, D.C.": "DC", "DC": "DC", "Florida": "FL", "Georgia": "GA", "Hawaii": "HI",
    "Idaho": "ID", "Illinois": "IL", "Indiana": "IN", "Iowa": "IA", "Kansas": "KS", "Kentucky": "KY",
    "Louisiana": "LA", "Maine": "ME", "Maryland": "MD", "Massachusetts": "MA", "Michigan": "MI",
    "Minnesota": "MN", "Mississippi": "MS", "Missouri": "MO", "Montana": "MT", "Nebraska": "NE",
    "Nevada": "NV", "New Hampshire": "NH", "New Jersey": "NJ", "New Mexico": "NM", "New York": "NY",
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


def normalize_state_display(search_label: str, scraped_state_abbr: str = "") -> str:
    if scraped_state_abbr:
        for state_name, abbr in STATE_ABBR_MAP.items():
            if abbr == scraped_state_abbr:
                return STATE_DISPLAY_MAP.get(state_name, state_name)
    return STATE_DISPLAY_MAP.get(search_label, search_label)


def normalize_state_abbr(search_label: str, scraped_state_abbr: str = "") -> str:
    if scraped_state_abbr:
        return scraped_state_abbr
    return STATE_ABBR_MAP.get(search_label, "")


def clean_program_label(program_text: str) -> str:
    if not program_text:
        return ""
    return program_text.replace("(Deemed)", "").strip()


def normalize_service_list(services_text: str) -> List[str]:
    if not services_text:
        return []
    return [s.strip() for s in services_text.split(",") if s.strip()]


def normalize_address_component(text: str) -> str:
    if not text:
        return ""

    replacements = [
        (r"\bSte\b\.?", "Suite"),
        (r"\bSTE\b\.?", "Suite"),
        (r"\bApt\b\.?", "Apartment"),
        (r"\bAPT\b\.?", "Apartment"),
        (r"\bFl\b\.?", "Floor"),
        (r"\bFL\b\.?", "Floor"),
        (r"\bBldg\b\.?", "Building"),
        (r"\bBLDG\b\.?", "Building"),
        (r"\bRm\b\.?", "Room"),
        (r"\bRM\b\.?", "Room"),
    ]

    cleaned = text.strip()
    for pattern, replacement in replacements:
        cleaned = re.sub(pattern, replacement, cleaned)

    cleaned = re.sub(r"\s+", " ", cleaned).strip(" ,")
    return cleaned


def split_address_lines(address_raw: str) -> Tuple[str, str]:
    if not address_raw:
        return "", ""

    normalized = normalize_address_component(address_raw)

    secondary_patterns = [
        r"\bSuite\s+[A-Za-z0-9\-]+\b",
        r"\bUnit\s+[A-Za-z0-9\-]+\b",
        r"\bApartment\s+[A-Za-z0-9\-]+\b",
        r"\bFloor\s+[A-Za-z0-9\-]+\b",
        r"\bBuilding\s+[A-Za-z0-9\-]+\b",
        r"\bRoom\s+[A-Za-z0-9\-]+\b",
        r"\b#\s*[A-Za-z0-9\-]+\b",
    ]

    for pattern in secondary_patterns:
        match = re.search(pattern, normalized, flags=re.IGNORECASE)
        if match:
            line_1 = normalized[:match.start()].strip(" ,")
            line_2 = normalized[match.start():].strip(" ,")
            return line_1, line_2

    return normalized, ""


def parse_dba_and_address_lines(lines: List[str]) -> Tuple[str, str, str]:
    if not lines:
        return "", "", ""

    legal_name = lines[0]
    dba_name = ""
    address_lines = []

    for line in lines[1:]:
        if re.match(r"^d/b/a\s+", line, flags=re.IGNORECASE):
            dba_name = re.sub(r"^d/b/a\s+", "", line, flags=re.IGNORECASE).strip()
        else:
            address_lines.append(line)

    joined_address = ", ".join(address_lines)
    return legal_name, dba_name, joined_address


def parse_details_text(details_text: str) -> dict:
    result = {
        "accreditation_dates": "",
        "accreditation_program": "",
        "services": ""
    }

    if not details_text:
        return result

    cleaned = re.sub(r"\s+", " ", details_text).strip()

    dates_match = re.search(r"Dates:\s*(.+?)(?:Program:|Services:|$)", cleaned, re.IGNORECASE)
    if dates_match:
        result["accreditation_dates"] = dates_match.group(1).strip()

    program_match = re.search(r"Program:\s*(.+?)(?:Services:|$)", cleaned, re.IGNORECASE)
    if program_match:
        result["accreditation_program"] = clean_program_label(program_match.group(1).strip())

    services_match = re.search(r"Services:\s*(.+)$", cleaned, re.IGNORECASE)
    if services_match:
        result["services"] = services_match.group(1).strip()

    return result


def build_location_key(address_line_1: str, city: str, state_abbr: str, zip_code: str) -> str:
    return "|".join([
        normalize_text(address_line_1),
        normalize_text(city),
        state_abbr.strip().upper(),
        zip_code.strip()
    ])


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
    await page.wait_for_timeout(1400)


async def expand_details_if_needed(row_locator, index: int):
    toggle_link = row_locator.locator("a:has-text('Show/Hide Accreditation Details'):visible").first
    if await toggle_link.count() == 0:
        return
    try:
        await toggle_link.click()
        await asyncio.sleep(0.18)
    except Exception as e:
        print(f"    Warning: could not expand details for row {index + 1}: {e}")


async def click_next_if_available(page) -> bool:
    next_link = page.locator("a:has-text('Next'):visible, a[title*='Next']:visible").first
    if await next_link.count() == 0:
        return False

    try:
        await next_link.click()
        await wait_for_results(page)
        return True
    except Exception:
        return False


async def scrape_current_page_rows(page, searched_program: str, searched_state_label: str) -> List[dict]:
    rows = []

    result_rows = page.locator("tr:visible").filter(
        has=page.locator("a:has-text('Show/Hide Accreditation Details'):visible")
    )

    row_count = await result_rows.count()
    print(f"  Visible result rows found: {row_count}")

    for i in range(row_count):
        row = result_rows.nth(i)

        await expand_details_if_needed(row, i)

        text = await row.inner_text()
        lines = [line.strip() for line in text.split("\n") if line.strip()]
        lines = [line for line in lines if "Show/Hide Accreditation Details" not in line]

        if len(lines) < 3:
            continue

        details_start = None
        for idx, line in enumerate(lines):
            if line.lower().startswith("accreditation details") or line.lower().startswith("dates:"):
                details_start = idx
                break

        listing_lines = lines if details_start is None else lines[:details_start]
        details_lines = [] if details_start is None else lines[details_start:]

        city_state_zip = ""
        listing_before_city = []

        for line in listing_lines[1:]:
            if re.search(r",\s*[A-Z]{2}\s+\d{5}(?:-\d{4})?$", line):
                city_state_zip = line
                break
            listing_before_city.append(line)

        if not city_state_zip:
            continue

        legal_name, dba_name, address_raw = parse_dba_and_address_lines([listing_lines[0]] + listing_before_city)
        address_line_1, address_line_2 = split_address_lines(address_raw)

        city, scraped_state_abbr, zipc = split_city_state_zip(city_state_zip)
        state_display = normalize_state_display(searched_state_label, scraped_state_abbr)
        state_abbr = normalize_state_abbr(searched_state_label, scraped_state_abbr)

        details_text = " ".join(details_lines)
        details = parse_details_text(details_text)

        accreditation_program = details["accreditation_program"] or searched_program
        display_name = dba_name if dba_name else legal_name
        location_key = build_location_key(address_line_1, city, state_abbr, zipc)

        rows.append({
            "location_key": location_key,
            "display_name": display_name,
            "legal_name": legal_name,
            "dba_name": dba_name,
            "searched_program_type": searched_program,
            "accreditation_program": accreditation_program,
            "services": details["services"],
            "address_raw": address_raw,
            "address_line_1": address_line_1,
            "address_line_2": address_line_2,
            "city": city,
            "state": state_display,
            "state_abbr": state_abbr,
            "zip": zipc,
            "phone": "",
            "website_url": "",
            "latitude": "",
            "longitude": "",
            "accreditation_dates": details["accreditation_dates"],
            "source_url": AMS_URL,
            "last_seen": datetime.utcnow().isoformat()
        })

    return rows


def deduplicate_raw_rows(rows: List[dict]) -> List[dict]:
    seen = set()
    unique = []

    for row in rows:
        key = (
            row["location_key"],
            row["searched_program_type"],
            row["accreditation_program"],
            row["services"]
        )
        if key not in seen:
            seen.add(key)
            unique.append(row)

    return unique


def build_merged_locations(raw_rows: List[dict]) -> List[dict]:
    grouped: Dict[str, Dict[str, Any]] = {}

    for row in raw_rows:
        key = row["location_key"]

        if key not in grouped:
            grouped[key] = {
                "location_key": key,
                "display_name": row["display_name"],
                "legal_name": row["legal_name"],
                "dba_names": set(),
                "name_variants": set(),
                "program_types": set(),
                "accreditation_programs": set(),
                "services": set(),
                "address_raw_variants": set(),
                "address_line_1": row["address_line_1"],
                "address_line_2": row["address_line_2"],
                "city": row["city"],
                "state": row["state"],
                "state_abbr": row["state_abbr"],
                "zip": row["zip"],
                "phone": row["phone"],
                "website_url": row["website_url"],
                "latitude": row["latitude"],
                "longitude": row["longitude"],
                "accreditation_dates": set(),
                "source_urls": set(),
                "source_count": 0,
                "last_seen": row["last_seen"],
                "enhanced_listing": False
            }

        g = grouped[key]
        g["source_count"] += 1
        g["name_variants"].add(row["legal_name"])
        g["program_types"].add(row["searched_program_type"])
        g["accreditation_programs"].add(row["accreditation_program"])
        g["source_urls"].add(row["source_url"])
        g["address_raw_variants"].add(row["address_raw"])

        if row["dba_name"]:
            g["dba_names"].add(row["dba_name"])

        for service in normalize_service_list(row["services"]):
            g["services"].add(service)

        if row["accreditation_dates"]:
            g["accreditation_dates"].add(row["accreditation_dates"])

        if row["dba_name"] and g["display_name"] == g["legal_name"]:
            g["display_name"] = row["dba_name"]

        if row["last_seen"] > g["last_seen"]:
            g["last_seen"] = row["last_seen"]

    merged_rows = []
    for g in grouped.values():
        merged_rows.append({
            "location_key": g["location_key"],
            "display_name": g["display_name"],
            "legal_name": g["legal_name"],
            "dba_names": " | ".join(sorted(g["dba_names"])),
            "name_variants": " | ".join(sorted(g["name_variants"])),
            "program_types": " | ".join(sorted(g["program_types"])),
            "accreditation_programs": " | ".join(sorted(g["accreditation_programs"])),
            "services": " | ".join(sorted(g["services"])),
            "address_raw_variants": " | ".join(sorted(g["address_raw_variants"])),
            "address_line_1": g["address_line_1"],
            "address_line_2": g["address_line_2"],
            "city": g["city"],
            "state": g["state"],
            "state_abbr": g["state_abbr"],
            "zip": g["zip"],
            "phone": g["phone"],
            "website_url": g["website_url"],
            "latitude": g["latitude"],
            "longitude": g["longitude"],
            "accreditation_dates": " | ".join(sorted(g["accreditation_dates"])),
            "source_urls": " | ".join(sorted(g["source_urls"])),
            "source_count": g["source_count"],
            "last_seen": g["last_seen"],
            "enhanced_listing": g["enhanced_listing"]
        })

    return merged_rows


async def scrape_program_state(page, program: str, state_label: str) -> List[dict]:
    print(f"Fetching: {program} / {state_label}")

    start = time.time()
    await page.goto(AMS_URL, timeout=60000)
    await page.wait_for_load_state("domcontentloaded")
    print(f"  goto took {time.time() - start:.2f}s")

    prog_select = await find_select_with_programs(page)
    state_select = await find_select_with_states(page)
    country_select, country_value, country_label = await find_country_select_and_value(page)

    if not prog_select or not state_select or not country_select or not country_value:
        raise Exception("Could not locate one or more dropdowns")

    await country_select.select_option(value=country_value)
    print(f"  Selected country option: {country_label}")

    await prog_select.select_option(label=program)
    await state_select.select_option(label=state_label)
    await polite_pause()

    start = time.time()
    await click_search(page)
    await wait_for_results(page)
    print(f"  search + wait took {time.time() - start:.2f}s")

    all_rows = []

    while True:
        page_rows = await scrape_current_page_rows(page, program, state_label)
        print(f"  Found {len(page_rows)} rows on current page")
        all_rows.extend(page_rows)

        if LIMIT_LOCATIONS > 0 and len(all_rows) >= LIMIT_LOCATIONS:
            all_rows = all_rows[:LIMIT_LOCATIONS]
            break

        moved = await click_next_if_available(page)
        if not moved:
            break

        await polite_pause()

    return all_rows


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

        async def worker(program_state_pairs: List[Tuple[str, str]]) -> List[dict]:
            local_rows = []
            page = await context.new_page()
            try:
                for program, state_label in program_state_pairs:
                    rows = await scrape_program_state(page, program, state_label)
                    local_rows.extend(rows)
            finally:
                await page.close()
            return local_rows

        pairs = [(program, state) for program in PROGRAMS for state in SEARCH_STATES]
        chunk_size = 8
        chunks = [pairs[i:i + chunk_size] for i in range(0, len(pairs), chunk_size)]

        tasks = [worker(chunk) for chunk in chunks[:4]]
        results = await asyncio.gather(*tasks)

        for result in results:
            all_rows.extend(result)

        await browser.close()

    return all_rows


async def write_to_google_sheets(raw_rows: List[dict], merged_rows: List[dict]):
    payload = {
        "action": "replace_all",
        "test_mode": TEST_MODE,
        "raw_rows": raw_rows,
        "merged_rows": merged_rows
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
    print("Starting ACHC scraper...")
    print(f"TEST_MODE: {TEST_MODE}")
    print(f"PROGRAMS: {PROGRAMS}")
    print(f"SEARCH_STATES: {SEARCH_STATES}")

    raw_rows = await run_scrape()

    if not raw_rows:
        raise Exception("No rows scraped")

    deduped_raw_rows = deduplicate_raw_rows(raw_rows)
    merged_rows = build_merged_locations(deduped_raw_rows)

    print(f"Scraped {len(raw_rows)} total raw rows")
    print(f"Deduped raw rows: {len(deduped_raw_rows)}")
    print(f"Merged locations: {len(merged_rows)}")

    await write_to_google_sheets(deduped_raw_rows, merged_rows)

    print("Scraper completed successfully")
    print("Raw + merged data written to Google Sheets")


if __name__ == "__main__":
    asyncio.run(main())
