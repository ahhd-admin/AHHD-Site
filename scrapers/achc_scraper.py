import asyncio
import os
import random
import re
import time
from datetime import datetime
from typing import List, Tuple

from dotenv import load_dotenv
from playwright.async_api import async_playwright

load_dotenv()

AMS_URL = "https://ams.achc.org/accredited_organizations.aspx"

# Fast debug mode
PROGRAMS = ["Home Health"]
SEARCH_STATES = ["Wyoming"]

LIMIT_LOCATIONS = int(os.getenv("LIMIT_LOCATIONS", "25"))

STATE_DISPLAY_MAP = {
    "District of Columbia": "Washington, D.C.",
    "Washington, D.C.": "Washington, D.C.",
    "DC": "Washington, D.C."
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

COUNTRY_PREFERRED_LABELS = [
    "USA",
    "United States",
    "United States of America",
    "U.S.A.",
    "U.S.",
    "US"
]


async def polite_pause(min_seconds: float = 0.08, max_seconds: float = 0.2):
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
        result["accreditation_program"] = program_match.group(1).strip()

    services_match = re.search(r"Services:\s*(.+)$", cleaned, re.IGNORECASE)
    if services_match:
        result["services"] = services_match.group(1).strip()

    return result


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
    await page.wait_for_timeout(1000)


async def expand_details_if_needed(container, index: int):
    toggle_link = container.locator("a:has-text('Show/Hide Accreditation Details')").first

    if await toggle_link.count() == 0:
        return

    try:
        await toggle_link.click()
        await asyncio.sleep(0.15)
    except Exception as e:
        print(f"    Warning: could not expand details for result {index + 1}: {e}")


async def scrape_current_page_rows(page, program: str, state_label: str) -> List[dict]:
    rows = []

    detail_links = page.locator("a:has-text('Show/Hide Accreditation Details')")
    link_count = await detail_links.count()

    print(f"  Detail links found: {link_count}")

    for i in range(link_count):
        link = detail_links.nth(i)
        container = link.locator("xpath=ancestor::tr[1]")

        # Expand inline details first
        await expand_details_if_needed(container, i)

        text = await container.inner_text()
        lines = [line.strip() for line in text.split("\n") if line.strip()]

        # Remove toggle link line
        lines = [line for line in lines if "Show/Hide Accreditation Details" not in line]

        if len(lines) < 3:
            continue

        # Basic structure:
        # Company Name
        # optional d/b/a line
        # address line(s)
        # City, ST ZIP
        # optional Accreditation Details
        # Dates: ...
        # Program: ...
        # Services: ...
        name = lines[0]

        address_lines = []
        city_state_zip = ""
        details_lines = []

        found_city_state_zip = False
        for line in lines[1:]:
            if not found_city_state_zip and re.search(r",\s*[A-Z]{2}\s+\d{5}(?:-\d{4})?$", line):
                city_state_zip = line
                found_city_state_zip = True
                continue

            if not found_city_state_zip:
                address_lines.append(line)
            else:
                details_lines.append(line)

        if not city_state_zip or not address_lines:
            continue

        address = ", ".join(address_lines)
        city, scraped_state_abbr, zipc = split_city_state_zip(city_state_zip)

        details_text = " ".join(details_lines)
        details = parse_details_text(details_text)

        rows.append({
            "organization": name,
            "program": program,
            "address": address,
            "city": city,
            "state": normalize_state_display(state_label, scraped_state_abbr),
            "state_abbr": normalize_state_abbr(state_label, scraped_state_abbr),
            "zip": zipc,
            "phone": "",
            "services": details["services"],
            "accreditation_program": details["accreditation_program"],
            "accreditation_dates": details["accreditation_dates"],
            "latitude": "",
            "longitude": "",
            "source_url": AMS_URL,
            "last_seen": datetime.utcnow().isoformat()
        })

        if LIMIT_LOCATIONS > 0 and len(rows) >= LIMIT_LOCATIONS:
            break

    return rows


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

                start = time.time()
                page_rows = await scrape_current_page_rows(page, program, state_label)
                print(f"  parse took {time.time() - start:.2f}s")
                print(f"  Found {len(page_rows)} rows on first page")

                all_rows.extend(page_rows)

        await browser.close()

    return all_rows


async def main():
    print("Starting ACHC scraper...")
    rows = await run_scrape()

    if not rows:
        raise Exception("No rows scraped")

    unique_rows = deduplicate_rows(rows)
    print(f"Scraped {len(rows)} total rows, {len(unique_rows)} unique")

    print("Sample rows:")
    for row in unique_rows[:5]:
        print(row)

    for row in unique_rows[:5]:
        print(row["organization"], "-", row["city"], row["state"])

    print("Test scrape completed successfully")


if __name__ == "__main__":
    asyncio.run(main())
