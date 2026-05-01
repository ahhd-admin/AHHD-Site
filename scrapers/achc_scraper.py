import asyncio
import json
import os
import random
import re
import time
from datetime import datetime
from typing import Dict, List, Set, Tuple

import aiohttp
from dotenv import load_dotenv
from geocode_helper import geocode_locations
from playwright.async_api import async_playwright

load_dotenv()

AMS_URL = "https://ams.achc.org/accredited_organizations.aspx"

DEFAULT_PROGRAMS = [
    "Home Care",
    "Home Health",
    "Hospice",
    "Ambulatory Care",
    "Assisted Living",
    "Behavioral Health",
    "Dentistry",
    "Home Infusion Therapy",
    "Palliative Care",
    "Renal Dialysis",
    "Sleep",
    "Community Retail",
    "DMEPOS",
    "Pharmacy",
]

DEFAULT_TRIGGER_STATE = "Texas"

# 0 means unlimited / full pull
LIMIT_LOCATIONS = int(os.getenv("LIMIT_LOCATIONS", "0"))

GOOGLE_SHEETS_URL = os.getenv("GOOGLE_SHEETS_WEB_APP_URL")
SCRAPER_SECRET = os.getenv("SCRAPER_SECRET", "")
TEST_MODE = os.getenv("TEST_MODE", "false").lower() == "true"
TEST_PROGRAMS_ENV = os.getenv("TEST_PROGRAMS", "").strip()
TRIGGER_STATE = os.getenv("TRIGGER_STATE", DEFAULT_TRIGGER_STATE).strip()

# True = do not select a state at all
NO_STATE_FILTER = os.getenv("NO_STATE_FILTER", "true").lower() == "true"

ENABLE_COVERAGE_DIAGNOSTIC = os.getenv("ENABLE_COVERAGE_DIAGNOSTIC", "true").lower() == "true"
COVERAGE_JSON_PATH = os.getenv("COVERAGE_JSON_PATH", "coverage_summary.json").strip()

ENABLE_ZERO_RESULT_DEBUG = os.getenv("ENABLE_ZERO_RESULT_DEBUG", "true").lower() == "true"
ZERO_RESULT_DEBUG_DIR = os.getenv("ZERO_RESULT_DEBUG_DIR", "debug_artifacts").strip()
ZERO_RESULT_DEBUG_TARGETS_ENV = os.getenv("ZERO_RESULT_DEBUG_TARGETS", "Home Health,Community Retail").strip()
ZERO_RESULT_DEBUG_TARGETS = [p.strip() for p in ZERO_RESULT_DEBUG_TARGETS_ENV.split(",") if p.strip()]

PROGRAM_SELECTION_WAIT_MS = int(os.getenv("PROGRAM_SELECTION_WAIT_MS", "2500"))
STATE_SELECTION_WAIT_MS = int(os.getenv("STATE_SELECTION_WAIT_MS", "1500"))
PRE_SEARCH_WAIT_MS = int(os.getenv("PRE_SEARCH_WAIT_MS", "2500"))
POST_SEARCH_WAIT_MS = int(os.getenv("POST_SEARCH_WAIT_MS", "5000"))

PROGRAMS = (
    [p.strip() for p in TEST_PROGRAMS_ENV.split(",") if p.strip()]
    if TEST_PROGRAMS_ENV
    else DEFAULT_PROGRAMS
)

if not GOOGLE_SHEETS_URL:
    raise ValueError("Missing GOOGLE_SHEETS_WEB_APP_URL in environment variables")

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
    "Wyoming": "WY",
}

COUNTRY_PREFERRED_LABELS = [
    "USA",
    "United States",
    "United States of America",
    "U.S.A.",
    "U.S.",
    "US",
]

EXPECTED_PROGRAMS: Set[str] = set(DEFAULT_PROGRAMS)

CANONICAL_PROGRAM_MAP: Dict[str, str] = {
    "home care": "Home Care",
    "home health": "Home Health",
    "hospice": "Hospice",
    "ambulatory care": "Ambulatory Care",
    "assisted living": "Assisted Living",
    "behavioral health": "Behavioral Health",
    "dentistry": "Dentistry",
    "home infusion therapy": "Home Infusion Therapy",
    "palliative care": "Palliative Care",
    "renal dialysis": "Renal Dialysis",
    "sleep": "Sleep",
    "community retail": "Community Retail",
    "dmepos": "DMEPOS",
    "pharmacy": "Pharmacy",
}


async def polite_pause(min_seconds: float = 0.12, max_seconds: float = 0.28):
    await asyncio.sleep(random.uniform(min_seconds, max_seconds))


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip().lower())


def safe_slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.strip().lower()).strip("-")


def get_result_scope(trigger_state: str, no_state_filter: bool) -> str:
    if no_state_filter:
        return "National raw ACHC pull; no state selected"
    return f"State-filtered ACHC pull; state = {trigger_state}"


def split_city_state_zip(text: str) -> Tuple[str, str, str]:
    if not text:
        return "", "", ""

    match = re.match(r"^(.*?),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)$", text.strip())
    if not match:
        return "", "", ""

    return match.group(1), match.group(2), match.group(3)


def parse_raw_block(raw_text: str) -> Tuple[str, str, str]:
    if not raw_text:
        return "", "", ""

    lines = [line.strip() for line in raw_text.split("\n") if line.strip()]
    cleaned_lines = []

    for line in lines:
        cleaned = line.replace("Show/Hide Accreditation Details", "").strip()
        cleaned = re.sub(r"\s+", " ", cleaned).strip(" \t")
        if cleaned:
            cleaned_lines.append(cleaned)

    if not cleaned_lines:
        return "", "", ""

    raw_name_line = cleaned_lines[0]
    city_state_zip = ""
    address_lines = []

    for line in cleaned_lines[1:]:
        if re.search(r",\s*[A-Z]{2}\s+\d{5}(?:-\d{4})?$", line):
            city_state_zip = line
            break
        address_lines.append(line)

    raw_address_block = " | ".join(address_lines)
    _, parsed_state_abbr, _ = split_city_state_zip(city_state_zip)
    return raw_name_line, raw_address_block, parsed_state_abbr


def detect_program_mentions(raw_text: str) -> List[str]:
    text_norm = normalize_text(raw_text)
    found = []

    for key, canonical in CANONICAL_PROGRAM_MAP.items():
        if key in text_norm:
            found.append(canonical)

    return sorted(set(found))


def summarize_unmapped_mentions(rows: List[dict]) -> List[str]:
    candidates = set()

    patterns = [
        r"Accreditation(?:\s+Commission)?[:\s]+([A-Za-z0-9&,\-/ ]{3,80})",
        r"Program(?:\s+Type)?[:\s]+([A-Za-z0-9&,\-/ ]{3,80})",
    ]

    for row in rows:
        raw_text = row.get("raw_text", "") or ""
        for pattern in patterns:
            for match in re.findall(pattern, raw_text, flags=re.IGNORECASE):
                candidate = re.sub(r"\s+", " ", match).strip(" .,:;|-")
                if not candidate:
                    continue

                candidate_norm = normalize_text(candidate)
                if candidate_norm not in CANONICAL_PROGRAM_MAP:
                    candidates.add(candidate)

    return sorted(candidates)


def normalize_row(row: dict) -> dict:
    raw_text = row.get("raw_text", "") or ""
    raw_name_line = row.get("raw_name_line", "") or ""
    raw_address_block = row.get("raw_address_block", "") or ""
    parsed_state_abbr = row.get("parsed_state_abbr", "") or ""

    # Extract city and ZIP from raw_text (find "City, ST ZIP" line)
    city, zip_code = "", ""
    city_zip_match = re.search(
        r'(.+),\s*[A-Z]{2}\s+(\d{5}(?:-\d{4})?)',
        raw_text,
        re.MULTILINE,
    )
    if city_zip_match:
        city_candidate, zip_code = city_zip_match.group(1).strip(), city_zip_match.group(2).strip()
        # Validate: shouldn't be a full address line (no digits at start)
        if not re.match(r'^\d', city_candidate):
            city = city_candidate

    # Extract phone from raw_text
    phone_match = re.search(r'\(?\d{3}\)?[\s.\-]\d{3}[\s.\-]\d{4}', raw_text)
    phone = phone_match.group(0).strip() if phone_match else ""

    # Services as list
    detected = row.get("detected_program_mentions", "") or ""
    services = [s.strip() for s in detected.split(",") if s.strip()]

    provider_name = raw_name_line.strip()
    provider_key = safe_slug(provider_name) + "_" + parsed_state_abbr if parsed_state_abbr else safe_slug(provider_name)

    return {
        "provider_key": provider_key,
        "provider_name": provider_name,
        "street_address": raw_address_block,
        "city": city,
        "state": parsed_state_abbr,
        "zip": zip_code,
        "phone": phone,
        "services": services,
        "accrediting_body": "ACHC",
        "source_url": row.get("source_url", ""),
        "last_seen": row.get("last_seen", ""),
        "confidence_status": "verified",
        "searched_program_type": row.get("searched_program_type", ""),
        "result_scope": row.get("result_scope", ""),
    }


def normalize_rows(raw_rows: List[dict]) -> List[dict]:
    return [normalize_row(r) for r in raw_rows]


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


async def get_selected_option_info(select_locator) -> Dict[str, str]:
    value = (await select_locator.input_value()) or ""
    selected_option = select_locator.locator("option:checked")
    label = ""
    if await selected_option.count():
        label = (await selected_option.first.inner_text()).strip()

    return {
        "value": value,
        "label": label,
    }


async def click_search(page):
    selectors = [
        "input[value='Find']",
        "input[value='Search']",
        "button:has-text('Find')",
        "button:has-text('Search')",
    ]
    for sel in selectors:
        if await page.locator(sel).count():
            await page.locator(sel).first.click()
            return

    await page.keyboard.press("Enter")


async def wait_for_results(page):
    await page.wait_for_timeout(POST_SEARCH_WAIT_MS)


async def write_zero_result_debug_artifacts(
    page,
    program: str,
    trigger_state: str,
    no_state_filter: bool,
    selected_program_before_search: Dict[str, str],
    selected_state_before_search: Dict[str, str],
    detail_links_found: int,
):
    if not ENABLE_ZERO_RESULT_DEBUG:
        return

    os.makedirs(ZERO_RESULT_DEBUG_DIR, exist_ok=True)

    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    program_slug = safe_slug(program)
    state_slug = "no-state" if no_state_filter else safe_slug(trigger_state)

    screenshot_path = os.path.join(
        ZERO_RESULT_DEBUG_DIR,
        f"{timestamp}_{program_slug}_{state_slug}_zero_results.png",
    )
    html_path = os.path.join(
        ZERO_RESULT_DEBUG_DIR,
        f"{timestamp}_{program_slug}_{state_slug}_zero_results.html",
    )
    meta_path = os.path.join(
        ZERO_RESULT_DEBUG_DIR,
        f"{timestamp}_{program_slug}_{state_slug}_zero_results.json",
    )

    await page.screenshot(path=screenshot_path, full_page=True)

    html = await page.content()
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)

    meta = {
        "generated_at_utc": datetime.utcnow().isoformat(),
        "program_requested": program,
        "no_state_filter": no_state_filter,
        "trigger_state_requested": "" if no_state_filter else trigger_state,
        "result_scope": get_result_scope(trigger_state, no_state_filter),
        "selected_program_before_search": selected_program_before_search,
        "selected_state_before_search": selected_state_before_search,
        "detail_links_found": detail_links_found,
        "page_title": await page.title(),
        "page_url": page.url,
    }

    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)

    print(f" Zero-result screenshot written to: {screenshot_path}")
    print(f" Zero-result HTML written to: {html_path}")
    print(f" Zero-result metadata written to: {meta_path}")


async def scrape_raw_rows(page, searched_program: str, trigger_state: str, no_state_filter: bool) -> Tuple[List[dict], int]:
    rows = []
    detail_links = page.locator("a:has-text('Show/Hide Accreditation Details')")
    link_count = await detail_links.count()
    print(f" Detail links found in DOM for {searched_program}: {link_count}")

    trigger_state_abbr = STATE_ABBR_MAP.get(trigger_state, "")

    for i in range(link_count):
        link = detail_links.nth(i)

        candidate_locators = [
            link.locator("xpath=ancestor::tr[1]"),
            link.locator("xpath=ancestor::div[1]"),
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

        cleaned_raw_text = raw_text.replace("Show/Hide Accreditation Details", "").strip()
        raw_name_line, raw_address_block, parsed_state_abbr = parse_raw_block(cleaned_raw_text)

        matches_trigger_state = False
        if not no_state_filter and parsed_state_abbr and trigger_state_abbr:
            matches_trigger_state = parsed_state_abbr == trigger_state_abbr

        detected_program_mentions = detect_program_mentions(cleaned_raw_text)

        rows.append(
            {
                "raw_index": i + 1,
                "container_type": container_type,
                "searched_program_type": searched_program,
                "search_trigger_state": "" if no_state_filter else trigger_state,
                "result_scope": get_result_scope(trigger_state, no_state_filter),
                "raw_name_line": raw_name_line,
                "raw_address_block": raw_address_block,
                "parsed_state_abbr": parsed_state_abbr,
                "matches_trigger_state": matches_trigger_state,
                "detected_program_mentions": ", ".join(detected_program_mentions),
                "raw_text": cleaned_raw_text,
                "source_url": AMS_URL,
                "last_seen": datetime.utcnow().isoformat(),
            }
        )

        if LIMIT_LOCATIONS > 0 and len(rows) >= LIMIT_LOCATIONS:
            break

    return rows, link_count


async def select_dropdown_with_verification(select_locator, label: str, wait_ms: int, select_name: str) -> Dict[str, Dict[str, str]]:
    before = await get_selected_option_info(select_locator)
    await select_locator.select_option(label=label)
    await asyncio.sleep(wait_ms / 1000)
    after = await get_selected_option_info(select_locator)

    print(
        f" {select_name} selection requested='{label}' | "
        f"before_label='{before['label']}' before_value='{before['value']}' | "
        f"after_label='{after['label']}' after_value='{after['value']}'"
    )

    return {
        "before": before,
        "after": after,
    }


async def scrape_program(page, program: str, trigger_state: str, no_state_filter: bool) -> Tuple[List[dict], dict]:
    mode_label = "no state selected" if no_state_filter else f"trigger state {trigger_state}"
    print(f"Fetching: {program} / {mode_label}")

    await page.goto(AMS_URL, timeout=60000)
    await page.wait_for_load_state("domcontentloaded")
    await page.wait_for_timeout(1500)

    prog_select = await find_select_with_programs(page)
    state_select = await find_select_with_states(page)
    country_select, country_value, country_label = await find_country_select_and_value(page)

    if not prog_select or not state_select or not country_select or not country_value:
        raise Exception("Could not locate one or more dropdowns")

    await country_select.select_option(value=country_value)
    await page.wait_for_timeout(1000)
    print(f" Selected country option: {country_label}")

    program_selection_info = await select_dropdown_with_verification(
        prog_select,
        program,
        PROGRAM_SELECTION_WAIT_MS,
        "Program",
    )

    state_selection_info = {
        "before": {"value": "", "label": ""},
        "after": {"value": "", "label": ""},
    }

    if no_state_filter:
        print(" No state selected for this run")
    else:
        state_selection_info = await select_dropdown_with_verification(
            state_select,
            trigger_state,
            STATE_SELECTION_WAIT_MS,
            "State",
        )

    selected_program_before_search = await get_selected_option_info(prog_select)
    selected_state_before_search = await get_selected_option_info(state_select)

    print(
        f" Final selections before search | "
        f"program_label='{selected_program_before_search['label']}' "
        f"program_value='{selected_program_before_search['value']}' | "
        f"state_label='{selected_state_before_search['label']}' "
        f"state_value='{selected_state_before_search['value']}'"
    )

    await page.wait_for_timeout(PRE_SEARCH_WAIT_MS)
    await polite_pause()
    await click_search(page)
    await wait_for_results(page)

    rows, detail_link_count = await scrape_raw_rows(page, program, trigger_state, no_state_filter)

    if detail_link_count == 0 and program in ZERO_RESULT_DEBUG_TARGETS:
        await write_zero_result_debug_artifacts(
            page=page,
            program=program,
            trigger_state=trigger_state,
            no_state_filter=no_state_filter,
            selected_program_before_search=selected_program_before_search,
            selected_state_before_search=selected_state_before_search,
            detail_links_found=detail_link_count,
        )

    unique_states = sorted({r["parsed_state_abbr"] for r in rows if r.get("parsed_state_abbr")})
    sample_names = [r["raw_name_line"] for r in rows[:5] if r.get("raw_name_line")]
    detected_mentions = sorted(
        {
            mention.strip()
            for row in rows
            for mention in (row.get("detected_program_mentions", "") or "").split(",")
            if mention.strip()
        }
    )

    coverage = {
        "program_requested": program,
        "requested_program_unmapped": program not in EXPECTED_PROGRAMS,
        "no_state_filter": no_state_filter,
        "result_scope": get_result_scope(trigger_state, no_state_filter),
        "program_selection_before": program_selection_info["before"],
        "program_selection_after": program_selection_info["after"],
        "state_selection_before": state_selection_info["before"],
        "state_selection_after": state_selection_info["after"],
        "selected_program_before_search": selected_program_before_search,
        "selected_state_before_search": selected_state_before_search,
        "detail_links_found": detail_link_count,
        "rows_parsed": len(rows),
        "unique_parsed_states_count": len(unique_states),
        "unique_parsed_states_sample": unique_states[:15],
        "sample_names": sample_names,
        "detected_program_mentions": detected_mentions,
    }

    print(f" Found {len(rows)} raw rows for {program}")
    print(f" Coverage summary for {program}: {json.dumps(coverage, indent=2)}")

    return rows, coverage


async def run_scrape() -> Tuple[List[dict], List[dict]]:
    all_rows = []
    coverage_summary = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--disable-dev-shm-usage", "--no-sandbox"],
        )

        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122 Safari/537.36"
        )
        page = await context.new_page()

        for program in PROGRAMS:
            rows, coverage = await scrape_program(page, program, TRIGGER_STATE, NO_STATE_FILTER)
            all_rows.extend(rows)
            coverage_summary.append(coverage)

            print(f"Total raw rows so far: {len(all_rows)}")

            if LIMIT_LOCATIONS > 0 and len(all_rows) >= LIMIT_LOCATIONS:
                all_rows = all_rows[:LIMIT_LOCATIONS]
                print(f"Global LIMIT_LOCATIONS reached: {LIMIT_LOCATIONS}")
                break

        await browser.close()

    return all_rows, coverage_summary


def print_coverage_report(coverage_summary: List[dict], all_rows: List[dict]):
    print("\n" + "=" * 80)
    print("PROGRAM COVERAGE DIAGNOSTIC REPORT")
    print("=" * 80)

    missing_programs = [c["program_requested"] for c in coverage_summary if c["rows_parsed"] == 0]
    requested_unmapped = [c["program_requested"] for c in coverage_summary if c["requested_program_unmapped"]]
    discovered_unmapped = summarize_unmapped_mentions(all_rows)

    for item in coverage_summary:
        print(json.dumps(item, indent=2))

    print("-" * 80)
    print(f"Programs requested: {PROGRAMS}")
    print(f"NO_STATE_FILTER: {NO_STATE_FILTER}")
    print(f"Programs with zero parsed rows: {missing_programs}")
    print(f"Requested program labels not in canonical list: {requested_unmapped}")
    print(f"Possible unmapped discovered labels: {discovered_unmapped}")
    print(f"Total raw rows captured: {len(all_rows)}")
    print("=" * 80)

    if ENABLE_COVERAGE_DIAGNOSTIC:
        payload = {
            "generated_at_utc": datetime.utcnow().isoformat(),
            "no_state_filter": NO_STATE_FILTER,
            "trigger_state": "" if NO_STATE_FILTER else TRIGGER_STATE,
            "limit_locations": LIMIT_LOCATIONS,
            "programs_requested": PROGRAMS,
            "programs_with_zero_rows": missing_programs,
            "requested_program_labels_unmapped": requested_unmapped,
            "possible_unmapped_discovered_labels": discovered_unmapped,
            "coverage_summary": coverage_summary,
            "total_raw_rows": len(all_rows),
        }

        with open(COVERAGE_JSON_PATH, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)

        print(f"Coverage JSON written to: {COVERAGE_JSON_PATH}")


async def write_to_google_sheets(raw_rows: List[dict], normalized_rows: List[dict], run_metadata: dict):
    payload = {
        "action": "replace_raw_only",
        "test_mode": TEST_MODE,
        "secret": SCRAPER_SECRET,
        "raw_rows": raw_rows,
        "normalized_rows": normalized_rows,
        "run_metadata": run_metadata,
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(
            GOOGLE_SHEETS_URL,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=aiohttp.ClientTimeout(total=180),
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
    print(f"NO_STATE_FILTER: {NO_STATE_FILTER}")
    print(f"LIMIT_LOCATIONS: {LIMIT_LOCATIONS}")
    print(f"ENABLE_COVERAGE_DIAGNOSTIC: {ENABLE_COVERAGE_DIAGNOSTIC}")
    print(f"COVERAGE_JSON_PATH: {COVERAGE_JSON_PATH}")
    print(f"ENABLE_ZERO_RESULT_DEBUG: {ENABLE_ZERO_RESULT_DEBUG}")
    print(f"ZERO_RESULT_DEBUG_DIR: {ZERO_RESULT_DEBUG_DIR}")
    print(f"ZERO_RESULT_DEBUG_TARGETS: {ZERO_RESULT_DEBUG_TARGETS}")
    print(f"PROGRAM_SELECTION_WAIT_MS: {PROGRAM_SELECTION_WAIT_MS}")
    print(f"STATE_SELECTION_WAIT_MS: {STATE_SELECTION_WAIT_MS}")
    print(f"PRE_SEARCH_WAIT_MS: {PRE_SEARCH_WAIT_MS}")
    print(f"POST_SEARCH_WAIT_MS: {POST_SEARCH_WAIT_MS}")

    scrape_start_time = time.time()
    scrape_start_dt = datetime.utcnow()

    all_rows, coverage_summary = await run_scrape()

    scrape_end_time = time.time()
    scrape_end_dt = datetime.utcnow()
    duration_seconds = int(scrape_end_time - scrape_start_time)
    duration_minutes = duration_seconds // 60
    duration_remaining_seconds = duration_seconds % 60
    duration_human = f"{duration_minutes} minutes {duration_remaining_seconds} seconds"

    programs_with_zero_rows = [c["program_requested"] for c in coverage_summary if c["rows_parsed"] == 0]
    scrape_status = "incomplete_scrape" if len(programs_with_zero_rows) > 2 else "complete"

    if scrape_status == "incomplete_scrape":
        print(f"WARNING: {len(programs_with_zero_rows)} programs returned 0 rows — marking as incomplete_scrape")

    run_metadata = {
        "run_id": f"run_{scrape_start_dt.strftime('%Y%m%dT%H%M%SZ')}",
        "started_at_utc": scrape_start_dt.isoformat(),
        "completed_at_utc": scrape_end_dt.isoformat(),
        "duration_seconds": duration_seconds,
        "duration_human": duration_human,
        "total_rows_captured": len(all_rows),
        "programs_scraped": PROGRAMS,
        "programs_with_zero_rows": programs_with_zero_rows,
        "limit_locations": LIMIT_LOCATIONS,
        "no_state_filter": NO_STATE_FILTER,
        "trigger_state": "" if NO_STATE_FILTER else TRIGGER_STATE,
        "test_mode": TEST_MODE,
        "scrape_status": scrape_status,
    }

    print(f"Scrape duration: {duration_human}")
    print(f"Scrape status: {scrape_status}")

    if not all_rows and not TEST_MODE:
        raise Exception("No rows scraped")

    print(f"Raw rows captured: {len(all_rows)}")
    print("Sample raw rows:")
    for row in all_rows[:3]:
        print(
            {
                "raw_index": row["raw_index"],
                "searched_program_type": row["searched_program_type"],
                "search_trigger_state": row["search_trigger_state"],
                "parsed_state_abbr": row["parsed_state_abbr"],
                "matches_trigger_state": row["matches_trigger_state"],
                "detected_program_mentions": row["detected_program_mentions"],
                "raw_name_line": row["raw_name_line"],
                "result_scope": row["result_scope"],
            }
        )

    print_coverage_report(coverage_summary, all_rows)

    normalized = normalize_rows(all_rows)
    print(f"Normalized rows: {len(normalized)}")

    normalized = await geocode_locations(normalized)
    print(f"Geocoding complete")

    await write_to_google_sheets(all_rows, normalized, run_metadata)
    print("Raw and normalized data written to Google Sheets")
    print(f"Run complete: {run_metadata['run_id']} — {duration_human} — {len(all_rows)} rows — status: {scrape_status}")


if __name__ == "__main__":
    asyncio.run(main())
