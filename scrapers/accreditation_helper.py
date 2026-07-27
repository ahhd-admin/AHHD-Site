import asyncio
import random
import re
from typing import Dict, List

import aiohttp

try:
    from bs4 import BeautifulSoup
    _BS4_AVAILABLE = True
except ImportError:
    _BS4_AVAILABLE = False

CANONICAL_PROGRAMS = [
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
    "Private Duty",
    "Healthcare Staffing Services Certification",
    "PCAB Compounding Pharmacy",
    "Long-Term Care Dialysis Certification",
    "Telehealth Certification",
    "ACHC Inspection Services",
]

_CANONICAL_LOWER = {p.lower(): p for p in CANONICAL_PROGRAMS}

_AJAX_URL = "https://ams.achc.org/accredited_organizations.aspx/GetAccreditedServicesListByCompanyID"
_HEADERS = {
    "Content-Type": "application/json; charset=utf-8",
    "X-Requested-With": "XMLHttpRequest",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122 Safari/537.36",
}


def parse_accreditation_html(html: str) -> List[str]:
    if not html:
        return []

    if _BS4_AVAILABLE:
        soup = BeautifulSoup(html, "html.parser")
        candidates = []
        for tag in soup.find_all(["b", "strong", "h3", "h4"]):
            text = tag.get_text(separator=" ", strip=True)
            if text:
                candidates.append(text)
    else:
        candidates = re.findall(
            r"<(?:b|strong|h[34])[^>]*>([^<]+)</(?:b|strong|h[34])>",
            html,
            flags=re.IGNORECASE,
        )

    found = set()
    for candidate in candidates:
        candidate_lower = candidate.lower()
        for key, canonical in _CANONICAL_LOWER.items():
            if key in candidate_lower:
                found.add(canonical)

    return sorted(found)


async def _fetch_one(
    session: aiohttp.ClientSession,
    company_id: int,
    delay_range: tuple,
    max_retries: int,
) -> List[str]:
    body = {"lngCompanyID": company_id, "boolFullDescription": True}

    for attempt in range(max_retries + 1):
        try:
            async with session.post(_AJAX_URL, json=body, headers=_HEADERS, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status != 200:
                    raise ValueError(f"HTTP {resp.status}")
                data = await resp.json(content_type=None)
                html = data.get("d", "") or ""
                return parse_accreditation_html(html)
        except Exception as exc:
            if attempt < max_retries:
                await asyncio.sleep(1.0)
            else:
                print(f"  WARNING: accreditation fetch failed for company_id={company_id}: {exc}")
                return []

    return []


async def fetch_accreditation_services(
    company_ids: List,
    base_url: str = _AJAX_URL,
    delay_range: tuple = (0.15, 0.35),
    max_retries: int = 2,
    concurrency: int = 10,
) -> Dict[str, List[str]]:
    int_ids = []
    for cid in company_ids:
        try:
            int_ids.append(int(cid))
        except (TypeError, ValueError):
            print(f"  WARNING: skipping invalid company_id: {cid!r}")

    print(f"Fetching accreditation details for {len(int_ids)} providers via AJAX (concurrency={concurrency})...")
    results: Dict[str, List[str]] = {}
    sem = asyncio.Semaphore(concurrency)
    completed = 0

    async def _fetch_with_delay(session: aiohttp.ClientSession, cid: int) -> None:
        nonlocal completed
        async with sem:
            services = await _fetch_one(session, cid, delay_range, max_retries)
            await asyncio.sleep(random.uniform(*delay_range))
        results[str(cid)] = services
        completed += 1
        if completed % 500 == 0:
            print(f"  ...{completed}/{len(int_ids)} fetched")

    async with aiohttp.ClientSession() as session:
        await asyncio.gather(*[_fetch_with_delay(session, cid) for cid in int_ids])

    print(f"Accreditation fetch complete: {len(results)} results")
    return results


def merge_ajax_services(
    normalized_rows: List[dict],
    ajax_results: Dict[str, List[str]],
) -> List[dict]:
    for row in normalized_rows:
        cid = str(row.get("achc_company_id", "") or "")
        if not cid:
            continue
        ajax_services = ajax_results.get(cid)
        if ajax_services:
            row["services"] = ajax_services
    return normalized_rows
