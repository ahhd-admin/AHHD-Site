"""
Diagnostic script: explores the DOM structure of ACHC listing containers.

Run from the scrapers/ directory:
    python explore_listing_structure.py

Outputs structure_dump.json and screenshots for each pass.
"""
import asyncio
import json
from playwright.async_api import async_playwright

AMS_URL = "https://ams.achc.org/accredited_organizations.aspx"
MAX_PER_PASS = 10


async def find_select_with_programs(page):
    selects = page.locator("select")
    count = await selects.count()
    for i in range(count):
        sel = selects.nth(i)
        options = await sel.evaluate("el => Array.from(el.options).map(o => o.text)")
        if any("Home Care" in o or "Home Health" in o or "Hospice" in o for o in options):
            return sel
    return None


async def find_select_with_states(page):
    selects = page.locator("select")
    count = await selects.count()
    for i in range(count):
        sel = selects.nth(i)
        options = await sel.evaluate("el => Array.from(el.options).map(o => o.text)")
        if any("Texas" in o or "California" in o for o in options):
            return sel
    return None


async def find_country_select(page):
    selects = page.locator("select")
    count = await selects.count()
    for i in range(count):
        sel = selects.nth(i)
        options = await sel.evaluate("el => Array.from(el.options).map(o => o.text)")
        if any("United States" in o for o in options):
            value = await sel.evaluate(
                "el => Array.from(el.options).find(o => o.text.includes('United States'))?.value"
            )
            return sel, value
    return None, None


async def inspect_listings(page, label: str, limit: int = MAX_PER_PASS) -> dict:
    detail_links = page.locator("a:has-text('Show/Hide Accreditation Details')")
    total = await detail_links.count()
    print(f"  [{label}] Total links in DOM: {total}")

    results = []
    visible_count = 0
    hidden_count = 0
    display_none_count = 0

    for i in range(min(total, limit)):
        link = detail_links.nth(i)
        entry = {"index": i + 1}

        try:
            container = None
            container_tag = None
            for tag, xpath in [("tr", "xpath=ancestor::tr[1]"), ("div", "xpath=ancestor::div[1]")]:
                candidate = link.locator(xpath)
                try:
                    text = await candidate.inner_text()
                    if text and "Show/Hide" in text:
                        container = candidate
                        container_tag = tag
                        break
                except Exception:
                    continue

            entry["container_tag"] = container_tag

            if container is None:
                entry["error"] = "no container found"
                results.append(entry)
                continue

            entry["is_visible"] = await container.is_visible()
            entry["bounding_box"] = await container.bounding_box()
            entry["inner_text"] = (await container.inner_text()).strip()[:800]
            entry["inner_html"] = (await container.inner_html())[:2000]
            entry["computed_display"] = await container.evaluate(
                "el => window.getComputedStyle(el).display"
            )
            entry["computed_visibility"] = await container.evaluate(
                "el => window.getComputedStyle(el).visibility"
            )

            if entry["is_visible"]:
                visible_count += 1
            else:
                hidden_count += 1

            if entry["computed_display"] == "none":
                display_none_count += 1

        except Exception as e:
            entry["error"] = str(e)

        results.append(entry)

    summary = {
        "label": label,
        "total_links_in_dom": total,
        "inspected": len(results),
        "visible": visible_count,
        "hidden": hidden_count,
        "display_none": display_none_count,
        "listings": results,
    }
    print(f"  [{label}] Visible: {visible_count} | Hidden: {hidden_count} | display:none: {display_none_count}")
    return summary


async def main():
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        page = await browser.new_page()

        print("Navigating to ACHC...")
        await page.goto(AMS_URL, timeout=60000)
        await page.wait_for_load_state("domcontentloaded")
        await page.wait_for_timeout(2000)

        country_select, country_value = await find_country_select(page)
        if country_select and country_value:
            await country_select.select_option(value=country_value)
            await page.wait_for_timeout(1000)
            print("Selected: United States")

        prog_select = await find_select_with_programs(page)
        if not prog_select:
            print("ERROR: Could not find program dropdown")
            await browser.close()
            return

        await prog_select.select_option(label="Home Care")
        await page.wait_for_timeout(3000)
        await page.keyboard.press("Enter")
        await page.wait_for_timeout(5000)
        print("Selected program: Home Care, ran search")

        await page.screenshot(path="structure_dump_pass1.png", full_page=True)
        pass1 = await inspect_listings(page, "Home Care / No State")

        # Pass 2: add Texas state filter
        state_select = await find_select_with_states(page)
        pass2 = None
        if state_select:
            await state_select.select_option(label="Texas")
            await page.wait_for_timeout(2000)
            await page.keyboard.press("Enter")
            await page.wait_for_timeout(5000)
            await page.screenshot(path="structure_dump_pass2.png", full_page=True)
            pass2 = await inspect_listings(page, "Home Care / Texas", limit=5)
        else:
            print("WARNING: Could not find state dropdown for pass 2")

        await browser.close()

        output = {"pass1": pass1, "pass2": pass2}
        with open("structure_dump.json", "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2)
        print("\nDone. Results written to structure_dump.json")


if __name__ == "__main__":
    asyncio.run(main())
