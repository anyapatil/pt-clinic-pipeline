"""
Google Maps scraper using Playwright.

Strategy:
1. Search Google Maps and scroll the results list.
2. Collect the href of each result card (each points to a /maps/place/... URL).
3. Visit each place URL directly — this gives structured name/address/phone/website
   in the page DOM without needing to click-and-back navigate.
"""

import asyncio
import logging
import re
from typing import Optional

from playwright.async_api import async_playwright, TimeoutError as PWTimeout

from config import MAPS_SCROLL_PAUSES, MAPS_MAX_RESULTS

logger = logging.getLogger(__name__)

_PLACE_URL_RE = re.compile(r"https://www\.google\.com/maps/place/[^\"']+")


async def _scroll_panel(page, pauses: int = MAPS_SCROLL_PAUSES):
    try:
        panel = page.locator('div[role="feed"]').first
        for _ in range(pauses):
            await panel.evaluate("el => el.scrollBy(0, 2000)")
            await page.wait_for_timeout(1200)
    except Exception as e:
        logger.debug(f"scroll: {e}")


async def _collect_place_urls(page) -> list[str]:
    """Collect all /maps/place/... hrefs from the results feed."""
    urls: list[str] = []
    try:
        links = await page.locator('div[role="feed"] a[href*="/maps/place/"]').all()
        for link in links:
            href = await link.get_attribute("href") or ""
            if "/maps/place/" in href and href not in urls:
                urls.append(href)
    except Exception as e:
        logger.debug(f"collect_place_urls: {e}")
    return urls


async def _scrape_place_page(context, url: str) -> Optional[dict]:
    """
    Open a Google Maps place URL in a new tab and extract structured data.
    """
    page = await context.new_page()
    try:
        await page.goto(url, timeout=20_000, wait_until="domcontentloaded")
        await page.wait_for_timeout(2500)

        # Dismiss cookie consent if present
        try:
            await page.locator('button:has-text("Accept all")').first.click(timeout=2000)
            await page.wait_for_timeout(800)
        except Exception:
            pass

        # Name
        name = ""
        for sel in ['h1[class*="fontHeadlineLarge"]', 'h1[class*="DUwDvf"]', 'h1']:
            try:
                name = await page.locator(sel).first.inner_text(timeout=3000)
                if name:
                    break
            except Exception:
                pass

        if not name:
            return None

        # Address
        address = ""
        try:
            btn = page.locator('button[data-item-id="address"]').first
            raw = await btn.get_attribute("aria-label", timeout=3000) or ""
            address = raw.replace("Address: ", "").strip()
        except Exception:
            pass

        # Phone
        phone = ""
        try:
            btn = page.locator('button[data-item-id^="phone:tel"]').first
            raw = await btn.get_attribute("aria-label", timeout=3000) or ""
            phone = raw.replace("Phone: ", "").strip()
        except Exception:
            pass

        # Website
        website = ""
        try:
            web_link = page.locator('a[data-item-id="authority"]').first
            website = await web_link.get_attribute("href", timeout=3000) or ""
        except Exception:
            pass

        # Category / description text visible on the card
        listing_text = ""
        try:
            listing_text = await page.locator('button[jsaction*="category"]').first.inner_text(timeout=2000)
        except Exception:
            pass

        # Strip Google Maps link-text artifact: "Visit Foo Bar's website"
        name = name.strip()
        _visit_match = re.match(r"^Visit (.+?)'s website$", name, re.I)
        if _visit_match:
            name = _visit_match.group(1).strip()

        return {
            "name": name,
            "address": address,
            "phone": phone,
            "website": website,
            "listing_text": listing_text,
        }

    except Exception as e:
        logger.debug(f"place page error ({url[:60]}): {e}")
        return None
    finally:
        await page.close()


async def scrape_google_maps(city_key: str, city_cfg: dict, scan_websites: bool = False) -> list[dict]:
    """
    Run all search queries for a city and return a deduplicated list of clinic dicts.
    """
    place_urls: list[str] = []  # ordered list of unique place URLs
    url_set: set[str] = set()

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        context = await browser.new_context(
            viewport={"width": 1280, "height": 900},
            locale="en-US",
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
        )

        # --- Phase 1: collect place URLs from search results ---
        list_page = await context.new_page()
        for query in city_cfg["search_terms"]:
            if len(place_urls) >= MAPS_MAX_RESULTS:
                break
            logger.info(f"[google_maps] Collecting URLs for: {query}")
            encoded = query.replace(" ", "+")
            search_url = f"https://www.google.com/maps/search/{encoded}/"
            try:
                await list_page.goto(search_url, timeout=25_000, wait_until="domcontentloaded")
                await list_page.wait_for_timeout(3000)
                # Dismiss consent
                try:
                    await list_page.locator('button:has-text("Accept all")').first.click(timeout=2500)
                    await list_page.wait_for_timeout(800)
                except Exception:
                    pass
                try:
                    await list_page.wait_for_selector('div[role="feed"]', timeout=10_000)
                except PWTimeout:
                    logger.warning(f"  No feed for: {query}")
                    continue

                await _scroll_panel(list_page)
                urls = await _collect_place_urls(list_page)
                new = 0
                for u in urls:
                    if u not in url_set:
                        url_set.add(u)
                        place_urls.append(u)
                        new += 1
                logger.info(f"  +{new} place URLs (total {len(place_urls)})")
            except Exception as e:
                logger.error(f"Search failed '{query}': {e}")

        await list_page.close()
        logger.info(f"[google_maps] Phase 1 complete: {len(place_urls)} place URLs")

        # --- Phase 2: visit each place URL for structured data ---
        results: list[dict] = []
        cap = min(len(place_urls), MAPS_MAX_RESULTS)
        logger.info(f"[google_maps] Phase 2: scraping {cap} place pages ...")
        for i, purl in enumerate(place_urls[:cap]):
            data = await _scrape_place_page(context, purl)
            if data and data.get("name"):
                data["city"] = city_cfg["label"].split(",")[0].strip()
                data["state"] = city_cfg["state"]
                data["source"] = "google_maps"
                data["source_url"] = purl
                results.append(data)
                if (i + 1) % 10 == 0:
                    logger.info(f"  ... {i+1}/{cap} done ({len(results)} with data)")
            await asyncio.sleep(0.3)

        await browser.close()

    logger.info(f"[google_maps] {city_key}: {len(results)} clinics with data")
    return results
