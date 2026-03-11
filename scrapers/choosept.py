"""
Scraper for the APTA's Find a PT directory (choosept.com).

The search results are server-rendered HTML at:
  https://www.choosept.com/find-a-pt?lat=...&lon=...&loc=...&dist=25&p=N
"""

import logging
import re
import requests
from urllib.parse import urljoin
from scrapers.base import polite_sleep, fetch_html, soup, clean

logger = logging.getLogger(__name__)

BASE_URL = "https://www.choosept.com"
SEARCH_PATH = "/find-a-pt"
MAX_PAGES = 15


def _parse_card(card, city_label: str, state: str) -> dict | None:
    name_el = card.select_one(".find-a-pt__profile-title a")
    if not name_el:
        return None
    name = clean(name_el.get_text())
    if not name:
        return None

    profile_path = name_el.get("href", "")
    profile_url = urljoin(BASE_URL, profile_path) if profile_path else ""

    # Phone
    phone_el = card.select_one('a[href^="tel:"]')
    phone = phone_el["href"].replace("tel:", "").strip() if phone_el else ""

    # External website
    web_el = card.select_one('a[href^="http"]')
    website = web_el["href"].strip() if web_el else ""

    # Address — extract from raw text via regex after collapsing whitespace
    raw_text = re.sub(r"\s+", " ", card.get_text(" ", strip=True))

    # Pattern: street number + street type, then city/state/zip
    addr_m = re.search(
        r"(\d[\w\s\.\#\-]+(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|"
        r"Drive|Dr|Way|Lane|Ln|Court|Ct|Place|Pl|Floor|Suite|Ste)"
        r"[\w\s\.\#\-,]*?\b[A-Z]{2}\b\s*\d{5})",
        raw_text,
        re.IGNORECASE,
    )
    address = clean(addr_m.group(1)) if addr_m else ""

    # If no street address, look for the city/state/zip block
    if not address:
        zip_m = re.search(r"([A-Za-z\s]+,?\s+[A-Z]{2}\s+\d{5})", raw_text)
        address = clean(zip_m.group(1)) if zip_m else ""

    # Listing text — the full card text for signal detection
    listing_text = raw_text[:500]

    return {
        "name": name,
        "address": address,
        "city": city_label.split(",")[0].strip(),
        "state": state,
        "zip_code": "",
        "phone": phone,
        "website": website,
        "listing_text": listing_text,
        "source": "choosept",
        "source_url": profile_url or (BASE_URL + SEARCH_PATH),
    }


def scrape_choosept(city_key: str, city_cfg: dict, scan_websites: bool = False) -> list[dict]:
    """
    Scrape choosept.com HTML search results for a city.
    Paginates through up to MAX_PAGES pages.
    """
    clinics: list[dict] = []
    seen_names: set[str] = set()

    base_params = (
        f"lat={city_cfg['choosept_lat']}"
        f"&lon={city_cfg['choosept_lng']}"
        f"&loc={city_cfg['choosept_location'].replace(' ', '+').replace(',', '%2c')}"
        f"&dist=25"
    )

    for page in range(1, MAX_PAGES + 1):
        url = f"{BASE_URL}{SEARCH_PATH}?{base_params}&p={page}"
        logger.info(f"[choosept] Page {page}: {url}")

        html = fetch_html(url)
        if not html:
            logger.warning(f"[choosept] Empty response on page {page}, stopping.")
            break

        s = soup(html)
        cards = s.select(".find-a-pt__results-item")
        if not cards:
            logger.info(f"[choosept] No cards on page {page}, done.")
            break

        page_count = 0
        for card in cards:
            rec = _parse_card(card, city_cfg["label"], city_cfg["state"])
            if rec and rec["name"] not in seen_names:
                seen_names.add(rec["name"])
                clinics.append(rec)
                page_count += 1

        logger.info(f"[choosept] Page {page}: +{page_count} clinics (total {len(clinics)})")

        # Check if there are more pages
        page_links = s.select(".pagination a")
        has_next = any(f"p={page + 1}" in (a.get("href", "")) for a in page_links)
        if not has_next:
            break

        polite_sleep()

    logger.info(f"[choosept] {city_key}: {len(clinics)} clinics total")
    return clinics
