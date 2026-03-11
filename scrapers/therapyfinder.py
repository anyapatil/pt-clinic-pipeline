"""
'therapyfinder' source — scrapes therapyfinder.com, then falls back to the
free CMS NPI Registry API (physical therapy organizations + practitioners).

NPI Registry: https://npiregistry.cms.hhs.gov/api
No key required; returns structured clinic data.
"""

import logging
import re
import requests
from urllib.parse import urljoin, urlencode
from scrapers.base import polite_sleep, fetch_html, soup, clean

logger = logging.getLogger(__name__)

NPI_API = "https://npiregistry.cms.hhs.gov/api/"

# NPI taxonomy codes that cover physical therapy
PT_TAXONOMY_TERMS = [
    "Physical Therapist",
    "Physical Therapy",
]


# ---------------------------------------------------------------------------
# NPI Registry helpers
# ---------------------------------------------------------------------------

def _npi_org_search(city: str, state: str, taxonomy: str, skip: int = 0) -> list[dict]:
    params = {
        "version": "2.1",
        "enumeration_type": "NPI-2",          # organizations
        "taxonomy_description": taxonomy,
        "city": city,
        "state": state,
        "limit": 200,
        "skip": skip,
    }
    try:
        resp = requests.get(NPI_API, params=params, timeout=15,
                            headers={"Accept": "application/json"})
        resp.raise_for_status()
        return resp.json().get("results", [])
    except Exception as e:
        logger.warning(f"[npi] org query failed (city={city}, taxonomy={taxonomy}): {e}")
        return []


def _npi_record_to_clinic(r: dict, city_label: str, state: str) -> dict | None:
    basic = r.get("basic", {})
    name = clean(
        basic.get("organization_name")
        or basic.get("name")
        or f"{basic.get('first_name','')} {basic.get('last_name','')}".strip()
    )
    if not name:
        return None

    # Prefer the LOCATION address over MAILING
    addresses = r.get("addresses", [])
    location_addrs = [a for a in addresses if a.get("address_purpose") == "LOCATION"]
    addr_rec = location_addrs[0] if location_addrs else (addresses[0] if addresses else {})

    street = clean(addr_rec.get("address_1", ""))
    street2 = clean(addr_rec.get("address_2", ""))
    addr_city = clean(addr_rec.get("city", ""))
    addr_state = addr_rec.get("state", "")
    zip_code = addr_rec.get("postal_code", "")[:5]
    phone = clean(addr_rec.get("telephone_number", ""))

    parts = [p for p in [street, street2, addr_city, addr_state, zip_code] if p]
    address = ", ".join(parts)

    # Taxonomy description as listing text
    taxonomies = r.get("taxonomies", [])
    tax_text = " ".join(t.get("desc", "") for t in taxonomies if t.get("primary"))
    listing_text = f"{name} {tax_text}".strip()

    return {
        "name": name,
        "address": address,
        "city": city_label.split(",")[0].strip(),
        "state": addr_state or state,
        "zip_code": zip_code,
        "phone": phone,
        "website": "",       # NPI doesn't include websites
        "listing_text": listing_text,
        "source": "npi_registry",
        "source_url": f"https://npiregistry.cms.hhs.gov/provider-view/{r.get('number','')}",
    }


def _scrape_npi(city_key: str, city_cfg: dict) -> list[dict]:
    city_label = city_cfg["label"]
    # Use explicit npi_city if set, otherwise derive from label
    city_name = city_cfg.get("npi_city") or city_label.split(",")[0].strip()
    state = city_cfg["state"]

    clinics: list[dict] = []
    seen: set[str] = set()

    for taxonomy in PT_TAXONOMY_TERMS:
        skip = 0
        while True:
            logger.info(f"[npi] {city_name}, {state} — '{taxonomy}' skip={skip}")
            results = _npi_org_search(city_name, state, taxonomy, skip)
            if not results:
                break
            for r in results:
                rec = _npi_record_to_clinic(r, city_label, state)
                if rec:
                    key = f"{rec['name']}|{rec['address']}"
                    if key not in seen:
                        seen.add(key)
                        clinics.append(rec)
            if len(results) < 200:
                break
            skip += 200
            polite_sleep()

    logger.info(f"[npi] {city_key}: {len(clinics)} org clinics")
    return clinics


# ---------------------------------------------------------------------------
# therapyfinder.com (mental-health focused but worth a try)
# ---------------------------------------------------------------------------

def _scrape_therapyfinder_html(city_cfg: dict) -> list[dict]:
    """
    Best-effort HTML scrape of therapyfinder.com.
    Returns empty list if the site structure doesn't match.
    """
    city_label = city_cfg["label"]
    state = city_cfg["state"]
    location = city_cfg["therapyfinder_location"]
    clinics: list[dict] = []

    # Try known URL patterns
    candidate_urls = [
        f"https://therapyfinder.com/us/{state.lower()}/{location.split(',')[0].strip().lower().replace(' ', '-')}/physical-therapists/",
        f"https://www.therapyfinder.com/search?specialty=physical-therapy&location={location.replace(' ', '+')}",
    ]

    for url in candidate_urls:
        html = fetch_html(url)
        if not html:
            continue
        s = soup(html)
        cards = (
            s.select("div.provider-card")
            or s.select("article[class*=therapist]")
            or s.select("div[class*=therapist-card]")
            or s.select("div[class*=listing-card]")
        )
        for card in cards:
            name_el = card.select_one("h2, h3, [class*=name]")
            name = clean(name_el.get_text()) if name_el else ""
            if not name:
                continue
            address = clean((card.select_one("[class*=address]") or {}).get_text("") or "")
            phone_m = re.search(r"\(?\d{3}\)?[\s\-\.]\d{3}[\s\-\.]\d{4}", card.get_text())
            phone = phone_m.group(0) if phone_m else ""
            web_el = card.select_one("a[href^='http']")
            website = web_el["href"] if web_el else ""
            clinics.append({
                "name": name,
                "address": address,
                "city": city_label.split(",")[0].strip(),
                "state": state,
                "zip_code": "",
                "phone": phone,
                "website": website,
                "listing_text": clean(card.get_text(" ")),
                "source": "therapyfinder",
                "source_url": url,
            })
        if clinics:
            break
        polite_sleep()

    return clinics


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def scrape_therapyfinder(city_key: str, city_cfg: dict, scan_websites: bool = False) -> list[dict]:
    """
    1. Try therapyfinder.com HTML scrape.
    2. Always supplement with CMS NPI Registry org data.
    """
    clinics: list[dict] = []

    # Try therapyfinder.com first
    tf_clinics = _scrape_therapyfinder_html(city_cfg)
    if tf_clinics:
        logger.info(f"[therapyfinder] HTML: {len(tf_clinics)} clinics")
        clinics.extend(tf_clinics)

    # Always hit NPI registry for additional coverage
    npi_clinics = _scrape_npi(city_key, city_cfg)
    clinics.extend(npi_clinics)

    logger.info(f"[therapyfinder/npi] {city_key}: {len(clinics)} total clinics")
    return clinics
