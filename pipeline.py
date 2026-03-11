"""
Main pipeline: orchestrates scrapers, signal detection, and DB writes.
"""

import asyncio
import logging
import requests
from datetime import datetime

from config import CITIES
from database import init_db, upsert_clinic, start_run, finish_run, DB_PATH
from signals import analyze_clinic
from scrapers.base import fetch_website_text, polite_sleep
from scrapers.choosept import scrape_choosept
from scrapers.therapyfinder import scrape_therapyfinder
from scrapers.google_maps import scrape_google_maps

logger = logging.getLogger(__name__)


def _enrich_with_signals(clinics: list[dict], scan_websites: bool = False) -> list[dict]:
    """
    Run signal detection on each clinic's listing_text (and optionally website).
    Mutates and returns the list.
    """
    session = requests.Session()
    for i, c in enumerate(clinics):
        website_text = ""
        if scan_websites and c.get("website"):
            logger.debug(f"  Scanning website {i+1}/{len(clinics)}: {c['website']}")
            website_text = fetch_website_text(c["website"], session=session)
            c["website_text"] = website_text
            c["website_checked_at"] = datetime.utcnow().isoformat()
            polite_sleep()

        signals = analyze_clinic(
            listing_text=c.get("listing_text", ""),
            website_text=website_text,
        )
        c.update(signals)
    return clinics


def run_city(city_key: str, scan_websites: bool = False, sources: list[str] = None, db_path: str = DB_PATH):
    """
    Scrape all sources for a city, detect signals, and persist to DB.
    """
    if city_key not in CITIES:
        raise ValueError(f"Unknown city key: {city_key}. Valid: {list(CITIES.keys())}")

    city_cfg = CITIES[city_key]
    if sources is None:
        sources = ["google_maps", "choosept", "therapyfinder"]

    init_db(db_path)

    total_saved = 0

    for source in sources:
        run_id = start_run(city_key, source, db_path)
        clinics: list[dict] = []

        try:
            logger.info(f"=== Scraping {source} for {city_cfg['label']} ===")

            if source == "google_maps":
                clinics = asyncio.run(scrape_google_maps(city_key, city_cfg, scan_websites))
            elif source == "choosept":
                clinics = scrape_choosept(city_key, city_cfg, scan_websites)
            elif source == "therapyfinder":
                clinics = scrape_therapyfinder(city_key, city_cfg, scan_websites)
            else:
                logger.warning(f"Unknown source: {source}")
                finish_run(run_id, 0, "skipped", db_path)
                continue

            logger.info(f"  Enriching {len(clinics)} clinics with signal detection...")
            clinics = _enrich_with_signals(clinics, scan_websites=scan_websites)

            saved = 0
            for c in clinics:
                try:
                    upsert_clinic(c, db_path)
                    saved += 1
                except Exception as e:
                    logger.warning(f"  DB upsert error for '{c.get('name')}': {e}")

            logger.info(f"  Saved {saved}/{len(clinics)} clinics from {source}")
            total_saved += saved
            finish_run(run_id, saved, "ok", db_path)

        except Exception as e:
            logger.error(f"  Source {source} failed: {e}", exc_info=True)
            finish_run(run_id, len(clinics), "error", db_path)

    logger.info(f"=== {city_cfg['label']}: {total_saved} total clinics saved ===")
    return total_saved


def run_all(scan_websites: bool = False, sources: list[str] = None, db_path: str = DB_PATH):
    """Run pipeline for all configured cities."""
    grand_total = 0
    for city_key in CITIES:
        count = run_city(city_key, scan_websites=scan_websites, sources=sources, db_path=db_path)
        grand_total += count
    logger.info(f"All cities complete. Grand total: {grand_total} clinics.")
    return grand_total
