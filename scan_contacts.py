#!/usr/bin/env python3
"""
Targeted scan to populate primary_staff_name, primary_staff_email,
and primary_staff_linkedin for all clinics with a website.

For each clinic, checks: homepage, /contact, /contact-us, /about,
/about-us, /team — plus any team/contact links discovered on the homepage.
Does NOT touch staff_count or staff_names.

Usage:
    python scan_contacts.py                 # all clinics missing contact data
    python scan_contacts.py --force         # re-scan all clinics with a website
    python scan_contacts.py --city Boston   # single market
    python scan_contacts.py --limit 50      # cap at N clinics
"""

import argparse
import logging
import sys
import os
from urllib.parse import urljoin

sys.path.insert(0, os.path.dirname(__file__))

from database import get_conn, init_db, update_primary_contact, DB_PATH
from scrapers.staff_scraper import (
    _extract_email,
    _extract_linkedin,
    _count_staff_in_html,
    _find_team_links,
    _find_contact_links,
)
from scrapers.base import fetch_html, polite_sleep

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# Fixed sub-paths to probe on every clinic domain
_PROBE_PATHS = ["/contact", "/contact-us", "/about", "/about-us", "/team"]


def scan_clinic_contact(website_url: str) -> tuple[str | None, str | None, str | None]:
    """
    Return (primary_name, primary_email, primary_linkedin) for a clinic website.
    Checks homepage then probes fixed contact/about/team paths, then follows
    any discovered team/contact links.
    """
    if not website_url.startswith("http"):
        website_url = "https://" + website_url

    primary_name    = None
    primary_email   = None
    primary_linkedin = None
    visited: set[str] = set()

    try:
        html = fetch_html(website_url)
        if not html:
            return None, None, None

        visited.add(website_url)
        _, _, primary_name    = _count_staff_in_html(html)
        primary_email         = _extract_email(html)
        primary_linkedin      = _extract_linkedin(html)

        def _all_found() -> bool:
            return primary_name and primary_email and primary_linkedin

        # 1. Probe fixed paths
        for path in _PROBE_PATHS:
            if _all_found():
                break
            url = urljoin(website_url, path)
            if url in visited:
                continue
            visited.add(url)
            polite_sleep()
            page_html = fetch_html(url)
            if not page_html:
                continue
            if primary_name is None:
                _, _, pn = _count_staff_in_html(page_html)
                if pn:
                    primary_name = pn
            if primary_email is None:
                primary_email = _extract_email(page_html)
            if primary_linkedin is None:
                primary_linkedin = _extract_linkedin(page_html)

        # 2. Follow discovered team + contact links from homepage
        if not _all_found():
            discovered = (
                _find_team_links(html, website_url)
                + _find_contact_links(html, website_url, exclude=visited)
            )
            for url in discovered:
                if _all_found() or url in visited:
                    continue
                visited.add(url)
                polite_sleep()
                page_html = fetch_html(url)
                if not page_html:
                    continue
                if primary_name is None:
                    _, _, pn = _count_staff_in_html(page_html)
                    if pn:
                        primary_name = pn
                if primary_email is None:
                    primary_email = _extract_email(page_html)
                if primary_linkedin is None:
                    primary_linkedin = _extract_linkedin(page_html)

    except Exception as e:
        logger.warning(f"[contact] {website_url}: {e}")

    return primary_name, primary_email, primary_linkedin


def get_targets(
    city: str = None,
    limit: int = None,
    force: bool = False,
    db_path: str = DB_PATH,
) -> list[dict]:
    conn = get_conn(db_path)
    conditions = ["website IS NOT NULL", "website != ''"]
    params: list = []

    if city:
        conditions.append("city = ?")
        params.append(city)

    if not force:
        # Only clinics missing all three fields
        conditions.append(
            "(primary_staff_name IS NULL "
            " AND primary_staff_email IS NULL "
            " AND primary_staff_linkedin IS NULL)"
        )

    sql = (
        f"SELECT id, name, city, website FROM clinics "
        f"WHERE {' AND '.join(conditions)} ORDER BY id"
    )
    if limit:
        sql += f" LIMIT {int(limit)}"

    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def main():
    parser = argparse.ArgumentParser(description="Scan clinic websites for primary contact info.")
    parser.add_argument("--city",  default=None, help="Filter to a single city")
    parser.add_argument("--limit", type=int, default=None, help="Max clinics to scan")
    parser.add_argument("--force", action="store_true",
                        help="Re-scan clinics that already have contact data")
    args = parser.parse_args()

    init_db()
    targets = get_targets(city=args.city, limit=args.limit, force=args.force)

    if not targets:
        logger.info("No eligible clinics found.")
        return

    logger.info(f"Scanning {len(targets)} clinic{'s' if len(targets) != 1 else ''} for primary contact info...")

    found = 0
    for i, clinic in enumerate(targets, 1):
        logger.info(f"[{i}/{len(targets)}] {clinic['name']} ({clinic['city']})")
        logger.info(f"  URL: {clinic['website']}")

        name, email, linkedin = scan_clinic_contact(clinic["website"])
        logger.info(f"  → name={name!r}  email={email!r}  linkedin={linkedin!r}")

        update_primary_contact(clinic["id"], name, email, linkedin)
        if name or email or linkedin:
            found += 1

        if i < len(targets):
            polite_sleep()

    logger.info(f"Done. Contact info found for {found}/{len(targets)} clinics.")


if __name__ == "__main__":
    main()
