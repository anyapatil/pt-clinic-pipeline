#!/usr/bin/env python3
"""
Find any email address on each clinic's website and store it as clinic_email.

Checks the homepage first; falls back to /contact if no email found there.
No filtering — stores the first email found regardless of what it is.

Usage:
    python scan_contacts.py                 # clinics with no clinic_email yet
    python scan_contacts.py --force         # re-scan all clinics with a website
    python scan_contacts.py --city Boston
    python scan_contacts.py --limit 50
"""

import argparse
import logging
import re
import sys
import os

from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(__file__))

from database import get_conn, init_db, update_clinic_email, DB_PATH
from scrapers.base import fetch_html, polite_sleep

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

_EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")


def find_email(html: str) -> str | None:
    """Return the first email found — mailto links first, then page text."""
    bs = BeautifulSoup(html, "lxml")
    for a in bs.find_all("a", href=True):
        href = a["href"].strip()
        if href.lower().startswith("mailto:"):
            email = href[7:].split("?")[0].strip()
            if _EMAIL_RE.match(email):
                return email
    m = _EMAIL_RE.search(bs.get_text(" ", strip=True))
    return m.group(0) if m else None


def scrape_email(website_url: str) -> str | None:
    """Fetch a clinic website and return the first email address found anywhere."""
    if not website_url.startswith("http"):
        website_url = "https://" + website_url
    try:
        html = fetch_html(website_url)
        if html:
            email = find_email(html)
            if email:
                return email
        # Try /contact as one fallback page
        polite_sleep()
        contact_html = fetch_html(website_url.rstrip("/") + "/contact")
        if contact_html:
            return find_email(contact_html)
    except Exception as e:
        logger.warning(f"{website_url}: {e}")
    return None


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
        conditions.append("clinic_email IS NULL")
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
    parser = argparse.ArgumentParser(description="Scan clinic websites for email addresses.")
    parser.add_argument("--city",  default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--force", action="store_true", help="Re-scan all clinics with a website")
    args = parser.parse_args()

    init_db()
    targets = get_targets(city=args.city, limit=args.limit, force=args.force)

    if not targets:
        logger.info("No eligible clinics found.")
        return

    logger.info(f"Scanning {len(targets)} clinic{'s' if len(targets) != 1 else ''} for email...")
    found = 0
    for i, clinic in enumerate(targets, 1):
        logger.info(f"[{i}/{len(targets)}] {clinic['name']} ({clinic['city']})  {clinic['website']}")
        email = scrape_email(clinic["website"])
        logger.info(f"  → {email!r}")
        update_clinic_email(clinic["id"], email)
        if email:
            found += 1
        if i < len(targets):
            polite_sleep()

    logger.info(f"Done. Found email for {found}/{len(targets)} clinics.")


if __name__ == "__main__":
    main()
