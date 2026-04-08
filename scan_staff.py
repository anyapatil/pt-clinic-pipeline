#!/usr/bin/env python3
"""
Scan clinic websites to estimate PT staff count.

Usage:
    python scan_staff.py                    # cash-pay clinics only
    python scan_staff.py --all              # all clinics with a website
    python scan_staff.py --city Boston      # one market
    python scan_staff.py --limit 50         # cap at N clinics
    python scan_staff.py --force            # re-scan already-scanned clinics
"""

import argparse
import logging
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from database import get_conn, update_staff_count, init_db, DB_PATH
from scrapers.staff_scraper import scrape_staff_count
from scrapers.base import polite_sleep

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def get_targets(
    city: str = None,
    limit: int = None,
    force: bool = False,
    all_clinics: bool = False,
    db_path: str = DB_PATH,
) -> list[dict]:
    conn = get_conn(db_path)
    conditions = [
        "website IS NOT NULL",
        "website != ''",
    ]
    if not all_clinics:
        conditions.append("cash_pay_signal = 1")
    params: list = []

    if city:
        conditions.append("city = ?")
        params.append(city)

    if not force:
        conditions.append("staff_count IS NULL")

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
    parser = argparse.ArgumentParser(description="Scan clinic websites for staff count.")
    parser.add_argument("--city",  default=None, help="Filter to a single city label (e.g. 'Boston')")
    parser.add_argument("--limit", type=int, default=None, help="Max clinics to scan")
    parser.add_argument("--force", action="store_true", help="Re-scan already-scanned clinics")
    parser.add_argument("--all",   dest="all_clinics", action="store_true",
                        help="Scan all clinics with a website, not just cash-pay flagged ones")
    args = parser.parse_args()

    init_db()
    targets = get_targets(
        city=args.city, limit=args.limit, force=args.force,
        all_clinics=args.all_clinics,
    )

    if not targets:
        logger.info("No eligible clinics found.")
        return

    scope = "all" if args.all_clinics else "cash-pay"
    logger.info(f"Scanning {len(targets)} {scope} clinic{'s' if len(targets) != 1 else ''} for staff count...")

    scanned = 0
    for i, clinic in enumerate(targets, 1):
        logger.info(f"[{i}/{len(targets)}] {clinic['name']} ({clinic['city']})")
        logger.info(f"  URL: {clinic['website']}")

        count, names, primary_name, primary_email, primary_linkedin = scrape_staff_count(clinic["website"])
        logger.info(
            f"  → staff_count = {count}  primary = {primary_name!r}  "
            f"email = {primary_email!r}  linkedin = {primary_linkedin!r}  "
            f"names = {names[:3]}{'…' if len(names) > 3 else ''}"
        )

        if count is not None:
            update_staff_count(clinic["id"], count, names, primary_name, primary_email, primary_linkedin)
            scanned += 1

        if i < len(targets):
            polite_sleep()

    logger.info(f"Done. Updated {scanned}/{len(targets)} records.")


if __name__ == "__main__":
    main()
