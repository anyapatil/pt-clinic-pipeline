#!/usr/bin/env python3
"""
Re-validate stored staff_names arrays against the current _looks_like_name
filter and remove any entries that don't pass.

Usage:
    python clean_staff_names.py            # dry run — shows what would change
    python clean_staff_names.py --apply    # commit changes to the database
"""

import argparse
import json
import logging
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from database import get_conn, DB_PATH
from scrapers.staff_scraper import _looks_like_name

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def clean(dry_run: bool = True, db_path: str = DB_PATH):
    conn = get_conn(db_path)
    rows = conn.execute(
        "SELECT id, name, staff_names, staff_count FROM clinics "
        "WHERE staff_names IS NOT NULL AND staff_names != '' AND staff_names != '[]'"
    ).fetchall()

    logger.info(f"Checking {len(rows)} records with stored staff names…")

    changed = 0
    removed_total = 0

    for row in rows:
        clinic_id   = row["id"]
        clinic_name = row["name"]
        raw         = row["staff_names"] or "[]"
        old_count   = row["staff_count"]

        try:
            original = json.loads(raw)
        except Exception:
            continue

        if not original:
            continue

        filtered = [n for n in original if _looks_like_name(n)]
        removed  = len(original) - len(filtered)

        if removed == 0:
            continue

        changed      += 1
        removed_total += removed

        logger.info(
            f"  [{clinic_id}] {clinic_name[:55]}"
            f"\n    before ({len(original)}): {original[:5]}{'…' if len(original) > 5 else ''}"
            f"\n    after  ({len(filtered)}): {filtered[:5]}{'…' if len(filtered) > 5 else ''}"
            f"\n    dropped: {[n for n in original if n not in filtered][:5]}"
        )

        if not dry_run:
            # Update staff_names; update staff_count only when the old count
            # matched the old name list length (i.e. count came from name detection)
            new_count = old_count
            if old_count is not None and old_count == len(original):
                new_count = len(filtered)

            conn.execute(
                "UPDATE clinics SET staff_names = ?, staff_count = ? WHERE id = ?",
                (json.dumps(filtered), new_count, clinic_id),
            )

    if dry_run:
        logger.info(
            f"\nDRY RUN — {changed} records would be updated, "
            f"{removed_total} invalid name entries removed. "
            f"Run with --apply to commit."
        )
    else:
        conn.commit()
        logger.info(
            f"\nDone — {changed} records updated, "
            f"{removed_total} invalid name entries removed."
        )

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Commit changes (default is dry run)")
    parser.add_argument("--db",    default=DB_PATH)
    args = parser.parse_args()
    clean(dry_run=not args.apply, db_path=args.db)
