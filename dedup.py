#!/usr/bin/env python3
"""
Consolidate duplicate clinic records that share the same physical address.

When multiple records exist at the same address (e.g. individual practitioners
all registered at the same clinic), we:
  1. Pick the best canonical record (source priority: google_maps > choosept >
     npi_registry; clinic names preferred over practitioner names)
  2. Merge signals, keywords, phone, website, staff_count into the canonical
  3. Delete the rest

Usage:
    python dedup.py            # dry run — shows what would change
    python dedup.py --apply    # commit changes to the database
"""

import argparse
import json
import logging
import re
from collections import defaultdict
from typing import Any

from database import get_conn, DB_PATH

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
logger = logging.getLogger(__name__)

SOURCE_PRIORITY = {"google_maps": 0, "choosept": 1, "npi_registry": 2}

_CREDENTIAL_RE = re.compile(
    r",\s*\b(DPT|PT|MPT|MSPT|OCS|ATC|PhD|MS|MA|BS|FAAOMPT|SCS|CSCS|PCS|NCS|"
    r"GCS|COMT|PRPC|WCS|ScD|EdD|BSPT|LSVT|ASTYM|CAFS)\b",
    re.I,
)

# Street abbreviation expansions for address normalisation
_ABBREVS = [
    (r"\bst\b",     "street"),
    (r"\bave?\b",   "avenue"),
    (r"\bblvd\b",   "boulevard"),
    (r"\bdr\b",     "drive"),
    (r"\brd\b",     "road"),
    (r"\bln\b",     "lane"),
    (r"\bct\b",     "court"),
    (r"\bpl\b",     "place"),
    (r"\bsq\b",     "square"),
    (r"\bste\b",    "suite"),
    (r"\bapt\b",    "apartment"),
    (r"\bpkwy\b",   "parkway"),
    (r"\bhwy\b",    "highway"),
]


def normalize_address(addr: str) -> str:
    if not addr:
        return ""
    s = addr.lower().strip()
    s = re.sub(r"[,#\.]", " ", s)       # strip punctuation
    for pattern, replacement in _ABBREVS:
        s = re.sub(pattern, replacement, s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def is_practitioner_name(name: str) -> bool:
    """True if name looks like 'Firstname Lastname, PT, DPT' rather than a clinic."""
    return bool(_CREDENTIAL_RE.search(name))


def canonical_score(row: dict) -> tuple:
    """Lower score = better canonical. Prefer clinic names from authoritative sources."""
    src      = SOURCE_PRIORITY.get(row.get("source") or "", 3)
    person   = 1 if is_practitioner_name(row.get("name") or "") else 0
    no_web   = 0 if row.get("website") else 1
    no_staff = 0 if (row.get("staff_count") is not None and (row.get("staff_count") or 0) > 0) else 1
    return (src, person, no_web, no_staff)


def merge_into(canonical: dict, others: list) -> dict:
    merged = dict(canonical)
    for r in others:
        # OR-merge boolean signal columns
        for col in ("specialty_sports", "specialty_ortho", "specialty_pelvic", "cash_pay_signal"):
            merged[col] = max(merged.get(col) or 0, r.get(col) or 0)
        # Union cash-pay keyword sets
        try:
            kws = set(json.loads(merged.get("cash_pay_keywords") or "[]"))
            kws.update(json.loads(r.get("cash_pay_keywords") or "[]"))
            merged["cash_pay_keywords"] = json.dumps(sorted(kws))
        except Exception:
            pass
        # Fill blank fields from duplicates
        for field in ("phone", "website", "zip_code", "staff_count", "listing_text"):
            if not merged.get(field) and r.get(field):
                merged[field] = r[field]
    return merged


def normalize_phone(phone: str) -> str:
    """Strip non-digits and return last 10 digits (US numbers)."""
    if not phone:
        return ""
    digits = re.sub(r"\D", "", phone)
    return digits[-10:] if len(digits) >= 10 else digits


# ---------------------------------------------------------------------------
# Union-Find
# ---------------------------------------------------------------------------

class UnionFind:
    def __init__(self):
        self._parent: dict[Any, Any] = {}

    def find(self, x):
        self._parent.setdefault(x, x)
        if self._parent[x] != x:
            self._parent[x] = self.find(self._parent[x])
        return self._parent[x]

    def union(self, a, b):
        self._parent[self.find(a)] = self.find(b)

    def groups(self) -> dict:
        result: dict[Any, list] = defaultdict(list)
        for x in self._parent:
            result[self.find(x)].append(x)
        return dict(result)


def run(dry_run: bool = True, db_path: str = DB_PATH):
    conn = get_conn(db_path)
    all_rows = [dict(r) for r in conn.execute("SELECT * FROM clinics").fetchall()]
    logger.info(f"Total records before dedup: {len(all_rows)}")

    id_to_row = {r["id"]: r for r in all_rows}
    uf = UnionFind()

    # Seed every id into the UF so isolated records appear in groups() too
    for r in all_rows:
        uf.find(r["id"])

    # --- Pass 1: group by normalised address (practitioner-name safety applies) ---
    addr_groups: dict[str, list[int]] = defaultdict(list)
    for r in all_rows:
        key = normalize_address(r.get("address") or "")
        if key:
            addr_groups[key].append(r["id"])

    for addr_key, ids in addr_groups.items():
        if len(ids) < 2:
            continue
        rows_in_group = [id_to_row[i] for i in ids]
        # Safety: only merge address groups where at least one is a practitioner name.
        # Co-located distinct businesses share an address but should not be merged.
        if not any(is_practitioner_name(r.get("name") or "") for r in rows_in_group):
            continue
        for i in ids[1:]:
            uf.union(ids[0], i)

    # --- Pass 2: group by (normalised address + normalised phone) ---
    # Same address + same phone = definitely the same business, no practitioner
    # safety check needed.  Phone-only matching is too broad (a practitioner may
    # share a clinic's main number without being a duplicate of the clinic entity).
    addr_phone_groups: dict[tuple, list[int]] = defaultdict(list)
    for r in all_rows:
        addr = normalize_address(r.get("address") or "")
        phone = normalize_phone(r.get("phone") or "")
        if addr and phone:
            addr_phone_groups[(addr, phone)].append(r["id"])

    for key, ids in addr_phone_groups.items():
        if len(ids) < 2:
            continue
        for i in ids[1:]:
            uf.union(ids[0], i)

    # --- Collect groups with more than one member ---
    raw_groups = uf.groups()
    dup_groups: dict[int, list[dict]] = {}
    for root, members in raw_groups.items():
        if len(members) > 1:
            dup_groups[root] = [id_to_row[m] for m in members]

    logger.info(f"Groups with duplicates (address + phone): {len(dup_groups)}")

    total_to_delete = 0
    updates = []
    deletes = []

    for _root, group in sorted(dup_groups.items(), key=lambda x: -len(x[1])):
        group_sorted = sorted(group, key=canonical_score)
        canonical   = group_sorted[0]
        duplicates  = group_sorted[1:]
        merged      = merge_into(canonical, duplicates)
        dup_ids     = [r["id"] for r in duplicates]
        total_to_delete += len(dup_ids)

        logger.info(
            f"\n  [{len(group)}→1]  keep '{canonical['name']}'"
            f"  (id={canonical['id']}, src={canonical['source']})"
        )
        for d in duplicates:
            logger.info(f"    drop  '{d['name']}'  (id={d['id']}, src={d['source']})")

        updates.append(merged)
        deletes.extend(dup_ids)

    if dry_run:
        logger.info(
            f"\nDRY RUN — {total_to_delete} records would be removed "
            f"({len(all_rows)} → {len(all_rows) - total_to_delete}). "
            f"Run with --apply to commit."
        )
        conn.close()
        return

    # Apply: update canonicals, then delete duplicates
    for merged in updates:
        conn.execute("""
            UPDATE clinics SET
                phone             = :phone,
                website           = :website,
                zip_code          = :zip_code,
                staff_count       = :staff_count,
                listing_text      = :listing_text,
                specialty_sports  = :specialty_sports,
                specialty_ortho   = :specialty_ortho,
                specialty_pelvic  = :specialty_pelvic,
                cash_pay_signal   = :cash_pay_signal,
                cash_pay_keywords = :cash_pay_keywords
            WHERE id = :id
        """, merged)

    if deletes:
        conn.execute(
            f"DELETE FROM clinics WHERE id IN ({','.join('?' * len(deletes))})",
            deletes,
        )

    conn.commit()
    remaining = conn.execute("SELECT COUNT(*) FROM clinics").fetchone()[0]
    logger.info(f"\nDeleted {total_to_delete} duplicate records. Remaining: {remaining}")
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Commit changes (default is dry run)")
    parser.add_argument("--db",    default=DB_PATH)
    args = parser.parse_args()
    run(dry_run=not args.apply, db_path=args.db)
