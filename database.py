"""
SQLite database setup and query helpers.
"""

import re
import sqlite3
import json
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse
from config import DB_PATH

_CREDENTIAL_RE = re.compile(
    r",\s*\b(DPT|PT|MPT|MSPT|OCS|ATC|PhD|MS|MA|BS|FAAOMPT|SCS|CSCS|PCS|NCS|"
    r"GCS|COMT|PRPC|WCS|ScD|EdD|BSPT|LSVT|ASTYM|CAFS)\b",
    re.I,
)


def _is_practitioner_name(name: str) -> bool:
    return bool(_CREDENTIAL_RE.search(name or ""))


def _normalize_domain(url: str) -> str:
    """Return bare hostname (no www., no path) for grouping chain locations."""
    if not url:
        return ""
    url = url.strip()
    if not url.startswith("http"):
        url = "https://" + url
    try:
        host = urlparse(url).netloc.lower()
        if host.startswith("www."):
            host = host[4:]
        return host
    except Exception:
        return ""


def get_conn(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db(db_path: str = DB_PATH):
    conn = get_conn(db_path)
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS clinics (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            name             TEXT    NOT NULL,
            address          TEXT,
            city             TEXT,
            state            TEXT,
            zip_code         TEXT,
            phone            TEXT,
            website          TEXT,
            source           TEXT,
            source_url       TEXT,
            listing_text     TEXT,
            website_text     TEXT,
            specialty_sports INTEGER DEFAULT 0,
            specialty_ortho  INTEGER DEFAULT 0,
            specialty_pelvic INTEGER DEFAULT 0,
            cash_pay_signal  INTEGER DEFAULT 0,
            cash_pay_keywords TEXT,
            scraped_at       TEXT    DEFAULT (datetime('now')),
            website_checked_at TEXT,
            UNIQUE(name, address)
        );

        CREATE INDEX IF NOT EXISTS idx_clinics_city          ON clinics(city);
        CREATE INDEX IF NOT EXISTS idx_clinics_cash_pay      ON clinics(cash_pay_signal);
        CREATE INDEX IF NOT EXISTS idx_clinics_sports        ON clinics(specialty_sports);
        CREATE INDEX IF NOT EXISTS idx_clinics_ortho         ON clinics(specialty_ortho);
        CREATE INDEX IF NOT EXISTS idx_clinics_pelvic        ON clinics(specialty_pelvic);

        CREATE TABLE IF NOT EXISTS scrape_runs (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            city          TEXT,
            source        TEXT,
            started_at    TEXT DEFAULT (datetime('now')),
            finished_at   TEXT,
            clinics_found INTEGER DEFAULT 0,
            status        TEXT DEFAULT 'running'
        );
        """
    )
    conn.commit()

    # Non-destructive migrations: add columns introduced after initial schema
    migrations = [
        "ALTER TABLE clinics ADD COLUMN staff_count INTEGER",
        "ALTER TABLE clinics ADD COLUMN staff_names TEXT",
        "ALTER TABLE clinics ADD COLUMN website_domain TEXT",
        "ALTER TABLE clinics ADD COLUMN is_practitioner INTEGER DEFAULT 0",
    ]
    for stmt in migrations:
        try:
            conn.execute(stmt)
            conn.commit()
        except sqlite3.OperationalError:
            pass  # column already exists

    # Populate website_domain for rows that have a website but no domain yet
    rows = conn.execute(
        "SELECT id, website FROM clinics WHERE website_domain IS NULL AND website IS NOT NULL AND website != ''"
    ).fetchall()
    for row in rows:
        domain = _normalize_domain(row["website"])
        conn.execute("UPDATE clinics SET website_domain = ? WHERE id = ?", (domain, row["id"]))

    # Populate is_practitioner for all rows (re-evaluate on every startup to catch new records)
    rows = conn.execute("SELECT id, name FROM clinics").fetchall()
    for row in rows:
        flag = 1 if _is_practitioner_name(row["name"]) else 0
        conn.execute("UPDATE clinics SET is_practitioner = ? WHERE id = ?", (flag, row["id"]))

    conn.commit()
    conn.close()


def upsert_clinic(data: dict, db_path: str = DB_PATH) -> int:
    """
    Insert a clinic or update if (name, address) already exists.
    Returns the row id.
    """
    conn = get_conn(db_path)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO clinics
            (name, address, city, state, zip_code, phone, website, source,
             source_url, listing_text, website_text,
             specialty_sports, specialty_ortho, specialty_pelvic,
             cash_pay_signal, cash_pay_keywords, scraped_at, website_checked_at)
        VALUES
            (:name, :address, :city, :state, :zip_code, :phone, :website, :source,
             :source_url, :listing_text, :website_text,
             :specialty_sports, :specialty_ortho, :specialty_pelvic,
             :cash_pay_signal, :cash_pay_keywords, :scraped_at, :website_checked_at)
        ON CONFLICT(name, address) DO UPDATE SET
            phone             = COALESCE(excluded.phone,    clinics.phone),
            website           = COALESCE(excluded.website,  clinics.website),
            listing_text      = COALESCE(excluded.listing_text, clinics.listing_text),
            website_text      = COALESCE(excluded.website_text, clinics.website_text),
            specialty_sports  = MAX(excluded.specialty_sports,  clinics.specialty_sports),
            specialty_ortho   = MAX(excluded.specialty_ortho,   clinics.specialty_ortho),
            specialty_pelvic  = MAX(excluded.specialty_pelvic,  clinics.specialty_pelvic),
            cash_pay_signal   = MAX(excluded.cash_pay_signal,   clinics.cash_pay_signal),
            cash_pay_keywords = COALESCE(excluded.cash_pay_keywords, clinics.cash_pay_keywords),
            scraped_at        = excluded.scraped_at,
            website_checked_at = COALESCE(excluded.website_checked_at, clinics.website_checked_at)
        """,
        {
            "name": data.get("name", ""),
            "address": data.get("address", ""),
            "city": data.get("city", ""),
            "state": data.get("state", ""),
            "zip_code": data.get("zip_code", ""),
            "phone": data.get("phone", ""),
            "website": data.get("website", ""),
            "source": data.get("source", ""),
            "source_url": data.get("source_url", ""),
            "listing_text": data.get("listing_text", ""),
            "website_text": data.get("website_text", ""),
            "specialty_sports": int(data.get("specialty_sports", False)),
            "specialty_ortho": int(data.get("specialty_ortho", False)),
            "specialty_pelvic": int(data.get("specialty_pelvic", False)),
            "cash_pay_signal": int(data.get("cash_pay_signal", False)),
            "cash_pay_keywords": json.dumps(data.get("cash_pay_keywords", [])),
            "scraped_at": datetime.utcnow().isoformat(),
            "website_checked_at": data.get("website_checked_at"),
        },
    )
    row_id = cur.lastrowid
    conn.commit()
    conn.close()
    return row_id


def start_run(city: str, source: str, db_path: str = DB_PATH) -> int:
    conn = get_conn(db_path)
    cur = conn.execute(
        "INSERT INTO scrape_runs (city, source) VALUES (?, ?)", (city, source)
    )
    run_id = cur.lastrowid
    conn.commit()
    conn.close()
    return run_id


def finish_run(run_id: int, found: int, status: str = "ok", db_path: str = DB_PATH):
    conn = get_conn(db_path)
    conn.execute(
        """UPDATE scrape_runs
           SET finished_at = datetime('now'), clinics_found = ?, status = ?
           WHERE id = ?""",
        (found, status, run_id),
    )
    conn.commit()
    conn.close()


def query_clinics(
    city: str = None,
    specialty: list = None,
    cash_pay_only: bool = False,
    hide_unverified: bool = False,
    hide_practitioners: bool = False,
    zip_code: str = None,
    search: str = None,
    min_staff: int = None,
    min_locations: int = None,
    limit: int = 500,
    offset: int = 0,
    db_path: str = DB_PATH,
) -> list:
    conn = get_conn(db_path)
    conditions = []
    params = []

    if city:
        conditions.append("city = ?")
        params.append(city)
    if cash_pay_only:
        conditions.append("cash_pay_signal = 1")
    if hide_unverified:
        conditions.append("NOT (source = 'npi_registry' AND (website IS NULL OR website = ''))")
    if hide_practitioners:
        conditions.append("is_practitioner = 0")
    if zip_code:
        conditions.append("zip_code LIKE ?")
        params.append(zip_code + "%")
    if specialty:
        spec_clauses = []
        for s in specialty:
            if s == "sports":
                spec_clauses.append("specialty_sports = 1")
            elif s == "ortho":
                spec_clauses.append("specialty_ortho = 1")
            elif s == "pelvic":
                spec_clauses.append("specialty_pelvic = 1")
        if spec_clauses:
            conditions.append("(" + " OR ".join(spec_clauses) + ")")
    if search:
        conditions.append("(name LIKE ? OR address LIKE ?)")
        params += [f"%{search}%", f"%{search}%"]
    if min_staff:
        conditions.append("staff_count >= ?")
        params.append(min_staff)
    if min_locations and min_locations > 1:
        conditions.append(
            "(website_domain IS NOT NULL AND website_domain != '' AND "
            "(SELECT COUNT(*) FROM clinics c2 WHERE c2.website_domain = clinics.website_domain) >= ?)"
        )
        params.append(min_locations)

    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    sql = f"""
        SELECT *,
          CASE WHEN (website_domain IS NOT NULL AND website_domain != '')
            THEN (SELECT COUNT(*) FROM clinics c2 WHERE c2.website_domain = clinics.website_domain)
            ELSE 1
          END AS location_count
        FROM clinics
        {where}
        ORDER BY cash_pay_signal DESC, name ASC
        LIMIT ? OFFSET ?
    """
    params += [limit, offset]
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def count_clinics(
    city: str = None,
    specialty: list = None,
    cash_pay_only: bool = False,
    hide_unverified: bool = False,
    hide_practitioners: bool = False,
    zip_code: str = None,
    search: str = None,
    min_staff: int = None,
    min_locations: int = None,
    db_path: str = DB_PATH,
) -> int:
    conn = get_conn(db_path)
    conditions = []
    params = []
    if city:
        conditions.append("city = ?")
        params.append(city)
    if cash_pay_only:
        conditions.append("cash_pay_signal = 1")
    if hide_unverified:
        conditions.append("NOT (source = 'npi_registry' AND (website IS NULL OR website = ''))")
    if hide_practitioners:
        conditions.append("is_practitioner = 0")
    if zip_code:
        conditions.append("zip_code LIKE ?")
        params.append(zip_code + "%")
    if specialty:
        spec_clauses = []
        for s in specialty:
            if s == "sports":
                spec_clauses.append("specialty_sports = 1")
            elif s == "ortho":
                spec_clauses.append("specialty_ortho = 1")
            elif s == "pelvic":
                spec_clauses.append("specialty_pelvic = 1")
        if spec_clauses:
            conditions.append("(" + " OR ".join(spec_clauses) + ")")
    if search:
        conditions.append("(name LIKE ? OR address LIKE ?)")
        params += [f"%{search}%", f"%{search}%"]
    if min_staff:
        conditions.append("staff_count >= ?")
        params.append(min_staff)
    if min_locations and min_locations > 1:
        conditions.append(
            "(website_domain IS NOT NULL AND website_domain != '' AND "
            "(SELECT COUNT(*) FROM clinics c2 WHERE c2.website_domain = clinics.website_domain) >= ?)"
        )
        params.append(min_locations)
    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    row = conn.execute(f"SELECT COUNT(*) FROM clinics {where}", params).fetchone()
    conn.close()
    return row[0]


def update_staff_count(clinic_id: int, count: int, names: list = None, db_path: str = DB_PATH):
    conn = get_conn(db_path)
    conn.execute(
        "UPDATE clinics SET staff_count = ?, staff_names = ? WHERE id = ?",
        (count, json.dumps(names or []), clinic_id),
    )
    conn.commit()
    conn.close()


def get_distinct_cities(db_path: str = DB_PATH) -> list:
    conn = get_conn(db_path)
    rows = conn.execute(
        "SELECT DISTINCT city FROM clinics WHERE city != '' ORDER BY city"
    ).fetchall()
    conn.close()
    return [r[0] for r in rows]
