"""
Flask web application for browsing and exporting PT clinic data.
"""

import csv
import io
import json
import sys
import os
import subprocess
import threading
import logging

from flask import (
    Flask,
    render_template,
    request,
    jsonify,
    Response,
    session,
    stream_with_context,
)

# Allow imports from parent directory
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from database import (
    init_db,
    query_clinics,
    count_clinics,
    get_distinct_cities,
    get_conn,
    DB_PATH,
)
from config import CITIES

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", "pt-pipeline-secret-2024")
logger = logging.getLogger(__name__)

# Track running scrape jobs
_scrape_lock = threading.Lock()
_scrape_running = False
_scrape_log: list[str] = []


# ---------------------------------------------------------------------------
# Pages
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    init_db()
    cities = get_distinct_cities()
    return render_template("index.html", cities=cities, city_configs=CITIES)


# ---------------------------------------------------------------------------
# API: aggregate stats for the stats bar
# ---------------------------------------------------------------------------

@app.route("/api/stats")
def api_stats():
    city = request.args.get("city", "") or None
    conn = get_conn()
    where = "WHERE city = ?" if city else ""
    params = [city] if city else []
    row = conn.execute(f"""
        SELECT
            COUNT(*)                                  AS total,
            SUM(cash_pay_signal)                      AS cash_pay,
            SUM(specialty_sports)                     AS sports,
            SUM(specialty_ortho)                      AS ortho,
            SUM(specialty_pelvic)                     AS pelvic,
            COUNT(DISTINCT city)                      AS markets
        FROM clinics {where}
    """, params).fetchone()
    conn.close()
    return jsonify({
        "total":    row["total"]    or 0,
        "cash_pay": row["cash_pay"] or 0,
        "sports":   row["sports"]   or 0,
        "ortho":    row["ortho"]    or 0,
        "pelvic":   row["pelvic"]   or 0,
        "markets":  row["markets"]  or 0,
    })


# ---------------------------------------------------------------------------
# API: list clinics (JSON)
# ---------------------------------------------------------------------------

@app.route("/api/clinics")
def api_clinics():
    city = request.args.get("city", "")
    cash_pay = request.args.get("cash_pay", "") == "1"
    hide_unverified = request.args.get("hide_unverified", "") == "1"
    hide_practitioners = request.args.get("hide_practitioners", "") == "1"
    zip_code = request.args.get("zip", "").strip()
    specialties = request.args.getlist("specialty")
    search = request.args.get("search", "")
    min_staff = int(request.args.get("min_staff", 0) or 0)
    min_locations = int(request.args.get("min_locations", 0) or 0)
    page = max(1, int(request.args.get("page", 1)))
    per_page = min(100, int(request.args.get("per_page", 50)))
    offset = (page - 1) * per_page

    clinics = query_clinics(
        city=city or None,
        specialty=specialties or None,
        cash_pay_only=cash_pay,
        hide_unverified=hide_unverified,
        hide_practitioners=hide_practitioners,
        zip_code=zip_code or None,
        search=search or None,
        min_staff=min_staff or None,
        min_locations=min_locations or None,
        limit=per_page,
        offset=offset,
    )
    total = count_clinics(
        city=city or None,
        specialty=specialties or None,
        cash_pay_only=cash_pay,
        hide_unverified=hide_unverified,
        hide_practitioners=hide_practitioners,
        zip_code=zip_code or None,
        search=search or None,
        min_staff=min_staff or None,
        min_locations=min_locations or None,
    )

    # Parse cash_pay_keywords JSON for display and drop heavy website_text
    for c in clinics:
        try:
            c["cash_pay_keywords"] = json.loads(c.get("cash_pay_keywords") or "[]")
        except Exception:
            c["cash_pay_keywords"] = []
        c.pop("website_text", None)
        c.pop("listing_text", None)

    return jsonify({"clinics": clinics, "total": total, "page": page, "per_page": per_page})


# ---------------------------------------------------------------------------
# API: city list
# ---------------------------------------------------------------------------

@app.route("/api/cities")
def api_cities():
    cities = get_distinct_cities()
    return jsonify(cities)


# ---------------------------------------------------------------------------
# CSV export
# ---------------------------------------------------------------------------

@app.route("/api/export.csv")
def export_csv():
    city = request.args.get("city", "")
    cash_pay = request.args.get("cash_pay", "") == "1"
    hide_unverified = request.args.get("hide_unverified", "") == "1"
    hide_practitioners = request.args.get("hide_practitioners", "") == "1"
    zip_code = request.args.get("zip", "").strip()
    specialties = request.args.getlist("specialty")
    search = request.args.get("search", "")
    min_staff = int(request.args.get("min_staff", 0) or 0)
    min_locations = int(request.args.get("min_locations", 0) or 0)

    clinics = query_clinics(
        city=city or None,
        specialty=specialties or None,
        cash_pay_only=cash_pay,
        hide_unverified=hide_unverified,
        hide_practitioners=hide_practitioners,
        zip_code=zip_code or None,
        search=search or None,
        min_staff=min_staff or None,
        min_locations=min_locations or None,
        limit=10_000,
        offset=0,
    )

    fieldnames = [
        "id", "name", "address", "city", "state", "zip_code",
        "phone", "website", "source", "specialty",
        "cash_pay_signal", "cash_pay_keywords", "staff_count",
        "primary_contact_name", "primary_contact_email", "primary_contact_linkedin",
        "scraped_at",
    ]

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for c in clinics:
        spec_parts = []
        if c.get("specialty_sports"): spec_parts.append("Sports")
        if c.get("specialty_ortho"):  spec_parts.append("Ortho")
        if c.get("specialty_pelvic"): spec_parts.append("Pelvic")
        c["specialty"] = " / ".join(spec_parts)
        try:
            c["cash_pay_keywords"] = ", ".join(
                json.loads(c.get("cash_pay_keywords") or "[]")
            )
        except Exception:
            c["cash_pay_keywords"] = ""
        c["primary_contact_name"]     = c.get("primary_staff_name") or ""
        c["primary_contact_email"]    = c.get("primary_staff_email") or ""
        c["primary_contact_linkedin"] = c.get("primary_staff_linkedin") or ""
        writer.writerow(c)

    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=pt_clinics.csv"},
    )


# ---------------------------------------------------------------------------
# Scraper control
# ---------------------------------------------------------------------------

@app.route("/api/scrape", methods=["POST"])
def api_scrape():
    global _scrape_running, _scrape_log

    body = request.get_json(silent=True) or {}
    city = body.get("city", "boston")
    sources = body.get("sources", ["google_maps", "choosept", "therapyfinder"])
    scan_websites = body.get("scan_websites", False)

    if city not in CITIES and city != "all":
        return jsonify({"error": f"Unknown city: {city}"}), 400

    with _scrape_lock:
        if _scrape_running:
            return jsonify({"error": "A scrape is already running"}), 409
        _scrape_running = True
        _scrape_log = []

    def run():
        global _scrape_running, _scrape_log
        try:
            cmd = [
                sys.executable,
                os.path.join(os.path.dirname(os.path.dirname(__file__)), "run_scraper.py"),
            ]
            if city == "all":
                cmd += ["--all"]
            else:
                cmd += ["--city", city]
            cmd += ["--sources"] + sources
            if scan_websites:
                cmd += ["--scan-websites"]

            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                cwd=os.path.dirname(os.path.dirname(__file__)),
            )
            for line in proc.stdout:
                line = line.rstrip()
                _scrape_log.append(line)
                logger.info(f"[scraper] {line}")
            proc.wait()
        except Exception as e:
            _scrape_log.append(f"ERROR: {e}")
        finally:
            _scrape_running = False

    t = threading.Thread(target=run, daemon=True)
    t.start()
    return jsonify({"status": "started", "city": city})


@app.route("/api/scrape/status")
def api_scrape_status():
    return jsonify({
        "running": _scrape_running,
        "log": _scrape_log[-100:],  # last 100 lines
    })


# ---------------------------------------------------------------------------
# API: single clinic detail
# ---------------------------------------------------------------------------

@app.route("/api/clinics/<int:clinic_id>")
def api_clinic_detail(clinic_id):
    conn = get_conn()
    row = conn.execute("""
        SELECT *,
          CASE WHEN (website_domain IS NOT NULL AND website_domain != '')
            THEN (SELECT COUNT(*) FROM clinics c2 WHERE c2.website_domain = clinics.website_domain)
            ELSE 1
          END AS location_count
        FROM clinics WHERE id = ?
    """, (clinic_id,)).fetchone()
    conn.close()
    if not row:
        return jsonify({"error": "Not found"}), 404
    c = dict(row)
    try:
        c["cash_pay_keywords"] = json.loads(c.get("cash_pay_keywords") or "[]")
    except Exception:
        c["cash_pay_keywords"] = []
    c.pop("website_text", None)   # too large; not needed for detail view
    return jsonify(c)


# ---------------------------------------------------------------------------
# Shortlist (session-backed)
# ---------------------------------------------------------------------------

def _shortlist_ids() -> list[int]:
    return session.get("shortlist", [])


def _fetch_clinics_by_ids(ids: list[int]) -> list[dict]:
    if not ids:
        return []
    conn = get_conn()
    placeholders = ",".join("?" * len(ids))
    rows = conn.execute(
        f"SELECT * FROM clinics WHERE id IN ({placeholders})", ids
    ).fetchall()
    conn.close()
    clinics = [dict(r) for r in rows]
    for c in clinics:
        try:
            c["cash_pay_keywords"] = json.loads(c.get("cash_pay_keywords") or "[]")
        except Exception:
            c["cash_pay_keywords"] = []
        c.pop("website_text", None)
        c.pop("listing_text", None)
    return clinics


@app.route("/api/shortlist", methods=["GET"])
def api_shortlist_get():
    ids = _shortlist_ids()
    clinics = _fetch_clinics_by_ids(ids)
    return jsonify({"clinics": clinics, "ids": ids})


@app.route("/api/shortlist", methods=["POST"])
def api_shortlist_add():
    body = request.get_json(silent=True) or {}
    clinic_id = body.get("id")
    if not isinstance(clinic_id, int):
        return jsonify({"error": "Missing or invalid id"}), 400
    shortlist = _shortlist_ids()
    if clinic_id not in shortlist:
        shortlist.append(clinic_id)
        session["shortlist"] = shortlist
        session.modified = True
    return jsonify({"ids": shortlist})


@app.route("/api/shortlist/<int:clinic_id>", methods=["DELETE"])
def api_shortlist_remove(clinic_id):
    shortlist = [i for i in _shortlist_ids() if i != clinic_id]
    session["shortlist"] = shortlist
    session.modified = True
    return jsonify({"ids": shortlist})


@app.route("/api/shortlist/export.csv")
def shortlist_export_csv():
    ids = _shortlist_ids()
    clinics = _fetch_clinics_by_ids(ids)

    fieldnames = [
        "id", "name", "address", "city", "state", "zip_code",
        "phone", "website", "source", "specialty",
        "cash_pay_signal", "cash_pay_keywords", "staff_count",
        "primary_contact_name", "primary_contact_email", "primary_contact_linkedin",
        "scraped_at",
    ]

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for c in clinics:
        spec_parts = []
        if c.get("specialty_sports"): spec_parts.append("Sports")
        if c.get("specialty_ortho"):  spec_parts.append("Ortho")
        if c.get("specialty_pelvic"): spec_parts.append("Pelvic")
        c["specialty"] = " / ".join(spec_parts)
        kws = c.get("cash_pay_keywords", [])
        c["cash_pay_keywords"] = ", ".join(kws) if isinstance(kws, list) else ""
        c["primary_contact_name"]     = c.get("primary_staff_name") or ""
        c["primary_contact_email"]    = c.get("primary_staff_email") or ""
        c["primary_contact_linkedin"] = c.get("primary_staff_linkedin") or ""
        writer.writerow(c)

    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=pt_shortlist.csv"},
    )


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    init_db()
    app.run(debug=True, port=5050, use_reloader=False)
