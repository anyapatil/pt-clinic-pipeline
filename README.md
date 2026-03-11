# PT Clinic Acquisition Pipeline

A data pipeline and web application for identifying cash-pay / out-of-network physical therapy clinics across five US markets. Built as a deal-sourcing tool for evaluating PT clinic acquisition targets.

## What It Does

1. **Scrapes** PT clinic listings from three sources: Google Maps, APTA's ChoosePT directory, and the CMS NPI Registry
2. **Detects signals** indicating a cash-pay or out-of-network practice by scanning listing text and clinic websites for keywords (e.g. "direct access", "cash based", "1:1 care", "out of network")
3. **Tags specialties** — Sports/Athletic, Orthopedic, and Pelvic Health — based on language found in listings and websites
4. **Estimates staff size** by scraping each clinic's website for team/staff pages and counting listed practitioners
5. **Stores everything** in a local SQLite database with deduplication across sources
6. **Serves a web UI** for filtering, browsing, and exporting the data

### Markets Covered

| Key | Market |
|-----|--------|
| `boston` | Boston, MA |
| `dc` | Washington, DC / DMV |
| `minneapolis` | Minneapolis, MN |
| `denver` | Denver / Boulder, CO |
| `bay_area` | Bay Area, CA |

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Scraping (dynamic) | [Playwright](https://playwright.dev/python/) — headless Chromium for Google Maps |
| Scraping (static) | [Requests](https://requests.readthedocs.io/) + [BeautifulSoup4](https://www.crummy.com/software/BeautifulSoup/) |
| NPI data | [CMS NPI Registry API](https://npiregistry.cms.hhs.gov/api/) — free, no key required |
| Database | SQLite (via Python `sqlite3`) |
| Web framework | [Flask](https://flask.palletsprojects.com/) |
| Frontend | Vanilla JS + CSS (no framework); Cormorant Garamond + Inter via Google Fonts |

---

## Data Sources

### Google Maps
Searches Google Maps for PT-related queries in each market. Phase 1 collects place URLs from the results feed; Phase 2 visits each place page to extract structured name, address, phone, and website data. Capped at 40 results per search query.

### APTA ChoosePT (`choosept.com`)
Scrapes the APTA's physical therapist finder. Paginates through results for each city using lat/lon coordinates. Extracts clinic name, address, phone, and external website links.

### CMS NPI Registry
Queries the free CMS NPI Registry API (`npiregistry.cms.hhs.gov/api`) for physical therapy organizations (NPI-2 entity type) by city and state. No API key required. Returns structured name, address, and phone data. Used as a high-coverage supplemental source.

> **Note:** `cash_pay_signal` is a language-based flag only. It indicates that cash-pay or out-of-network language was detected in the clinic's listing or website — it does not verify insurance status.

---

## Setup

### 1. Clone and create a virtual environment

```bash
git clone <repo-url>
cd pt_scraper
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Install Playwright browsers

```bash
playwright install chromium
```

### 3. Initialize the database

The database is created automatically on first run. To initialize manually:

```bash
python -c "from database import init_db; init_db()"
```

---

## Running the Scraper

```bash
# Scrape a single market
python run_scraper.py --city boston

# Scrape all five markets
python run_scraper.py --all

# Scrape specific sources only
python run_scraper.py --city dc --sources choosept therapyfinder

# Also scan clinic websites for cash-pay signals (slower, more thorough)
python run_scraper.py --city boston --scan-websites
```

**Available `--sources`:** `google_maps`, `choosept`, `therapyfinder` (includes NPI Registry)

---

## Populating Staff Counts

After scraping, run the staff enrichment pass to estimate the number of PT staff listed on each cash-pay clinic's website:

```bash
# Scan all eligible clinics (cash-pay flagged, have a website)
python scan_staff.py

# Limit to one market
python scan_staff.py --city Boston

# Re-scan already-scanned clinics to refresh counts
python scan_staff.py --force

# Cap the number of clinics scanned
python scan_staff.py --limit 50
```

The scraper finds team/staff pages by following internal links that mention "team", "staff", "therapist", etc., then counts practitioner names and card-like elements on those pages.

---

## Web UI

```bash
python run_web.py
# → http://127.0.0.1:5050
```

### Features

- **Filter** by market, specialty (Sports / Ortho / Pelvic), cash-pay signal, and free-text search
- **Hide unverified listings** — toggle to exclude NPI Registry entries with no website
- **Clinic cards** — click any card to open a detail modal with full contact info, staff count, detected signal keywords, and listing excerpt
- **Shortlist** — save clinics to a session-backed shortlist accessible via the Shortlist tab
- **CSV export** — export the current filtered view or your shortlist to CSV, including `staff_count`

---

## Project Structure

```
pt_scraper/
├── config.py               # City configs, keyword lists, scraper settings
├── database.py             # SQLite schema, upsert, query helpers
├── signals.py              # Cash-pay and specialty keyword detection
├── pipeline.py             # Orchestrates scraper → signals → DB
├── run_scraper.py          # CLI entry point for scraping
├── scan_staff.py           # Staff count enrichment script
├── scrapers/
│   ├── base.py             # Shared HTTP helpers, polite_sleep
│   ├── google_maps.py      # Playwright-based Google Maps scraper
│   ├── choosept.py         # ChoosePT HTML scraper
│   ├── therapyfinder.py    # NPI Registry API client
│   └── staff_scraper.py    # Website staff count estimator
└── web/
    ├── app.py              # Flask app and API endpoints
    ├── run_web.py          # Web server entry point
    └── templates/
        └── index.html      # Single-page UI
```

---

## Database Schema

The `clinics` table (SQLite, `pt_clinics.db`):

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER | Primary key |
| `name` | TEXT | Clinic or practitioner name |
| `address` | TEXT | Full address |
| `city` | TEXT | City label (e.g. `Boston`) |
| `state` | TEXT | State abbreviation |
| `zip_code` | TEXT | 5-digit ZIP code |
| `phone` | TEXT | Phone number |
| `website` | TEXT | Clinic website URL |
| `source` | TEXT | `google_maps`, `choosept`, or `npi_registry` |
| `source_url` | TEXT | Link to the original listing |
| `listing_text` | TEXT | Raw text from the listing |
| `specialty_sports` | INTEGER | 1 if sports/athletic language detected |
| `specialty_ortho` | INTEGER | 1 if orthopedic language detected |
| `specialty_pelvic` | INTEGER | 1 if pelvic health language detected |
| `cash_pay_signal` | INTEGER | 1 if cash-pay language detected |
| `cash_pay_keywords` | TEXT | JSON array of matched keywords |
| `staff_count` | INTEGER | Estimated staff from website (NULL = not scanned) |
| `scraped_at` | TEXT | ISO timestamp of last scrape |

Deduplication is enforced by a `UNIQUE(name, address)` constraint with `ON CONFLICT DO UPDATE` — signals from multiple sources are OR-merged, never overwritten.

---

## Configuration

Edit `config.py` to:
- Add new cities (requires `label`, `state`, `search_terms`, `choosept_lat/lng`, and optionally `npi_city`)
- Adjust keyword lists for cash-pay, sports, ortho, or pelvic detection
- Tune scraper settings (`MAPS_MAX_RESULTS`, `INTER_REQUEST_DELAY`, etc.)
