#!/usr/bin/env python3
"""
CLI entry point for the PT clinic scraper pipeline.

Usage:
  python run_scraper.py --city boston
  python run_scraper.py --city boston --sources choosept therapyfinder
  python run_scraper.py --city boston --scan-websites
  python run_scraper.py --all
  python run_scraper.py --all --sources choosept
"""

import argparse
import logging
import sys
from pipeline import run_city, run_all
from config import CITIES

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("scraper.log"),
    ],
)


def main():
    parser = argparse.ArgumentParser(description="PT Clinic Cash-Pay Scraper")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--city",
        choices=list(CITIES.keys()),
        help="City key to scrape (start with 'boston')",
    )
    group.add_argument(
        "--all",
        action="store_true",
        help="Scrape all configured cities",
    )
    parser.add_argument(
        "--sources",
        nargs="+",
        choices=["google_maps", "choosept", "therapyfinder"],
        default=["google_maps", "choosept", "therapyfinder"],
        help="Which sources to scrape (default: all three)",
    )
    parser.add_argument(
        "--scan-websites",
        action="store_true",
        default=False,
        help="Visit each clinic's website to look for cash-pay signals (slower)",
    )
    parser.add_argument(
        "--db",
        default="pt_clinics.db",
        help="SQLite database file path (default: pt_clinics.db)",
    )

    args = parser.parse_args()

    if args.city:
        run_city(
            args.city,
            scan_websites=args.scan_websites,
            sources=args.sources,
            db_path=args.db,
        )
    else:
        run_all(
            scan_websites=args.scan_websites,
            sources=args.sources,
            db_path=args.db,
        )


if __name__ == "__main__":
    main()
