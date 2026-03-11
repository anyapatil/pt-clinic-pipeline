"""
Base scraper utilities shared by all source scrapers.
"""

import time
import random
import logging
import requests
from bs4 import BeautifulSoup
from config import REQUEST_TIMEOUT, INTER_REQUEST_DELAY

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


def polite_sleep():
    lo, hi = INTER_REQUEST_DELAY
    time.sleep(random.uniform(lo, hi))


def fetch_html(url: str, session: requests.Session = None, timeout: int = REQUEST_TIMEOUT) -> str:
    """
    Fetch a URL and return the response text.
    Returns empty string on any error.
    """
    try:
        requester = session or requests
        resp = requester.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        logger.warning(f"fetch_html failed for {url}: {e}")
        return ""


def soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "lxml")


def clean(text: str) -> str:
    if not text:
        return ""
    return " ".join(text.split())


def fetch_website_text(url: str, session: requests.Session = None) -> str:
    """
    Fetch a clinic's own website, strip tags, return plain text (first 10 000 chars).
    """
    if not url:
        return ""
    if not url.startswith("http"):
        url = "https://" + url
    html = fetch_html(url, session=session, timeout=REQUEST_TIMEOUT)
    if not html:
        return ""
    try:
        s = soup(html)
        for tag in s(["script", "style", "noscript", "head"]):
            tag.decompose()
        text = s.get_text(separator=" ", strip=True)
        return text[:10_000]
    except Exception as e:
        logger.warning(f"website text extraction failed for {url}: {e}")
        return ""
