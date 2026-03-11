"""
Cash-pay signal detection and specialty tagging.
Operates on free-text from listing descriptions and/or scraped website content.
"""

import re
from typing import Tuple
from config import CASH_PAY_KEYWORDS, SPORTS_KEYWORDS, ORTHO_KEYWORDS, PELVIC_KEYWORDS


def _normalize(text: str) -> str:
    if not text:
        return ""
    text = text.lower()
    # collapse whitespace and strip HTML entities
    text = re.sub(r"\s+", " ", text)
    return text


def detect_cash_pay(texts: list[str]) -> Tuple[bool, list[str]]:
    """
    Scan one or more text blobs for cash-pay signals.
    Returns (signal_found, list_of_matched_keywords).
    """
    combined = _normalize(" ".join(t for t in texts if t))
    matched = []
    for kw in CASH_PAY_KEYWORDS:
        if kw in combined:
            matched.append(kw)
    return bool(matched), matched


def detect_specialties(texts: list[str]) -> dict:
    """
    Returns dict with keys: sports, ortho, pelvic (bool each).
    """
    combined = _normalize(" ".join(t for t in texts if t))
    return {
        "sports": any(kw in combined for kw in SPORTS_KEYWORDS),
        "ortho": any(kw in combined for kw in ORTHO_KEYWORDS),
        "pelvic": any(kw in combined for kw in PELVIC_KEYWORDS),
    }


def analyze_clinic(listing_text: str = "", website_text: str = "") -> dict:
    """
    Run all signal detection on a clinic's available text.
    Returns a dict ready to merge into the clinic record.
    """
    texts = [listing_text, website_text]
    cash_pay, keywords = detect_cash_pay(texts)
    specs = detect_specialties(texts)
    return {
        "cash_pay_signal": cash_pay,
        "cash_pay_keywords": keywords,
        "specialty_sports": specs["sports"],
        "specialty_ortho": specs["ortho"],
        "specialty_pelvic": specs["pelvic"],
    }
