"""
Estimates the number of PT staff listed on a clinic's website.

Strategy:
1. Fetch the homepage and check for team content directly.
2. Find links to team/staff pages (up to 3) and check those too.
3. Run two counting strategies on each page and take the highest result:
   - Card counting: find team-section containers and count card-like children.
   - Name heading counting: find h2–h5 text that looks like a person's name.
"""

import logging
import re
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from scrapers.base import fetch_html, polite_sleep

logger = logging.getLogger(__name__)

# Matches href paths or link text suggesting a team/staff page
_TEAM_HREF_RE = re.compile(
    r"team|staff|therapist|provider|clinician|practitioner|meet-|meet_|"
    r"about[/-]team|about[/-]staff|our-people|who-we-are",
    re.I,
)
_TEAM_TEXT_RE = re.compile(
    r"our team|our staff|meet our|our therapist|our provider|the team|"
    r"clinical staff|meet the team|our clinicians|our practitioners|who we are",
    re.I,
)

# Credentials stripped before testing whether a heading is a person's name
_CREDENTIALS_RE = re.compile(
    r",?\s*\b("
    r"DPT|PT|MPT|OCS|CSCS|FAAOMPT|ATC|SCS|COMT|PhD|ScD|EdD|MS|MA|BS|BA|MSPT|"
    r"Cert\.?\s*MDT|PCS|NCS|GCS|CWS|CLT|CEES|ASTYM|LSVT|CAFS|PRPC|WCS"
    r")[\w\s,\.]*",
    re.I,
)
_TITLE_PREFIX_RE = re.compile(r"^(Dr\.?|Mr\.?|Ms\.?|Mrs\.?|Prof\.?)\s+", re.I)

# Section-level headings that are titles, not names
_GENERIC_HEADINGS = {
    "meet our team", "our team", "our staff", "our therapists",
    "meet the team", "our providers", "clinical staff", "meet our providers",
    "the team", "staff", "team", "our clinicians", "therapists", "providers",
    "meet our therapists", "our clinical team", "physical therapists",
    "physical therapy staff", "our physical therapists", "treatment team",
    "care team", "healthcare team", "rehabilitation team", "about us",
    "about our team", "meet the staff", "our specialists",
}

# Class/id fragments that suggest a team container
_TEAM_CONTAINER_KW = (
    "team", "staff", "therapist", "provider", "clinician",
    "practitioner", "bio", "people", "member", "doctor", "specialist",
)


def _looks_like_name(text: str) -> bool:
    """Return True if text plausibly represents a person's name."""
    text = text.strip()
    if not text or text.lower() in _GENERIC_HEADINGS:
        return False
    text = _CREDENTIALS_RE.sub("", text)
    text = _TITLE_PREFIX_RE.sub("", text)
    text = text.strip().rstrip(",").strip()
    words = text.split()
    if not 2 <= len(words) <= 5:
        return False
    for word in words:
        # Allow hyphenated and apostrophe names (O'Brien, Smith-Jones)
        core = re.sub(r"^[-']|[-']$", "", word)
        if not core or not core[0].isupper():
            return False
    return True


def _find_team_links(html: str, base_url: str) -> list[str]:
    """Return up to 3 internal links that likely lead to a team/staff page."""
    bs = BeautifulSoup(html, "lxml")
    base_domain = urlparse(base_url).netloc
    seen: set[str] = set()
    links: list[str] = []

    for a in bs.find_all("a", href=True):
        href = a["href"].strip()
        text = a.get_text(" ", strip=True)
        parsed = urlparse(urljoin(base_url, href))
        # Internal links only
        if parsed.netloc and parsed.netloc != base_domain:
            continue
        full_url = urljoin(base_url, href).split("#")[0]  # drop anchors
        if not full_url.startswith("http") or full_url in seen or full_url == base_url:
            continue
        if _TEAM_HREF_RE.search(href) or _TEAM_TEXT_RE.search(text):
            seen.add(full_url)
            links.append(full_url)
        if len(links) >= 3:
            break

    return links


def _count_staff_in_html(html: str) -> tuple[int, list[str]]:
    """
    Count probable staff members on a page using two independent strategies.
    Returns (count, names) where count is the highest plausible result and
    names is the list of detected name strings (may be empty if only card
    counting succeeded).
    """
    bs = BeautifulSoup(html, "lxml")
    counts: list[int] = []
    all_names: list[str] = []

    # --- Strategy 1: team container → count card-like direct children ---
    def _is_team_container(tag) -> bool:
        if tag.name not in ("div", "section", "ul", "ol"):
            return False
        classes = " ".join(tag.get("class", [])).lower()
        tag_id = (tag.get("id") or "").lower()
        return any(kw in classes or kw in tag_id for kw in _TEAM_CONTAINER_KW)

    for container in bs.find_all(_is_team_container):
        card_children = [
            c for c in container.children
            if getattr(c, "name", None) in ("div", "article", "li", "section")
        ]
        if len(card_children) >= 2:
            counts.append(len(card_children))

    # --- Strategy 2: name-like headings anywhere on the page ---
    name_headings: list[str] = []
    seen_lower: set[str] = set()
    for tag in bs.find_all(["h2", "h3", "h4", "h5"]):
        text = tag.get_text(" ", strip=True)
        if _looks_like_name(text) and text.lower() not in seen_lower:
            seen_lower.add(text.lower())
            name_headings.append(text)
    if len(name_headings) >= 2:
        counts.append(len(name_headings))
        all_names = name_headings

    # --- Strategy 3: name-like headings scoped to team containers ---
    for container in bs.find_all(_is_team_container):
        scoped: list[str] = []
        scoped_lower: set[str] = set()
        for tag in container.find_all(["h2", "h3", "h4", "h5", "strong"]):
            text = tag.get_text(" ", strip=True)
            if _looks_like_name(text) and text.lower() not in scoped_lower:
                scoped_lower.add(text.lower())
                scoped.append(text)
        if scoped:
            counts.append(len(scoped))
            if len(scoped) > len(all_names):
                all_names = scoped

    best = max(counts) if counts else 0
    return best, all_names


def scrape_staff_count(website_url: str) -> tuple[int | None, list[str]]:
    """
    Fetch a clinic website and estimate the number of PT staff listed.
    Returns (count, names):
      - count: integer (0 = site reachable but no staff found) or None if site unreachable
      - names: list of detected staff name strings (may be empty)
    """
    if not website_url:
        return None, []
    if not website_url.startswith("http"):
        website_url = "https://" + website_url

    try:
        html = fetch_html(website_url)
        if not html:
            return None, []

        best, best_names = _count_staff_in_html(html)

        for team_url in _find_team_links(html, website_url):
            polite_sleep()
            page_html = fetch_html(team_url)
            if page_html:
                count, names = _count_staff_in_html(page_html)
                if count > best:
                    best = count
                    best_names = names

        return best, best_names

    except Exception as e:
        logger.warning(f"[staff] {website_url}: {e}")
        return None, []
