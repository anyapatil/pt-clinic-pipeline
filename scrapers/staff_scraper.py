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

# Credentials stripped before testing whether a heading is a person's name.
# NOTE: case-sensitive — credentials are written uppercase; using re.I would
# cause "Ma" in "Major" or "Ms" in "Miss" to be consumed as a credential.
_CREDENTIALS_RE = re.compile(
    r",?\s*\b("
    r"DPT|PT|MPT|OCS|CSCS|FAAOMPT|ATC|SCS|COMT|PhD|ScD|EdD|MS|MA|BS|BA|MSPT|"
    r"OTR|CHT|PTA|COTA|"
    r"Cert\.?\s*MDT|PCS|NCS|GCS|CWS|CLT|CEES|ASTYM|LSVT|CAFS|PRPC|WCS"
    r")[\w\s,\.]*",
)
_TITLE_PREFIX_RE = re.compile(r"^(Dr\.?|Mr\.?|Ms\.?|Mrs\.?|Prof\.?)\s+", re.I)

# Detects a credential suffix in the original text (positive signal — not for stripping)
_HAS_CREDENTIAL_RE = re.compile(
    r"\b(DPT|MPT|MSPT|OCS|CSCS|FAAOMPT|ATC|SCS|COMT|OTR|CHT|PTA|COTA|LMT|"
    r"PhD|ScD|EdD|PRPC|WCS|CLT|LSVT|ASTYM|CAFS)\b"
    r"|(?<!\w)PT(?!\w)",  # PT as a standalone token
    re.I,
)

# Section-level headings that are titles, not names (exact full-text match)
_GENERIC_HEADINGS = {
    "meet our team", "our team", "our staff", "our therapists",
    "meet the team", "our providers", "clinical staff", "meet our providers",
    "the team", "staff", "team", "our clinicians", "therapists", "providers",
    "meet our therapists", "our clinical team", "physical therapists",
    "physical therapy staff", "our physical therapists", "treatment team",
    "care team", "healthcare team", "rehabilitation team", "about us",
    "about our team", "meet the staff", "our specialists",
}

# Words that appear in navigation/content headings but never in human names.
# Any entry whose cleaned text contains one of these words is rejected.
_GENERIC_WORDS = frozenset({
    # Articles, pronouns, conjunctions — appear in headings, never in names
    "the", "our", "your", "my", "we", "us", "you", "they", "them", "their",
    "all", "and", "or", "but", "in", "on", "at", "to", "for", "of", "by",
    "this", "that", "these", "those", "it", "its", "an", "a",
    # Common heading verbs
    "do", "get", "find", "learn", "see", "start", "join", "call", "ask",
    "visit", "meet", "know", "try", "make", "feel", "choose", "prevent",
    "improve", "help", "explore", "discover", "understand", "achieve",
    # Common adjectives in content headings
    "better", "faster", "first", "new", "more", "less", "best", "great",
    "different", "special", "free", "easy", "simple", "full", "next",
    # Common content nouns (non-name)
    "practice", "process", "approach", "values", "mission", "vision",
    "goal", "goals", "benefit", "benefits", "result", "results",
    "story", "stories", "success", "difference", "experience", "why", "how", "what",
    # Body parts and condition terms
    "pain", "neck", "back", "shoulder", "knee", "hip", "ankle", "wrist",
    "elbow", "motor", "vehicle", "accident", "injury", "injuries",
    "condition", "conditions", "syndrome", "chronic", "acute",
    # Event / workshop terms
    "workshop", "workshops", "seminar", "seminars", "webinar", "webinars",
    "event", "events", "engagement", "engagements", "conference",
    "scholars", "prizes", "awards",
    # Research / academic terms
    "trial", "trials", "platform", "innovation", "research", "study",
    "studies", "initiative", "als", "continuing", "education", "courses",
    "collaborators", "researcher", "visiting", "inside",
    # Technique / service terms
    "technique", "techniques", "method", "methods", "exercise", "exercises",
    "equipment", "evaluation", "evaluations", "assessment", "assessments",
    "release", "active", "graston", "empowering", "personalized",
    "attention", "movement", "comprehensive", "referral", "referrals",
    # Navigation / link items
    "links", "access", "handbook", "opportunities", "employment",
    "advisory", "newsletter", "systems", "finance", "business", "employee",
    "departments", "clinical", "helpful",
    # Condition / specialty terms
    "cancer", "recovery", "prevention", "acl", "dance",
    "plantar", "fasciitis", "headaches", "cervicogenic",
    # Compass directions (location identifiers, not surnames)
    "east", "west", "north", "south", "island",
    # Quality / ratings language
    "overall", "quality", "rating", "recommend", "functional", "retraining",
    "would",
    # Misc non-name terms seen in scrape output
    "station", "open", "position", "brand", "guidelines", "reserve",
    "helps", "with", "travelers", "international",
    "extensive", "specialized", "knowledge", "walk", "most", "managed",
    "frequently", "privacy", "follow", "accepting", "major",
    "premier", "performance", "peoplefit",
    "send", "message", "up", "in", "out", "visits", "visit",
    # Additional false positives
    "certified", "specialists", "residency", "trained", "lymphatic",
    "drainage", "athletic", "training", "sitemap", "wharf",
    "registration", "forms", "brazilian", "integrated", "advanced",
    "licensed", "registered", "board", "fellowship",
    "manual", "therapy", "therapies", "modalities",
    "sauna", "infrared", "isokinetic", "pilates", "nurse",
    "practitioner", "executive", "interactive", "testing",
    "transparent", "rates", "technology", "light",
    # Original list
    "hours", "address", "location", "locations", "contact", "office",
    "services", "service", "schedule", "appointment", "appointments",
    "directions", "about", "staff", "team", "physical", "therapy",
    "therapist", "therapists", "center", "clinic", "health", "care",
    "medical", "rehabilitation", "rehab", "patient", "patients",
    "information", "phone", "fax", "email", "website", "menu",
    "search", "home", "specialties", "specialty", "treatment", "treatments",
    "welcome", "insurance", "billing", "payment", "suite", "floor",
    "building", "support", "resources", "blog", "news", "gallery",
    "reviews", "testimonials", "map", "parking", "navigation",
    "donate", "donor", "blood", "healthy", "living", "wellness",
    "community", "program", "programs", "class", "classes", "group",
    "hospital", "foundation", "institute", "network", "associates",
    "association", "partners", "partner", "university", "college",
    "surgery", "surgical", "orthopedic", "orthopaedic", "sports",
    "pelvic", "pediatric", "geriatric", "neurology", "cardiology",
    "diagnostic", "imaging", "laboratory", "pharmacy",
})

# Class/id fragments that suggest a team container
_TEAM_CONTAINER_KW = (
    "team", "staff", "therapist", "provider", "clinician",
    "practitioner", "bio", "people", "member", "doctor", "specialist",
)


def _looks_like_name(text: str) -> bool:
    """
    Return True if text plausibly represents a human staff member's name.

    A valid entry must:
    - Not be a known generic section heading
    - Contain no generic non-name words (hours, address, services, etc.)
    - Have 2–5 words after credential stripping, each starting uppercase
    - Each word must be name-safe characters: letters, apostrophes, hyphens,
      optional trailing period (for middle initials like "K.")
    - Either carry a credential suffix (PT, DPT, MPT, OTR, CHT, …)
      OR consist of at most 3 words (first + optional middle + last)
    """
    text = text.strip()
    if not text or text.lower() in _GENERIC_HEADINGS:
        return False
    # Reject strings that are too long to be a single name (concatenated blobs)
    if len(text) > 80:
        return False

    # Check for credential in original text before stripping
    has_credential = bool(_HAS_CREDENTIAL_RE.search(text))

    cleaned = _CREDENTIALS_RE.sub("", text)
    cleaned = _TITLE_PREFIX_RE.sub("", cleaned)
    cleaned = cleaned.strip().rstrip(",").strip()

    words = cleaned.split()
    # Strict: allow only 2 or 3 words
    if not 2 <= len(words) <= 3:
        return False

    for word in words:
        # Reject if any word is a known non-name generic term
        if word.lower() in _GENERIC_WORDS:
            return False
        # Reject ALL-CAPS words longer than 4 chars — these are acronyms/labels,
        # not surname components (e.g. "TESTING", "HUMAC", "INfrared" → mixed caps)
        core_stripped = word.rstrip(".")
        if len(core_stripped) > 4 and core_stripped.isupper():
            return False
        # Reject mixed-caps words like "INfrared", "REd" — interior uppercase chars
        # A valid name word has at most one leading uppercase letter
        if re.search(r"[a-z][A-Z]", word):
            return False
        # Each word must start with uppercase
        core = re.sub(r"^[-']|[-']$", "", word)
        if not core or not core[0].isupper():
            return False
        # Name-safe character check: letters, apostrophes, hyphens,
        # optional single trailing period (middle initial "K.")
        if not re.match(r"^[A-Z][a-zA-Z\'\-]*\.?$", word):
            return False
        # Hyphenated words: every hyphen-separated part must start uppercase
        # AND must not be a generic word (rejects "State-of-the", "Walk-In", etc.)
        if "-" in word:
            parts = word.rstrip(".").split("-")
            if not all(p and p[0].isupper() for p in parts if p):
                return False
            if any(p.lower() in _GENERIC_WORDS for p in parts if p):
                return False

    # 3-word entries require a credential (reduces "Brazilian Lymphatic Drainage" etc.)
    if len(words) == 3 and not has_credential:
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


def _has_credential_nearby(tag) -> bool:
    """
    Return True if a PT credential appears in `tag` itself OR in any sibling
    element within the same parent container.

    This enforces: only record a name when a qualifying credential
    (PT, DPT, MPT, OTR, CHT, PTA, LMT, …) is co-located on the page,
    eliminating content headings that happen to look like names.
    """
    if _HAS_CREDENTIAL_RE.search(tag.get_text(" ", strip=True)):
        return True
    parent = tag.parent
    if parent is None:
        return False
    for sibling in parent.children:
        if sibling is tag or not hasattr(sibling, "get_text"):
            continue
        if _HAS_CREDENTIAL_RE.search(sibling.get_text(" ", strip=True)):
            return True
    return False


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
        if (
            _looks_like_name(text)
            and _has_credential_nearby(tag)
            and text.lower() not in seen_lower
        ):
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
            if (
                _looks_like_name(text)
                and _has_credential_nearby(tag)
                and text.lower() not in scoped_lower
            ):
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
