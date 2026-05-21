import streamlit as st
import pandas as pd
import requests
import re
import json
import concurrent.futures
import threading
import time as _time
from html.parser import HTMLParser
from openai import OpenAI

st.set_page_config(page_title="NCP Intermediary Sourcing Tool", page_icon="🔍", layout="wide")

# ---------------------------------------------------------------------------
# RATE LIMITER  — prevents Apollo 429 avalanche
# ---------------------------------------------------------------------------
class _RateLimiter:
    """Thread-safe rate limiter."""
    def __init__(self, calls_per_sec):
        self._interval = 1.0 / calls_per_sec
        self._lock = threading.Lock()
        self._last = 0.0
    def wait(self):
        with self._lock:
            now = _time.monotonic()
            gap = self._last + self._interval - now
            if gap > 0:
                _time.sleep(gap)
            self._last = _time.monotonic()

_apollo_limiter = _RateLimiter(4)  # 4 req/sec — stays under Apollo's 5/sec cap

# ---------------------------------------------------------------------------
# PHONE SCRAPING — stdlib only (no BeautifulSoup / Firecrawl)
# ---------------------------------------------------------------------------
class _HTMLTextExtractor(HTMLParser):
    """Extract visible text from HTML."""
    def __init__(self):
        super().__init__()
        self._pieces = []
        self._skip = False
    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style", "noscript"):
            self._skip = True
    def handle_endtag(self, tag):
        if tag in ("script", "style", "noscript"):
            self._skip = False
    def handle_data(self, data):
        if not self._skip:
            self._pieces.append(data)
    def get_text(self):
        return " ".join(self._pieces)

_PHONE_RE = re.compile(
    r"(?<!\d)(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]\d{3}[-.\s]\d{4}(?!\d)"
)

_ADVISOR_CONTACT_PATHS = [
    "/team", "/our-team", "/advisors", "/our-advisors", "/people",
    "/about", "/about-us", "/contact", "/contact-us", "/professionals",
    "/staff", "/leadership", "/our-people", "/wealth-advisors",
    "/financial-advisors",
]

def _scrape_page(url):
    """Fetch a page and return visible text, or empty string on failure."""
    try:
        r = requests.get(url, timeout=8, headers={
            "User-Agent": "Mozilla/5.0 (compatible; NCPBot/1.0)"
        })
        if r.status_code != 200:
            return ""
        extractor = _HTMLTextExtractor()
        extractor.feed(r.text)
        return extractor.get_text()
    except Exception:
        return ""

def _find_phone_for_person(name, domain):
    """Scrape employer website to find a phone number for a specific person.
    Falls back to the company's main phone number if person-specific not found."""
    if not domain:
        return "N/A"
    last_name = name.split()[-1].lower() if name.strip() else ""
    base = f"https://{domain}"
    company_phone = None

    pages_to_try = [base + path for path in _ADVISOR_CONTACT_PATHS]
    pages_to_try.append(base)  # homepage as last resort

    for page_url in pages_to_try:
        text = _scrape_page(page_url)
        if not text:
            continue
        phones = _PHONE_RE.findall(text)
        if not phones:
            continue
        if company_phone is None:
            company_phone = phones[0]
        if last_name:
            text_lower = text.lower()
            idx = text_lower.find(last_name)
            if idx != -1:
                # Look for phones within ~300 chars of the name
                nearby = text[max(0, idx - 150): idx + 300]
                nearby_phones = _PHONE_RE.findall(nearby)
                if nearby_phones:
                    return nearby_phones[0]

    return company_phone or "N/A"

# ---------------------------------------------------------------------------
# AI VERIFICATION — double-checks geography + industry fit
# ---------------------------------------------------------------------------
_VERIFY_SYSTEM = """You are a strict quality-control bot for a professional contacts database.
You will receive a contact record, the search criteria, and specific rules about which
companies/organizations are valid. Determine whether the contact is a GOOD match by
checking TWO things:

1. GEOGRAPHY — Is the person located in or very near the searched city/metro area?
   (e.g., Hoover and Vestavia Hills are part of the Birmingham, AL metro.)
2. INDUSTRY FIT — Does the person's title AND company match the category rules below?
   You MUST apply the VALID/REJECT rules strictly. If the company is a type listed
   under REJECT, the contact FAILS regardless of their title. Internal corporate
   roles (e.g., "Tax Director" at a law firm or coal company) always FAIL.
   When in doubt, REJECT — false positives are worse than false negatives.

Respond with ONLY a JSON object: {"pass": true} or {"pass": false, "reason": "<short reason>"}"""

def _ai_verify_contact(row, category_key, search_city):
    """Ask GPT to verify a single contact's geography and industry fit."""
    if not _oai_client:
        return True, None
    cat = CATEGORIES[category_key]
    ai_context = cat.get("ai_context", "")
    prompt = (
        f"Search criteria: {category_key} in {search_city}\n\n"
    )
    if ai_context:
        prompt += f"Category rules:\n{ai_context}\n\n"
    prompt += (
        f"Contact:\n"
        f"  Name: {row.get('Name', 'N/A')}\n"
        f"  Title: {row.get('Title', 'N/A')}\n"
        f"  Company: {row.get('Company', 'N/A')}\n"
        f"  City: {row.get('City', 'N/A')}\n"
        f"  State: {row.get('State', 'N/A')}\n"
    )
    try:
        resp = _oai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": _VERIFY_SYSTEM},
                {"role": "user", "content": prompt},
            ],
            temperature=0,
            max_tokens=100,
        )
        text = resp.choices[0].message.content.strip()
        # Handle markdown-wrapped JSON
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
            text = text.strip()
        result = json.loads(text)
        return result.get("pass", True), result.get("reason")
    except Exception:
        return True, None  # fail open — don't discard on API errors

# ---------------------------------------------------------------------------
# AUTH
# ---------------------------------------------------------------------------
def check_password():
    def password_entered():
        if st.session_state["password"] == st.secrets["APP_PASSWORD"]:
            st.session_state["password_correct"] = True
            del st.session_state["password"]
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        st.text_input("Enter Password", type="password", on_change=password_entered, key="password")
        return False
    elif not st.session_state["password_correct"]:
        st.text_input("Enter Password", type="password", on_change=password_entered, key="password")
        st.error("Incorrect password")
        return False
    return True

if not check_password():
    st.stop()

# ---------------------------------------------------------------------------
# SECRETS
# ---------------------------------------------------------------------------
try:
    APOLLO_API_KEY = st.secrets["APOLLO_API_KEY"]
except (FileNotFoundError, KeyError):
    st.error("APOLLO_API_KEY missing. Set it in `.streamlit/secrets.toml`.")
    st.stop()

try:
    _oai_client = OpenAI(api_key=st.secrets["OPENAI_API_KEY"], timeout=20.0)
except (FileNotFoundError, KeyError):
    _oai_client = None

# ---------------------------------------------------------------------------
# CATEGORY DEFINITIONS
# ---------------------------------------------------------------------------
CATEGORIES = {
    "Private Wealth Advisors": {
        "industries": [
            "Financial Services", "Investment Management", "Banking",
            "Investment Banking", "Insurance",
        ],
        "keywords": [
            "wealth management", "private wealth", "financial planning",
            "financial advisory", "investment advisory", "wealth advisor",
        ],
        "title_filter": [
            "wealth advisor", "wealth manager", "wealth management",
            "financial advisor", "financial planner", "financial consultant",
            "portfolio manager", "investment advisor", "investment consultant",
            "private client advisor", "private client manager",
            "private banker", "asset management director",
            "financial planning", "investment management",
        ],
        "title_exclude": [
            "credit", "lending", "loan", "mortgage", "real estate", "housing",
            "equipment", "esg", "surveillance", "risk", "compliance", "bsa",
            "aml", "operations", "martech", "marketing", "recovery",
            "collections", "data", "product manager", "engineering",
            "technology", "audit", "insurance underwriting",
            "client services", "administrative", "hr ", "human resources",
            "legal", "paralegal", "recruiting", "talent", "facilities",
            "procurement", "supply chain", "logistics", "it ",
            "information technology", "software", "developer",
        ],
        "person_titles": [
            "wealth advisor", "wealth manager", "financial advisor",
            "financial planner", "private wealth", "investment advisor",
            "portfolio manager", "private client advisor",
            "private client manager", "financial consultant",
        ],
        "ai_context": (
            "VALID: wealth management firms, RIAs, financial advisory firms, "
            "private banks, family offices, independent broker-dealers. "
            "REJECT: law firms, accounting firms, insurance companies, banks "
            "(unless private banking division), healthcare, technology, "
            "industrial, retail, or any non-advisory company."
        ),
    },
    "Tax Advisors (High Net Worth)": {
        "industries": ["Accounting", "Financial Services"],
        "keywords": [
            "tax advisory", "tax services", "public accounting",
            "tax planning", "cpa", "certified public accountant",
        ],
        "title_filter": [
            "tax partner", "tax director", "tax advisor", "tax principal",
            "tax manager", "tax counsel", "tax planning",
            "cpa", "certified public accountant",
            "accountant", "accounting partner", "accounting director",
            "audit partner", "assurance partner",
        ],
        "title_exclude": [
            "attorney", "lawyer", "law ", "legal", "corporate and",
            "surgery", "surgical", "healthcare", "medical", "clinical",
            "nurse", "physician", "hospital", "pharma",
            "property", "real estate", "leasing", "construction",
            "mortgage", "lending", "loan",
            "insurance underwriting", "claims",
            "manufacturing", "warehouse", "logistics", "supply chain",
            "software", "developer", "engineering", "product manager",
            "marketing", "sales rep", "business development",
            "human resources", "hr ", "recruiting", "talent",
            "administrative", "receptionist", "office manager",
            "staff accountant", "accounts payable", "accounts receivable",
            "bookkeeper", "payroll", "billing",
        ],
        "person_titles": [
            "tax partner", "tax director", "tax advisor",
            "tax manager", "cpa", "tax principal",
        ],
        "org_industries_allowed": [
            "accounting", "financial services", "management consulting",
            "investment management",
        ],
        "ai_context": (
            "VALID: CPA firms, public accounting firms (Big 4, regional, local), "
            "tax advisory boutiques, financial advisory firms with tax practices. "
            "REJECT: law firms (even if they have a tax practice group — law firm "
            "tax attorneys are NOT tax advisors), healthcare companies, industrial/"
            "manufacturing companies, insurance companies, real estate companies, "
            "retail companies, coal/mining/energy companies, media companies, "
            "technology companies, and ANY other non-accounting/non-advisory "
            "organization. A 'Tax Director' at a non-accounting company is an "
            "internal corporate role, NOT a tax advisor."
        ),
    },
    "Estate Planning Attorneys": {
        "industries": ["Law Practice", "Legal Services"],
        "keywords": [
            "estate planning", "trusts and estates", "probate",
            "elder law", "trust administration", "wealth transfer",
        ],
        "auto_org_search": True,
        "title_filter": [
            "estate planning", "estate attorney", "estate lawyer",
            "trusts and estates", "trust attorney", "trust counsel",
            "trust lawyer", "trust administration",
            "probate attorney", "probate lawyer", "probate counsel",
            "elder law", "wealth transfer",
            "wills and estates", "estate counsel",
            "partner", "shareholder", "of counsel", "counsel",
            "managing associate", "managing partner", "member",
            "principal", "director",
        ],
        "title_exclude": [
            "real estate", "property", "housing",
            "university", "college", "education", "gift planning",
            "fundraising", "development officer", "advancement",
            "mental health", "court program", "social work",
            "counselor", "therapist", "clinical",
            "healthcare", "medical", "surgery", "nursing", "pharma",
            "insurance", "claims", "underwriting",
            "marketing", "sales rep", "business development",
            "software", "developer", "engineering", "product manager",
            "human resources", "hr ", "recruiting", "administrative",
            "paralegal", "legal assistant", "legal secretary",
        ],
        "person_titles": [
            "estate planning attorney", "estate planning partner",
            "trusts and estates", "probate attorney",
            "elder law attorney", "wealth transfer",
            "estate attorney", "estate planning counsel",
            "trust attorney", "estate planning",
        ],
        "ai_context": (
            "VALID: law firms with estate planning / trusts & estates practices, "
            "elder law firms, estate planning boutiques. "
            "REJECT: universities, nonprofits, court systems, mental health "
            "organizations, real estate companies, healthcare, corporate legal "
            "departments, and anyone whose role is not practicing estate "
            "planning law."
        ),
    },
    "Investment Bankers": {
        "industries": [
            "Investment Banking", "Capital Markets", "Financial Services",
            "Venture Capital & Private Equity",
        ],
        "keywords": [
            "investment banking", "mergers acquisitions", "capital markets",
            "corporate finance", "m&a advisory",
        ],
        "title_filter": [
            "investment bank", "m&a", "mergers", "capital markets",
            "corporate finance", "managing director",
            "dealmaker", "deal advisory", "deal origination",
        ],
        "title_exclude": [
            "real estate", "property", "mortgage", "lending", "loan",
            "insurance", "healthcare", "medical", "pharma",
            "retail", "restaurant", "hospitality",
            "software", "developer", "engineering", "product manager",
            "marketing", "human resources", "hr ", "recruiting",
            "administrative", "legal assistant", "paralegal",
            "accounting", "bookkeeper", "payroll",
        ],
        "person_titles": [
            "investment banker", "managing director", "m&a",
            "corporate finance", "capital markets", "mergers",
        ],
        "ai_context": (
            "VALID: investment banks, M&A advisory firms, boutique advisory "
            "firms, capital markets firms. "
            "REJECT: commercial banks (unless IB division), insurance, "
            "healthcare, technology, retail, industrial, real estate, and "
            "any non-financial-services company."
        ),
    },
    "Business Brokers": {
        "industries": [
            "Financial Services", "Management Consulting",
            "Accounting",
        ],
        "keywords": [
            "business brokerage", "business broker", "business sales",
            "business valuation", "business transfer", "business intermediary",
        ],
        "title_filter": [
            "business broker", "business intermediary", "business valuation",
            "business sales", "business transfer", "business advisor",
            "transaction advisor", "deal advisor", "m&a advisor",
            "m&a consultant", "mergers", "acquisition",
        ],
        "title_exclude": [
            "real estate broker", "real estate agent", "realtor",
            "insurance broker", "freight broker", "customs broker",
            "mortgage broker", "loan broker", "stock broker",
            "healthcare", "medical", "pharma", "surgery",
            "software", "developer", "engineering", "product manager",
            "marketing", "human resources", "hr ", "recruiting",
            "administrative", "receptionist",
            "bookkeeper", "payroll", "billing",
        ],
        "person_titles": [
            "business broker", "business intermediary", "business valuation",
            "transaction advisor", "deal advisor", "m&a advisor",
        ],
        "ai_context": (
            "VALID: business brokerage firms, M&A advisory boutiques, "
            "business valuation firms, transaction advisory firms. "
            "REJECT: real estate brokerages, insurance brokerages, freight "
            "brokers, stock brokers, and any company that is not in the "
            "business of selling/brokering businesses."
        ),
    },
}

SENIORITY_LEVELS = ["owner", "founder", "c_suite", "partner", "vp", "director"]

# ---------------------------------------------------------------------------
# STEP 1A: FIND PEOPLE DIRECTLY BY TITLE + LOCATION
# ---------------------------------------------------------------------------
MAX_PEOPLE_DIRECT = 300

def search_people_direct(category_key, city, max_pages=3):
    """Search Apollo for people by title + location — catches advisors at firms
    the org search misses (wrong industry tags, no keyword tags, etc.)."""
    cat = CATEGORIES[category_key]
    url = "https://api.apollo.io/api/v1/mixed_people/api_search"
    headers = {"Content-Type": "application/json", "X-Api-Key": APOLLO_API_KEY}

    all_people, seen_ids = [], set()

    for page in range(1, max_pages + 1):
        if len(all_people) >= MAX_PEOPLE_DIRECT:
            break
        payload = {
            "person_titles": cat["person_titles"],
            "person_locations": [city],
            "person_seniorities": SENIORITY_LEVELS,
            "per_page": 100,
            "page": page,
        }
        try:
            _apollo_limiter.wait()
            r = requests.post(url, headers=headers, json=payload, timeout=15)
            if r.status_code == 200:
                people = r.json().get("people", [])
                if not people:
                    break
                for p in people:
                    pid = p.get("id")
                    if pid and pid not in seen_ids:
                        seen_ids.add(pid)
                        all_people.append(p)
                if len(people) < 100:
                    break
            elif r.status_code == 429:
                _time.sleep(1); continue
            else:
                break
        except Exception:
            break

    return all_people


def enrich_person(person_id, headers):
    """Enrich a single person by ID to get full name, email, phone."""
    url = "https://api.apollo.io/v1/people/match"
    for attempt in range(2):
        try:
            _apollo_limiter.wait()
            r = requests.post(url, headers=headers, json={"id": person_id}, timeout=10)
            if r.status_code == 200:
                return r.json().get("person")
            elif r.status_code == 429:
                _time.sleep(1); continue
            else:
                return None
        except Exception:
            return None
    return None


# ---------------------------------------------------------------------------
# STEP 1B: FIND ORGANIZATIONS (catches additional firms)
# ---------------------------------------------------------------------------
MAX_ORGS = 200  # Cap total orgs to keep runtime under 5 minutes

def search_organizations(category_key, city, max_pages=2):
    cat = CATEGORIES[category_key]
    url = "https://api.apollo.io/v1/organizations/search"
    headers = {"Content-Type": "application/json", "X-Api-Key": APOLLO_API_KEY}

    all_orgs, seen_ids = [], set()

    def _fetch_pages(base_payload):
        for page in range(1, max_pages + 1):
            if len(all_orgs) >= MAX_ORGS:
                break
            payload = {**base_payload, "page": page, "per_page": 100}
            try:
                _apollo_limiter.wait()
                r = requests.post(url, headers=headers, json=payload, timeout=10)
                if r.status_code != 200:
                    break
                orgs = r.json().get("organizations", [])
                if not orgs:
                    break
                for o in orgs:
                    oid = o.get("id")
                    if oid and oid not in seen_ids:
                        seen_ids.add(oid)
                        all_orgs.append(o)
                if len(orgs) < 100:
                    break
            except Exception:
                break

    # Pass 1: Industry + keyword sweep (keywords keep results relevant)
    for industry in cat["industries"]:
        if len(all_orgs) >= MAX_ORGS:
            break
        _fetch_pages({
            "organization_locations": [city],
            "q_organization_industries": [industry],
            "q_organization_keyword_tags": cat["keywords"],
        })

    # Pass 2: Keyword-only sweep (catches firms in unexpected industries)
    if len(all_orgs) < MAX_ORGS:
        _fetch_pages({
            "organization_locations": [city],
            "q_organization_keyword_tags": cat["keywords"],
        })

    return all_orgs


# ---------------------------------------------------------------------------
# STEP 2: FIND SENIOR PEOPLE AT EACH ORG
# ---------------------------------------------------------------------------
def get_senior_people(org_id, org_name, domain=None, city=None):
    search_url = "https://api.apollo.io/api/v1/mixed_people/api_search"
    enrich_url = "https://api.apollo.io/v1/people/match"
    headers = {"Content-Type": "application/json", "X-Api-Key": APOLLO_API_KEY}

    # Step 1: Search to get person IDs (returns obfuscated data)
    payload = {
        "organization_ids": [org_id],
        "person_seniorities": SENIORITY_LEVELS,
        "per_page": 25,
    }
    if city:
        payload["person_locations"] = [city]
    people_ids = []
    for attempt in range(3):
        try:
            _apollo_limiter.wait()
            r = requests.post(search_url, headers=headers, json=payload, timeout=10)
            if r.status_code == 200:
                for p in r.json().get("people", []):
                    pid = p.get("id")
                    if pid and p.get("first_name"):
                        people_ids.append({
                            "id": pid,
                            "first_name": p.get("first_name"),
                            "title": p.get("title"),
                        })
                break
            elif r.status_code == 429:
                _time.sleep(1); continue
            else:
                return [{"_debug": True, "status": r.status_code, "body": r.text[:200]}]
        except Exception as e:
            if attempt < 2:
                _time.sleep(1); continue
            return [{"_debug": True, "error": str(e)}]

    if not people_ids:
        return []

    # Step 2: Enrich each person by ID to get full name, email, phone
    enriched = []
    for p in people_ids[:25]:
        for attempt in range(2):
            try:
                _apollo_limiter.wait()
                r = requests.post(enrich_url, headers=headers, json={"id": p["id"]}, timeout=10)
                if r.status_code == 200:
                    person = r.json().get("person")
                    if person and person.get("first_name") and person.get("last_name"):
                        if not person.get("organization"):
                            person["organization"] = {"name": org_name}
                        enriched.append(person)
                    break
                elif r.status_code == 429:
                    _time.sleep(1); continue
                else:
                    break
            except Exception:
                break

    return enriched if enriched else []


def bulk_enrich(people, domain):
    """Enrich people with emails/phones via Apollo bulk_match."""
    url = "https://api.apollo.io/v1/people/bulk_match"
    headers = {"Content-Type": "application/json", "X-Api-Key": APOLLO_API_KEY}

    details = []
    for p in people[:10]:  # bulk_match handles up to 10 at a time
        details.append({
            "first_name": p.get("first_name"),
            "last_name": p.get("last_name"),
            "domain": domain,
        })

    try:
        r = requests.post(url, headers=headers, json={"details": details}, timeout=15)
        if r.status_code == 200:
            matches = r.json().get("matches", [])
            enriched = []
            for i, match in enumerate(matches):
                if match:
                    # Preserve org info from original person
                    if not match.get("organization") and i < len(people):
                        match["organization"] = people[i].get("organization")
                    enriched.append(match)
                elif i < len(people):
                    enriched.append(people[i])
            return enriched
    except Exception:
        pass
    return None


def clean_domain(url):
    if not url or not isinstance(url, str):
        return None
    try:
        from urllib.parse import urlparse
        if not url.startswith("http"):
            url = "http://" + url
        d = urlparse(url).netloc
        return d[4:] if d.startswith("www.") else d
    except Exception:
        return None


# ---------------------------------------------------------------------------
# STEP 3: FILTER AND FORMAT
# ---------------------------------------------------------------------------
def is_senior_enough(title):
    if not title:
        return False
    t = title.lower()
    junior_signals = [
        "analyst", "associate", "intern", "assistant", "coordinator",
        "junior", "trainee", "entry", "clerk", "receptionist",
        "administrative", "support", "secretary",
    ]
    if any(j in t for j in junior_signals):
        return False
    return True


def title_matches_category(title, category_key):
    if not title:
        return True  # No title — let it through, user can filter in CSV
    t = title.lower()
    filters = CATEGORIES[category_key].get("title_filter", [])
    if not filters:
        return True
    if not any(f in t for f in filters):
        return False
    excludes = CATEGORIES[category_key].get("title_exclude", [])
    if excludes and any(x in t for x in excludes):
        return False
    return True


def _org_passes_industry_check(org_info, category_key):
    """Check if an organization's industry is relevant to the search category.
    Only applies when the category defines org_industries_allowed."""
    allowed = CATEGORIES[category_key].get("org_industries_allowed")
    if not allowed:
        return True
    industry = (org_info.get("industry") or "").lower()
    if not industry:
        return True  # no industry data — benefit of the doubt
    return any(a in industry for a in allowed)


def _normalize_state(s):
    """Normalize state name for comparison (handles abbreviations)."""
    if not s:
        return ""
    s = s.strip().lower()
    # Common abbreviation → full name mapping
    abbrevs = {
        "al": "alabama", "ak": "alaska", "az": "arizona", "ar": "arkansas",
        "ca": "california", "co": "colorado", "ct": "connecticut", "de": "delaware",
        "fl": "florida", "ga": "georgia", "hi": "hawaii", "id": "idaho",
        "il": "illinois", "in": "indiana", "ia": "iowa", "ks": "kansas",
        "ky": "kentucky", "la": "louisiana", "me": "maine", "md": "maryland",
        "ma": "massachusetts", "mi": "michigan", "mn": "minnesota", "ms": "mississippi",
        "mo": "missouri", "mt": "montana", "ne": "nebraska", "nv": "nevada",
        "nh": "new hampshire", "nj": "new jersey", "nm": "new mexico", "ny": "new york",
        "nc": "north carolina", "nd": "north dakota", "oh": "ohio", "ok": "oklahoma",
        "or": "oregon", "pa": "pennsylvania", "ri": "rhode island", "sc": "south carolina",
        "sd": "south dakota", "tn": "tennessee", "tx": "texas", "ut": "utah",
        "vt": "vermont", "va": "virginia", "wa": "washington", "wv": "west virginia",
        "wi": "wisconsin", "wy": "wyoming", "dc": "district of columbia",
    }
    return abbrevs.get(s, s)


def _person_matches_location(person, search_city):
    """Check if a person is located in or near the searched city/state."""
    if not search_city:
        return True
    # Parse "Birmingham, AL" or "Birmingham, Alabama" from search input
    parts = [p.strip().lower() for p in search_city.split(",")]
    search_city_name = parts[0] if parts else ""
    search_state = _normalize_state(parts[1]) if len(parts) > 1 else ""

    person_city = (person.get("city") or "").strip().lower()
    person_state = _normalize_state(person.get("state") or "")

    # If we have a state from the search, match on state at minimum
    if search_state and person_state:
        return person_state == search_state
    # Otherwise match on city name
    if search_city_name and person_city:
        return search_city_name in person_city or person_city in search_city_name
    # If person has no location data, let them through (user can filter in CSV)
    return True


def process_org(org, category_key, search_city=None):
    org_id = org.get("id")
    org_name = org.get("name", "Unknown")
    domain = clean_domain(org.get("website_url"))

    if not _org_passes_industry_check(org, category_key):
        return {"rows": [], "debug": [], "org_name": org_name, "people_count": 0}

    people = get_senior_people(org_id, org_name, domain, city=search_city)

    # Collect debug info
    debug_msgs = [p for p in people if isinstance(p, dict) and p.get("_debug")]
    people = [p for p in people if isinstance(p, dict) and not p.get("_debug")]

    rows = []
    for p in people:
        title = p.get("title") or ""
        if not is_senior_enough(title):
            continue
        if not title_matches_category(title, category_key):
            continue
        if not _person_matches_location(p, search_city):
            continue

        name = f"{p.get('first_name', '')} {p.get('last_name', '')}".strip()
        if not name:
            continue

        email = p.get("email") or "N/A"
        phone_nums = p.get("phone_numbers") or []
        phone = phone_nums[0].get("sanitized_number") if phone_nums else "N/A"
        company = (p.get("organization") or {}).get("name") or org_name

        rows.append({
            "Name": name,
            "Title": title,
            "Company": company,
            "Email": email,
            "Phone": phone or "N/A",
            "City": p.get("city") or org.get("city") or "N/A",
            "State": p.get("state") or org.get("state") or "N/A",
            "LinkedIn": p.get("linkedin_url") or "N/A",
            "_domain": domain or "",
        })

    return {"rows": rows, "debug": debug_msgs, "org_name": org_name,
            "people_count": len(people)}


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------
st.title("NCP Intermediary Sourcing Tool")
st.caption("Find senior wealth advisors, tax professionals, estate attorneys, investment bankers, and business brokers by city.")

col1, col2 = st.columns([2, 3])
category = col1.selectbox("Category", list(CATEGORIES.keys()))
city = col2.text_input("City", placeholder="e.g. Charlotte, NC  |  Raleigh, NC  |  Dallas, TX")
include_pass2 = st.checkbox("Include org-based search (slower, finds additional firms)", value=False)

if st.button("Search", type="primary"):
    if not city.strip():
        st.error("Please enter a city.")
        st.stop()

    headers = {"Content-Type": "application/json", "X-Api-Key": APOLLO_API_KEY}
    all_rows = []
    seen_person_ids = set()  # Dedup across both passes

    # ── Pass 1: People-direct search (primary — most exhaustive) ──────────
    with st.spinner(f"Searching for **{category.lower()}** by title in **{city}**..."):
        direct_people = search_people_direct(category, city)

    # Pre-filter using search-result fields → only enrich people we'll keep
    filtered = []
    for p in direct_people:
        title = p.get("title") or ""
        if not is_senior_enough(title):
            continue
        if not title_matches_category(title, category):
            continue
        if not _person_matches_location(p, city):
            continue
        if not _org_passes_industry_check(p.get("organization") or {}, category):
            continue
        filtered.append(p)

    st.info(f"Pass 1: **{len(direct_people)}** people found, **{len(filtered)}** match filters. Enriching…")
    progress_bar = st.progress(0)
    status_text = st.empty()
    total = len(filtered) or 1

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:
        futures = {
            ex.submit(enrich_person, p["id"], headers): p
            for p in filtered
        }
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            try:
                person = future.result()
                if person and person.get("first_name") and person.get("last_name"):
                    pid = person.get("id")
                    if pid:
                        seen_person_ids.add(pid)

                    name = f"{person.get('first_name', '')} {person.get('last_name', '')}".strip()
                    email = person.get("email") or "N/A"
                    phone_nums = person.get("phone_numbers") or []
                    phone = phone_nums[0].get("sanitized_number") if phone_nums else "N/A"
                    company = (person.get("organization") or {}).get("name") or "N/A"

                    org_obj = person.get("organization") or {}
                    if not _org_passes_industry_check(org_obj, category):
                        continue
                    person_domain = clean_domain(org_obj.get("website_url")) or ""

                    all_rows.append({
                        "Name": name,
                        "Title": title if not person.get("title") else person.get("title"),
                        "Company": company,
                        "Email": email,
                        "Phone": phone or "N/A",
                        "City": person.get("city") or "N/A",
                        "State": person.get("state") or "N/A",
                        "LinkedIn": person.get("linkedin_url") or "N/A",
                        "_domain": person_domain,
                    })
            except Exception:
                pass
            progress_bar.progress((i + 1) / total)
            status_text.caption(
                f"Enriched {i + 1}/{total} | {len(all_rows)} contacts so far..."
            )

    pass1_count = len(all_rows)

    # ── Pass 2: Org-based search (supplementary — catches different firms) ─
    run_pass2 = include_pass2 or CATEGORIES[category].get("auto_org_search", False)
    if not run_pass2:
        orgs = []
    else:
        with st.spinner(f"Pass 2: Searching firms in **{city}** for additional contacts..."):
            orgs = search_organizations(category, city)

    if orgs:
        status_text.caption(f"Found {len(orgs)} firms. Checking for new contacts...")
        progress_bar.progress(0)

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as ex:
            futures = {
                ex.submit(process_org, org, category, city): org for org in orgs
            }
            for i, future in enumerate(concurrent.futures.as_completed(futures)):
                try:
                    result = future.result()
                    if result:
                        for r in result["rows"]:
                            all_rows.append(r)
                except Exception:
                    pass
                progress_bar.progress((i + 1) / len(orgs))
                status_text.caption(
                    f"Processed {i + 1}/{len(orgs)} firms | {len(all_rows)} total contacts..."
                )

    # Deduplicate by name + company
    seen = set()
    unique = []
    for r in all_rows:
        key = (r["Name"].lower(), r["Company"].lower())
        if key not in seen:
            seen.add(key)
            unique.append(r)

    # ── AI Verification — double-check geography + industry fit ─────────
    ai_rejected = []
    if _oai_client and unique:
        status_text.caption(f"AI verification: checking {len(unique)} contacts…")
        progress_bar.progress(0)
        verify_total = len(unique)

        def _verify(row):
            return row, *_ai_verify_contact(row, category, city)

        verified = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
            futs = [ex.submit(_verify, r) for r in unique]
            for i, fut in enumerate(concurrent.futures.as_completed(futs)):
                try:
                    row, passed, reason = fut.result()
                    if passed:
                        verified.append(row)
                    else:
                        ai_rejected.append({"Name": row["Name"], "Company": row.get("Company", ""),
                                            "Title": row.get("Title", ""), "Reason": reason or "N/A"})
                except Exception:
                    pass
                progress_bar.progress((i + 1) / verify_total)
                status_text.caption(
                    f"AI verify: {i + 1}/{verify_total} | {len(verified)} passed…"
                )
        unique = verified

    # ── Pass 3: Scrape employer websites for missing phone numbers ─────────
    needs_phone = [r for r in unique if r.get("Phone", "N/A") == "N/A" and r.get("_domain")]
    if needs_phone:
        status_text.caption(f"Scraping employer websites for {len(needs_phone)} missing phone numbers…")
        progress_bar.progress(0)
        phone_total = len(needs_phone) or 1

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
            phone_futures = {
                ex.submit(_find_phone_for_person, r["Name"], r["_domain"]): r
                for r in needs_phone
            }
            for i, fut in enumerate(concurrent.futures.as_completed(phone_futures)):
                try:
                    phone = fut.result()
                    if phone and phone != "N/A":
                        phone_futures[fut]["Phone"] = phone
                except Exception:
                    pass
                progress_bar.progress((i + 1) / phone_total)
                status_text.caption(
                    f"Phone scrape: {i + 1}/{phone_total}…"
                )

    status_text.write("Done!")

    # Strip internal _domain field before display/export
    for r in unique:
        r.pop("_domain", None)

    # Debug info
    with st.expander("Debug Info"):
        st.write(f"Pass 1 (people-direct): {pass1_count} contacts")
        st.write(f"Pass 2 (org-based): {len(all_rows) - pass1_count} additional contacts")
        st.write(f"Total before dedup: {len(all_rows)}")
        st.write(f"After dedup: {len(unique) + len(ai_rejected)}")
        if ai_rejected:
            st.write(f"AI verification: {len(ai_rejected)} rejected, {len(unique)} passed")
            st.write("**Rejected contacts:**")
            st.dataframe(pd.DataFrame(ai_rejected), use_container_width=True)
        st.write(f"Final contacts: {len(unique)}")
        if needs_phone:
            filled = sum(1 for r in unique if r.get("Phone", "N/A") != "N/A")
            st.write(f"Phone scrape: {len(needs_phone)} attempted, {filled} total with phones")

    if not unique:
        st.warning(
            "No senior contacts found.\n\n"
            "**Tips:** Try a larger city or different category."
        )
        st.stop()

    st.success(f"**{len(unique)}** unique contacts found.")

    df = pd.DataFrame(unique)
    st.dataframe(df, use_container_width=True)

    csv = df.to_csv(index=False).encode("utf-8")
    fname = f"NCP_{category.replace(' ', '_')}_{city.replace(' ', '_').replace(',', '')}.csv"
    st.download_button(
        "Download CSV", data=csv, file_name=fname, mime="text/csv", type="primary"
    )
