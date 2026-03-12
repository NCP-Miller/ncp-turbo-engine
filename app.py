import streamlit as st
import pandas as pd
import requests
import json
import re
import concurrent.futures
import threading
import time
import xml.etree.ElementTree as ET
from urllib.parse import urlparse, urljoin, quote_plus
from openai import OpenAI

st.set_page_config(page_title="NCP Sourcing Engine", page_icon="🚀", layout="wide")

# ---------------------------------------------------------------------------
# AUTH
# ---------------------------------------------------------------------------
def check_password():
    try:
        app_password = st.secrets["APP_PASSWORD"]
    except (FileNotFoundError, KeyError):
        app_password = "NCP2026"

    def password_entered():
        if st.session_state["password"] == app_password:
            st.session_state["password_correct"] = True
            del st.session_state["password"]
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        st.text_input("Enter Password", type="password", on_change=password_entered, key="password")
        return False
    elif not st.session_state["password_correct"]:
        st.text_input("Enter Password", type="password", on_change=password_entered, key="password")
        st.error("Password incorrect")
        return False
    return True

if not check_password():
    st.stop()

# ---------------------------------------------------------------------------
# SECRETS
# ---------------------------------------------------------------------------
try:
    APOLLO_API_KEY    = st.secrets["APOLLO_API_KEY"]
    OPENAI_API_KEY    = st.secrets["OPENAI_API_KEY"]
    FIRECRAWL_API_KEY = st.secrets["FIRECRAWL_API_KEY"]
except (FileNotFoundError, KeyError):
    st.error("API Keys missing. Set them in `.streamlit/secrets.toml`.")
    st.stop()

client = OpenAI(api_key=OPENAI_API_KEY, timeout=30.0)


# ---------------------------------------------------------------------------
# RATE LIMITING
# ---------------------------------------------------------------------------
class RateLimiter:
    """Thread-safe rate limiter to avoid hitting API rate limits."""
    def __init__(self, calls_per_second: float):
        self._min_interval = 1.0 / calls_per_second
        self._lock = threading.Lock()
        self._last_call = 0.0

    def wait(self):
        with self._lock:
            now = time.monotonic()
            wait = self._min_interval - (now - self._last_call)
            if wait > 0:
                time.sleep(wait)
            self._last_call = time.monotonic()


_openai_limiter   = RateLimiter(calls_per_second=8)   # ~480/min
_apollo_limiter   = RateLimiter(calls_per_second=5)   # ~300/min
_firecrawl_limiter = RateLimiter(calls_per_second=3)  # ~180/min


# ---------------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------------
OPENAI_MODEL = "gpt-4o"  # Change here if model rotates or you upgrade


def _openai_create(**kwargs):
    """Rate-limited wrapper around client.chat.completions.create."""
    _openai_limiter.wait()
    return client.chat.completions.create(**kwargs)
APOLLO_INDUSTRIES = [
    "Accounting","Airlines/Aviation","Alternative Dispute Resolution","Alternative Medicine",
    "Animation","Apparel & Fashion","Architecture & Planning","Arts and Crafts","Automotive",
    "Aviation & Aerospace","Banking","Biotechnology","Broadcast Media","Building Materials",
    "Business Supplies and Equipment","Capital Markets","Chemicals","Civic & Social Organization",
    "Civil Engineering","Commercial Real Estate","Computer & Network Security","Computer Games",
    "Computer Hardware","Computer Networking","Computer Software","Construction",
    "Consumer Electronics","Consumer Goods","Consumer Services","Cosmetics","Dairy",
    "Defense & Space","Design","Education Management","E-Learning",
    "Electrical/Electronic Manufacturing","Entertainment","Environmental Services",
    "Events Services","Executive Office","Facilities Services","Farming","Financial Services",
    "Fine Art","Food & Beverages","Food Production","Fund-Raising","Furniture",
    "Gambling & Casinos","Glass, Ceramics & Concrete","Government Administration",
    "Government Relations","Graphic Design","Health, Wellness and Fitness","Higher Education",
    "Hospital & Health Care","Hospitality","Human Resources","Import and Export",
    "Individual & Family Services","Industrial Automation","Information Services",
    "Information Technology and Services","Insurance","International Affairs",
    "International Trade and Development","Internet","Investment Banking","Investment Management",
    "Judiciary","Law Enforcement","Law Practice","Legal Services","Legislative Office",
    "Leisure, Travel & Tourism","Libraries","Logistics and Supply Chain",
    "Luxury Goods & Jewelry","Machinery","Management Consulting","Maritime","Market Research",
    "Marketing and Advertising","Mechanical or Industrial Engineering","Media Production",
    "Medical Devices","Medical Practice","Mental Health Care","Military","Mining & Metals",
    "Motion Pictures and Film","Museums and Institutions","Music","Nanotechnology","Newspapers",
    "Non-Profit Organization Management","Oil & Energy","Online Media","Outsourcing/Offshoring",
    "Package/Freight Delivery","Packaging and Containers","Paper & Forest Products",
    "Performing Arts","Pharmaceuticals","Philanthropy","Photography","Plastics",
    "Political Organization","Primary/Secondary Education","Printing",
    "Professional Training & Coaching","Program Development","Public Policy",
    "Public Relations and Communications","Public Safety","Publishing","Railroad Manufacture",
    "Ranching","Real Estate","Recreational Facilities and Services","Religious Institutions",
    "Renewables & Environment","Research","Restaurants","Retail","Security and Investigations",
    "Semiconductors","Shipbuilding","Sporting Goods","Sports","Staffing and Recruiting",
    "Supermarkets","Telecommunications","Textiles","Think Tanks","Tobacco",
    "Translation and Localization","Transportation/Trucking/Railroad","Utilities",
    "Venture Capital & Private Equity","Veterinary","Warehousing","Wholesale",
    "Wine and Spirits","Wireless","Writing and Editing",
]

_TITLE_SCORES = {
    "owner": 100, "founder": 95, "co-founder": 90,
    "chief executive": 95, " ceo": 95,
    "president": 90, "managing partner": 88, "managing member": 88,
    "managing director": 85, "principal": 85,
    "executive director": 82, "medical director": 80,
    "chief operating": 75, " coo": 75,
    "chief financial": 70, " cfo": 70,
    "chief medical": 80, "chief clinical": 78, "chief nursing": 75,
    "administrator": 70, "director of": 60,
    "vice president": 50, " vp ": 50,
    "manager": 30,
}

_CONTACT_PATHS = [
    "/about", "/about-us", "/team", "/our-team", "/leadership",
    "/staff", "/management", "/who-we-are", "/meet-the-team",
    "/people", "/executives", "/administration", "/about/team",
    "/about/leadership", "/contact", "/contact-us",
]

# ---------------------------------------------------------------------------
# AI — NICHE → APOLLO PARAMETERS
# ---------------------------------------------------------------------------
def _fallback_keywords(niche: str) -> str:
    """Rule-based keyword fallback used when the AI call returns nothing."""
    n = niche.lower()
    if any(x in n for x in ["pace", "program for all-inclusive", "all-inclusive care"]):
        return "pace program, adult day care, home health"
    if any(x in n for x in ["senior", "elderly", "elder care", "aging in place"]):
        return "senior living, home health, assisted living"
    if any(x in n for x in ["home health", "home care", "home-based"]):
        return "home health, home care, skilled nursing"
    if any(x in n for x in ["hospice", "palliative"]):
        return "hospice, palliative care, home health"
    if any(x in n for x in ["assisted living", "memory care", "dementia"]):
        return "assisted living, memory care, senior living"
    if any(x in n for x in ["skilled nursing", "nursing home", "snf"]):
        return "skilled nursing facility, long-term care"
    if any(x in n for x in ["behavioral health", "mental health", "psychiatr"]):
        return "behavioral health, mental health, outpatient"
    if any(x in n for x in ["substance", "addiction", "recovery", "rehab"]):
        return "substance abuse, addiction treatment, behavioral health"
    if any(x in n for x in ["urgent care", "walk-in"]):
        return "urgent care, ambulatory care, primary care"
    if any(x in n for x in ["hvac", "heating", "cooling", "air condition"]):
        return "hvac, mechanical contractor, facilities management"
    if any(x in n for x in ["veterinary", "vet ", "animal hospital"]):
        return "veterinary, animal hospital, pet care"
    if any(x in n for x in ["dental", "dentist"]):
        return "dental practice, dental care"
    if any(x in n for x in ["physical therapy", "occupational therapy", "pt clinic"]):
        return "physical therapy, outpatient rehab, sports medicine"
    return ""


def suggest_search_params(niche_description: str) -> dict:
    """
    Maps a plain-English niche to Apollo industries + keyword tags.
    The prompt gives GPT-4o concrete Apollo tag examples so it generates
    short real tags rather than long descriptive phrases that match nothing.
    A rule-based fallback fires if the AI returns empty keywords.
    """
    prompt = f"""You are configuring a B2B company database search on Apollo.io.
The analyst is targeting companies in this niche:

  "{niche_description}"

Study that niche carefully. Your choices must be driven by what kinds of companies
operate in that specific niche — not just obvious label matching.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PARAMETER 1 — INDUSTRY CATEGORIES (choose 3–5)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Pick from this exact list only. Choose the categories where companies in this niche
are ACTUALLY classified in Apollo — include non-obvious ones:

{json.dumps(APOLLO_INDUSTRIES)}

Classification guidance:
• PACE / senior day programs → "Individual & Family Services", "Non-Profit Organization Management",
  "Hospital & Health Care" (NOT "Pharmaceuticals" — that is drug manufacturers)
• Skilled nursing / assisted living → "Hospital & Health Care", "Individual & Family Services"
• Behavioral / mental health → "Mental Health Care", "Hospital & Health Care", "Individual & Family Services"
• Small clinics / practices → "Medical Practice" is usually more accurate than "Hospital & Health Care"
• Home services / HVAC → "Construction", "Facilities Services"
• Non-profit care operators → always add "Non-Profit Organization Management"

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PARAMETER 2 — KEYWORD TAGS (REQUIRED — do not leave blank)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Apollo tags each company with SHORT keyword phrases (1–4 words each).
You MUST return 2–5 tags. Empty keywords is not acceptable.

Real Apollo tags by domain (use these as your vocabulary):
  PACE / senior day:   "pace program", "adult day care", "adult day services",
                       "managed long-term care", "home health", "senior services"
  Senior living:       "senior living", "assisted living", "memory care",
                       "independent living", "continuing care"
  Home-based care:     "home health", "home care", "home-based care", "homemaker services"
  Skilled nursing:     "skilled nursing facility", "long-term care", "nursing home", "rehabilitation"
  Hospice / palliative:"hospice", "palliative care", "end-of-life care"
  Behavioral health:   "behavioral health", "mental health", "outpatient therapy",
                       "substance abuse", "addiction treatment"
  General healthcare:  "managed care", "medicaid", "medicare", "primary care",
                       "ambulatory care", "urgent care", "telehealth"
  Home / trade:        "hvac", "plumbing", "electrical contractor", "facilities management"
  Veterinary:          "veterinary", "animal hospital", "pet care"

RULES:
1. Tags MUST be 1–4 words. Longer phrases will match nothing in Apollo.
2. Choose tags that specifically describe "{niche_description}" operators.
3. Return 2–5 tags as a comma-separated string.
4. You MUST return keywords — do not return an empty string.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Return JSON only — no explanation:
{{"industries": ["Category A", "Category B", "Category C"],
  "keywords":   "short tag one, short tag two, short tag three"}}"""

    def _parse_suggest(content):
        data     = json.loads(content)
        valid    = [i for i in (data.get("industries") or []) if i in APOLLO_INDUSTRIES]
        keywords = (data.get("keywords") or "").strip()
        if not keywords:
            keywords = _fallback_keywords(niche_description)
        return {
            "industries": valid or ["Hospital & Health Care"],
            "keywords":   keywords,
        }

    # Attempt 1 — structured JSON output
    try:
        resp = _openai_create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0,
            timeout=25,
        )
        return _parse_suggest(resp.choices[0].message.content)
    except Exception as e:
        if "content" not in str(e).lower() and "filter" not in str(e).lower() and "400" not in str(e):
            return {
                "industries": ["Hospital & Health Care"],
                "keywords":   _fallback_keywords(niche_description),
            }

    # Attempt 2 — retry without response_format (avoids content-filter on structured output)
    try:
        resp = _openai_create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            timeout=25,
        )
        raw   = resp.choices[0].message.content or ""
        match = re.search(r'\{.*\}', raw, re.DOTALL)
        if match:
            return _parse_suggest(match.group())
    except Exception:
        pass

    return {
        "industries": ["Hospital & Health Care"],
        "keywords":   _fallback_keywords(niche_description),
    }


# ---------------------------------------------------------------------------
# APOLLO — TWO-PASS ORGANIZATION SEARCH
# ---------------------------------------------------------------------------
def search_organizations(industries, location_input, keyword_tags=None, max_pages=10):
    """
    Two-pass search for maximum candidate coverage:

    Pass 1 — Industry sweep (NO keyword filter):
      Search each selected industry broadly so we don't miss companies
      that have the right industry tag but aren't tagged with our keywords.
      AI filter handles relevance.

    Pass 2 — Keyword-only sweep (NO industry filter):
      Search by keyword tags across ALL industries so we catch companies
      that Apollo has classified in an unexpected industry category.

    Both passes are deduplicated by Apollo org ID.
    """
    url     = "https://api.apollo.io/v1/organizations/search"
    headers = {"Content-Type": "application/json", "X-Api-Key": APOLLO_API_KEY}
    all_orgs, seen_ids = [], set()

    def _fetch_pages(base_payload):
        for page in range(1, max_pages + 1):
            payload = {**base_payload, "page": page, "per_page": 100}
            _apollo_limiter.wait()
            try:
                r = requests.post(url, headers=headers, json=payload, timeout=15)
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

    # Pass 1: broad industry sweeps — no keyword restriction
    for industry in (industries or [None]):
        base = {"organization_locations": [location_input]}
        if industry:
            base["q_organization_industries"] = [industry]
        _fetch_pages(base)

    # Pass 2: keyword-only sweep — catches misclassified companies
    if keyword_tags:
        _fetch_pages({
            "organization_locations":        [location_input],
            "q_organization_keyword_tags":   keyword_tags,
        })

    return all_orgs


def web_discovery_pass(niche, geography, seen_domains, seen_names):
    """
    Pass 3: Scrape Google page 1, page 2, and the Places tab to catch companies
    that Apollo doesn't have or hasn't tagged with the expected industry/keywords.
    Returns a list of org-like dicts compatible with process_single_company.
    """
    query = f"{niche} {geography}"

    _UA = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )

    search_urls = [
        f"https://www.google.com/search?q={quote_plus(query)}&num=20",            # Page 1
        f"https://www.google.com/search?q={quote_plus(query)}&num=20&start=20",   # Page 2
        f"https://www.google.com/search?q={quote_plus(query)}&tbm=lcl",           # Places tab
    ]

    def fetch_one(url):
        content = firecrawl_scrape(url)
        if content and len(content) >= 200:
            return content
        try:
            r = requests.get(url, headers={"User-Agent": _UA}, timeout=15)
            if r.status_code == 200 and len(r.text) > 500:
                return r.text[:30000]
        except Exception:
            pass
        return ""

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as ex:
        results = list(ex.map(fetch_one, search_urls))

    combined = "\n\n---NEW SOURCE---\n\n".join(r for r in results if r)
    if not combined or len(combined) < 200:
        return []

    extract_prompt = f"""From these Google search results (pages 1 & 2 plus the Places/local tab),
extract every company that appears to be an actual operator or provider in "{niche}"
located in or near "{geography}".

Local/Places listings may show a business name, address, phone number, and star rating
— include those even if no website URL is present.

Return JSON only:
{{"companies": [
  {{"name": "Company Name", "website": "https://example.com or blank",
    "city": "City", "state": "ST", "snippet": "What they do"}}
]}}

Rules:
- Only include actual operating companies in the niche (not vendors, consultants, or tech firms)
- Do NOT include directories, news articles, government agencies, Wikipedia, or ad listings
- Use the company's own website URL when visible; leave blank if not found
- Deduplicate — if the same company appears multiple times, include it only once
- Return {{"companies": []}} if none found

Search content:
{combined[:25000]}"""

    companies = []
    try:
        resp = _openai_create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": extract_prompt}],
            response_format={"type": "json_object"},
            timeout=30,
        )
        data = json.loads(resp.choices[0].message.content)
        companies = data.get("companies") or []
    except Exception as e:
        if "content" not in str(e).lower() and "filter" not in str(e).lower() and "400" not in str(e):
            return []
        try:
            resp = _openai_create(
                model=OPENAI_MODEL,
                messages=[{"role": "user", "content": extract_prompt}],
                timeout=30,
            )
            raw = resp.choices[0].message.content or ""
            match = re.search(r'\{.*\}', raw, re.DOTALL)
            if match:
                data = json.loads(match.group())
                companies = data.get("companies") or []
        except Exception:
            return []

    # Deduplicate against Apollo results and build org-like dicts
    new_orgs = []
    for c in companies:
        if not isinstance(c, dict) or not c.get("name"):
            continue
        name_lower = c["name"].strip().lower()
        if name_lower in seen_names:
            continue
        domain = clean_domain(c.get("website"))
        if domain and domain in seen_domains:
            continue

        org = {
            "id":                       None,
            "name":                     c["name"].strip(),
            "website_url":              c.get("website"),
            "city":                     c.get("city"),
            "state":                    c.get("state"),
            "linkedin_url":             None,
            "estimated_num_employees":  None,
            "short_description":        c.get("snippet") or "",
            "headline":                 "",
            "keywords":                 [],
            "ownership_status":         None,
        }
        new_orgs.append(org)
        if domain:
            seen_domains.add(domain)
        seen_names.add(name_lower)

    return new_orgs

# ---------------------------------------------------------------------------
# FILTERS
# ---------------------------------------------------------------------------
_PE_VC_SIGNALS = [
    "private equity", "venture capital", "portfolio company", "backed by",
    "pe-backed", "vc-backed", "pe backed", "vc backed", "growth equity",
    "buyout", "recapitalization", "capital partners", "equity partners",
    "investment partners", "apax", "bain capital", "carlyle", "kkr",
    "blackstone", "thoma bravo", "insight partners", "warburg pincus",
    "silver lake", "vista equity", "hellman & friedman", "permira",
    "advent international", "general atlantic", "summit partners",
    "series a", "series b", "series c", "series d", "series e",
    "funding round", "raised $", "investment from",
]


def is_buyable_structure(org, mode):
    emp    = org.get("estimated_num_employees", 0) or 0
    status = str(org.get("ownership_status") or "").strip().lower()
    tags   = [t.lower() for t in (org.get("keywords") or [])]

    if mode == "A":
        if status == "public":                                  return False, "Publicly Traded"
        if status == "subsidiary":                              return False, "Subsidiary"
        if "private equity" in tags or "venture capital" in tags: return False, "PE/VC Backed (tags)"
        if emp > 7500:                                          return False, f"Too Large ({emp})"

        # Deeper Apollo field scan for PE/VC signals
        desc = (org.get("short_description") or org.get("headline") or "").lower()
        tag_str = " ".join(tags)
        combined = f"{desc} {tag_str} {status}"
        for signal in _PE_VC_SIGNALS:
            if signal in combined:
                return False, f"PE/VC Backed ('{signal}' in Apollo data)"

    else:  # Mode B — block public companies and large conglomerates
        if "public" in status:                                  return False, "Publicly Traded"
        if emp > 10000:                                         return False, f"Too Large ({emp})"

    return True, "OK"


def check_pe_vc_web(company_name, domain):
    """
    Web-based PE/VC ownership check for Strategy A.
    Uses Firecrawl to scrape company pages + Crunchbase profile to detect
    private equity or venture capital backing that Apollo data missed.
    Returns (is_pe_vc: bool, reason: str).
    """
    snippets = []

    # 1. Scrape company's own about/investor pages for PE/VC mentions
    if domain:
        pe_paths = ["/about", "/about-us", "/investors", "/company"]
        for path in pe_paths:
            content = firecrawl_scrape(f"https://{domain}{path}")
            if content and len(content) >= 100:
                snippets.append(content[:8000])

    # 2. Scrape Crunchbase profile (reliable PE/VC data source)
    cb_slug = company_name.lower().replace(" ", "-").replace(",", "").replace(".", "")
    for slug in [cb_slug, cb_slug.split("-")[0]]:
        content = firecrawl_scrape(f"https://www.crunchbase.com/organization/{slug}")
        if content and len(content) >= 200:
            snippets.append(f"CRUNCHBASE PROFILE:\n{content[:10000]}")
            break

    # 3. Check known PE/VC firm portfolio pages if their name appears
    #    in the company's own website content
    _known_pe_portfolios = {
        "apax": "https://www.apax.com/portfolio/",
        "bain capital": "https://www.baincapital.com/portfolio",
        "carlyle": "https://www.carlyle.com/portfolio",
        "kkr": "https://www.kkr.com/businesses/private-equity/portfolio",
        "blackstone": "https://www.blackstone.com/portfolio/",
        "thoma bravo": "https://www.thomabravo.com/companies",
        "insight partners": "https://www.insightpartners.com/portfolio/",
        "warburg pincus": "https://www.warburgpincus.com/investments/",
        "silver lake": "https://www.silverlake.com/portfolio/",
        "vista equity": "https://www.vistaequitypartners.com/companies/",
        "hellman & friedman": "https://www.hfriedman.com/portfolio",
        "permira": "https://www.permira.com/portfolio",
        "advent international": "https://www.adventinternational.com/portfolio/",
        "general atlantic": "https://www.generalatlantic.com/portfolio/",
        "summit partners": "https://www.summitpartners.com/companies",
        "new capital partners": "https://www.newcapitalpartners.com/portfolio",
        "bv investment": "https://www.bvinvestmentpartners.com/portfolio",
    }
    snippet_text = " ".join(snippets).lower()
    for firm_name, portfolio_url in _known_pe_portfolios.items():
        if firm_name in snippet_text:
            content = firecrawl_scrape(portfolio_url)
            if content and len(content) >= 100:
                snippets.append(f"PE FIRM PORTFOLIO ({firm_name}):\n{content[:8000]}")

    if not snippets:
        return False, "No web data"

    combined = "\n---\n".join(snippets)
    prompt = f"""Determine whether "{company_name}" is owned by or has received significant
investment from a private equity firm or venture capital firm.

Look for evidence such as:
- "backed by [PE/VC firm name]"
- "portfolio company of [firm]"
- "acquired by [firm]"
- Company name appearing on a PE/VC firm's portfolio page
- Crunchbase listing showing PE/VC investors or funding rounds
- Series A/B/C/D funding rounds
- Recapitalization or leveraged buyout
- Any named PE or VC firm as an investor or owner

IMPORTANT: If "{company_name}" appears on any PE/VC firm's portfolio page, that is
definitive proof of PE/VC ownership — return true.

Web content:
{combined[:25000]}

Return JSON only:
{{"pe_vc_owned": true/false, "evidence": "one sentence explaining why, or 'No evidence found'"}}"""

    try:
        resp = _openai_create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            timeout=20,
        )
        data = json.loads(resp.choices[0].message.content)
        return data.get("pe_vc_owned", False), data.get("evidence", "")
    except Exception:
        pass

    try:
        resp = _openai_create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            timeout=20,
        )
        raw = resp.choices[0].message.content or ""
        match = re.search(r'\{.*\}', raw, re.DOTALL)
        if match:
            data = json.loads(match.group())
            return data.get("pe_vc_owned", False), data.get("evidence", "")
    except Exception:
        pass

    return False, "AI check failed"


def check_news_for_pe_vc(news_title):
    """Quick keyword scan of news headline for PE/VC signals."""
    if not news_title:
        return False
    t = news_title.lower()
    signals = [
        "private equity", "venture capital", "pe-backed", "vc-backed",
        "acquired by", "acquisition", "buyout", "recapitalization",
        "series a", "series b", "series c", "series d",
        "funding round", "raises $", "raised $", "secures $",
        "investment from", "growth equity", "portfolio company",
    ]
    return any(s in t for s in signals)


_UNIVERSAL_BLOCKS = [
    "university", "college", "food service", "catering",
    "staffing solutions", "temp agency",
    # Transportation / logistics — not care operators
    " transportation", "non-emergency transport", "medical transport",
    "nemt ", " logistics",
    # Large food-service management companies
    "eurest", "aramark", "sodexo", "compass group", "canteen",
]
_MODE_A_BLOCKS = [
    "consulting group", "advisory group", " billing services",
    "software inc", "software llc",
    # Tech firms that slip through on healthcare-adjacent keywords
    "healthtech", " technologies", "tech solutions", " it services",
]

def is_obvious_mismatch(org, target_niche, mode):
    name = (org.get("name") or "").lower()
    for f in _UNIVERSAL_BLOCKS:
        if f in name: return True, f"Block: '{f}'"
    if mode == "B": return False, "Pass"
    extras = list(_MODE_A_BLOCKS)
    if "architect" in target_niche.lower():
        extras += ["realty", "real estate", "lumber", "golf course"]
    for f in extras:
        if f in name: return True, f"Mode-A block: '{f}'"
    return False, "Pass"


def check_relevance_gpt4o(company_name, description, keywords, target_niche, mode):
    if mode == "A":
        prompt = f"""You are an acquisition filter for a private equity investor.
Target niche: "{target_niche}"

Company: "{company_name}"
Description: "{description}"
Keywords: {keywords}

PASS if the company fits ONE of these:
1. A direct operator or service provider in the exact niche described above.
2. A highly adjacent operator serving the same core customer base in a directly related vertical.
3. Description is vague, but the company name and keywords strongly align with the target niche.

FAIL if any of these apply:
- Software, SaaS, analytics, HealthTech, or pure technology vendor — even if health-adjacent
- IT services, digital health platform, or software development firm
- Consulting, billing, staffing, marketing, or outsourced services firm
- Training, education, or coaching organization with no direct patient/client care operations
- Medical transportation, non-emergency medical transport (NEMT), or logistics company
- Large national chain or massive enterprise not suitable for acquisition
- Completely unrelated industry
- "PACE" in the niche means Program for All-Inclusive Care for the Elderly — NOT fitness/pace

If the company appears to be a legitimate local or regional operator in the target niche, PASS them.
Do not over-filter if the description is brief.
Return JSON only: {{"match": true/false, "reason": "one sentence"}}"""

    else:
        prompt = f"""You are identifying realistic sales prospects and direct competitors for companies in: "{target_niche}"

Company: "{company_name}"
Description: "{description}"
Keywords: {keywords}

PASS if the company fits ONE of these:
1. Direct competitor — operates in the same niche or a closely adjacent one (any size).
2. Realistic sales prospect — a similarly-scaled operator in the same sector who could refer
   clients to, partner with, or purchase services from companies in this niche.

FAIL if any of these apply:
- Insurance company, managed care payer, financial services firm, or health insurer
- Large institutional network, national chain, or conglomerate with 10,000+ employees, UNLESS
  it is a direct competitor operating in the exact same niche as the target
- Technology company, IT firm, HealthTech, software developer, or digital health platform —
  even if their product serves the healthcare sector
- Medical transportation, non-emergency medical transport (NEMT), or logistics company
- Consulting, billing, staffing, training, or outsourced services firm
- Completely unrelated industry (manufacturing, finance, retail, food service, etc.)

Return JSON only: {{"match": true/false, "reason": "one sentence"}}"""

    def _parse_relevance(content):
        data = json.loads(content)
        return data.get("match"), data.get("reason")

    # Attempt 1 — structured JSON output
    try:
        resp = _openai_create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            timeout=20,
        )
        return _parse_relevance(resp.choices[0].message.content)
    except Exception as e:
        if "content" not in str(e).lower() and "filter" not in str(e).lower() and "400" not in str(e):
            return True, "AI Error"

    # Attempt 2 — retry without response_format
    try:
        resp = _openai_create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            timeout=20,
        )
        raw   = resp.choices[0].message.content or ""
        match = re.search(r'\{.*\}', raw, re.DOTALL)
        if match:
            return _parse_relevance(match.group())
    except Exception:
        pass

    return True, "AI Error"


# ---------------------------------------------------------------------------
# CONTACT FINDING — FIRECRAWL
# ---------------------------------------------------------------------------
def firecrawl_scrape(url):
    _firecrawl_limiter.wait()
    try:
        r = requests.post(
            "https://api.firecrawl.dev/v1/scrape",
            headers={"Authorization": f"Bearer {FIRECRAWL_API_KEY}",
                     "Content-Type": "application/json"},
            json={"url": url, "formats": ["markdown"]},
            timeout=20,
        )
        if r.status_code == 200:
            data = r.json()
            md   = (data.get("data") or {}).get("markdown") or data.get("markdown")
            return md[:50000] if md else None
    except Exception:
        pass
    return None


def extract_relevant_links(md, base_url):
    if not md: return []
    high = ["leadership","executive","our team","care team","management",
            "principals","partners","providers","medical staff","administration"]
    med  = ["about","who we are","meet","staff","firm","studio","people","team","contact"]
    skip = ["linkedin","facebook","twitter","pdf","jpg","login","mailto"]
    seen, out = set(), []
    for text, link in re.findall(r'\[([^\]]+)\]\(([^)]+)\)', md):
        t     = text.lower()
        score = 3 if any(x in t for x in high) else (1 if any(x in t for x in med) else 0)
        if not score: continue
        full = urljoin(base_url, link) if link.startswith("/") else \
               (link if link.startswith("http") else None)
        if not full or any(s in full for s in skip) or full in seen: continue
        seen.add(full)
        out.append((score, full))
    out.sort(key=lambda x: x[0], reverse=True)
    return [u for _, u in out[:4]]


def extract_names_openai(text, company_name):
    prompt = f"""From the website text of "{company_name}", extract the primary leader
(CEO, Owner, Founder, President, Principal, Administrator, Executive Director,
Medical Director, or equivalent top role) plus any visible contact info.

Return JSON only (use null when not found):
{{"name": "Full Name or null",
  "title": "Their Title or null",
  "email": "email@example.com or null",
  "phone": "phone number or null"}}

Text:
{text[:15000]}"""
    def _parse_contact(content):
        data = json.loads(content)
        for k in ("name", "title", "email", "phone"):
            if isinstance(data.get(k), str) and data[k].lower() in ("none", "n/a", "null", ""):
                data[k] = None
        return data

    # Attempt 1 — structured JSON output
    try:
        resp = _openai_create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            timeout=20,
        )
        return _parse_contact(resp.choices[0].message.content)
    except Exception as e:
        if "content" not in str(e).lower() and "filter" not in str(e).lower() and "400" not in str(e):
            return None

    # Attempt 2 — retry without response_format
    try:
        resp = _openai_create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            timeout=20,
        )
        raw   = resp.choices[0].message.content or ""
        match = re.search(r'\{.*\}', raw, re.DOTALL)
        if match:
            return _parse_contact(match.group())
    except Exception:
        pass

    return None


def spider_for_contact(company_name, domain):
    if not domain:
        return None, None, None, None

    base      = f"https://{domain}"
    web_email = web_phone = None

    for path in _CONTACT_PATHS:
        content = firecrawl_scrape(base + path)
        if not content or len(content) < 100: continue
        ai = extract_names_openai(content, company_name)
        if not ai: continue
        if ai.get("email"): web_email = ai["email"]
        if ai.get("phone"): web_phone = ai["phone"]
        n = ai.get("name")
        if n and " " in n and len(n) > 3:
            person = {
                "first_name":    n.split()[0],
                "last_name":     " ".join(n.split()[1:]),
                "title":         ai.get("title"),
                "email":         web_email,
                "phone_numbers": [{"sanitized_number": web_phone}] if web_phone else [],
            }
            return person, f"Web Path ({path})", web_email, web_phone

    visited, queue = set(), [base]
    for url in queue[:6]:
        if url in visited: continue
        visited.add(url)
        content = firecrawl_scrape(url)
        if not content or len(content) < 100: continue
        ai = extract_names_openai(content, company_name)
        if ai:
            if ai.get("email"): web_email = ai["email"]
            if ai.get("phone"): web_phone = ai["phone"]
            n = ai.get("name")
            if n and " " in n and len(n) > 3:
                person = {
                    "first_name":    n.split()[0],
                    "last_name":     " ".join(n.split()[1:]),
                    "title":         ai.get("title"),
                    "email":         web_email,
                    "phone_numbers": [{"sanitized_number": web_phone}] if web_phone else [],
                }
                return person, "Web Spider", web_email, web_phone
        for lnk in extract_relevant_links(content, url):
            if lnk not in visited: queue.insert(1, lnk)

    return None, None, web_email, web_phone


# ---------------------------------------------------------------------------
# CONTACT FINDING — APOLLO PEOPLE
# ---------------------------------------------------------------------------
def clean_domain(url):
    if not url or not isinstance(url, str): return None
    try:
        if not url.startswith("http"): url = "http://" + url
        d = urlparse(url).netloc
        return d[4:] if d.startswith("www.") else d
    except Exception:
        return None


def clean_company_name_for_search(name):
    if not name: return ""
    c = name.replace(",", "").replace(".", "")
    for s in [" inc"," llc"," group"," ltd"," corp"," p.c."," pc",
              " architects"," architecture"]:
        if c.lower().endswith(s): c = c[:-len(s)]
    return c.strip()


def _title_score(title: str) -> int:
    t = (title or "").lower()
    for phrase, score in _TITLE_SCORES.items():
        if phrase in t: return score
    return 0


def get_people_apollo_robust(company_name, domain, org_id=None):
    url     = "https://api.apollo.io/v1/mixed_people/search"
    headers = {"Content-Type": "application/json", "X-Api-Key": APOLLO_API_KEY}
    name    = clean_company_name_for_search(company_name)

    top_seniority  = ["owner", "founder", "c_suite", "president"]
    wide_seniority = ["owner", "founder", "c_suite", "president", "vp", "partner", "manager"]

    attempts = []
    if org_id:
        attempts += [
            {"organization_ids": [org_id], "person_seniority": top_seniority,  "per_page": 10},
            {"organization_ids": [org_id], "person_seniority": wide_seniority, "per_page": 25},
            {"organization_ids": [org_id],                                      "per_page": 25},
        ]
    if domain:
        domains = list({domain, f"www.{domain}",
                        domain[4:] if domain.startswith("www.") else domain})
        for d in domains:
            attempts += [
                {"q_organization_domains": [d], "person_seniority": top_seniority,  "per_page": 10},
                {"q_organization_domains": [d], "person_seniority": wide_seniority, "per_page": 25},
            ]
    if name:
        attempts += [
            {"q_organization_names": [name], "person_seniority": top_seniority,  "per_page": 10},
            {"q_organization_names": [name], "person_seniority": wide_seniority, "per_page": 15},
            {"q_organization_names": [name],                                      "per_page": 15},
        ]

    all_people, seen_ids = [], set()
    for payload in attempts:
        _apollo_limiter.wait()
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=10)
            if r.status_code == 200:
                people = r.json().get("people", [])
                new    = [p for p in people
                          if p.get("id") and p["id"] not in seen_ids
                          and p.get("first_name") and p.get("last_name")]
                for p in new: seen_ids.add(p["id"])
                all_people.extend(new)
                if any(_title_score(p.get("title")) >= 80 for p in all_people):
                    break
        except Exception:
            pass

    all_people.sort(
        key=lambda p: (_title_score(p.get("title")), bool(p.get("email"))),
        reverse=True,
    )
    return all_people


def select_best_apollo_contact(people):
    if not people: return None, "None"
    valid = [p for p in people
             if p.get("first_name") and p.get("last_name")
             and str(p.get("last_name","")).strip().lower() not in ("none","n/a","")]
    if not valid: return None, "None"
    scored = [(p, _title_score(p.get("title"))) for p in valid]
    scored.sort(key=lambda x: (x[1], bool(x[0].get("email"))), reverse=True)
    best, score = scored[0]
    label = "Apollo (Top)" if score >= 50 else "Apollo (Best Available)"
    if not best.get("email"): label += " [No Email]"
    return best, label


def repair_single_name(first_name, people_list):
    if not first_name or not people_list: return None
    target = first_name.split()[0].lower()
    for p in people_list:
        if target in (p.get("first_name") or "").lower(): return p
    return None


def bulk_enrich_names(people_list, domain):
    if not people_list or not domain: return []
    _apollo_limiter.wait()
    try:
        r = requests.post(
            "https://api.apollo.io/v1/people/bulk_match",
            headers={"Content-Type": "application/json", "X-Api-Key": APOLLO_API_KEY},
            json={"details": [{"first_name": p.get("first_name"),
                               "last_name":  p.get("last_name"),
                               "domain":     domain} for p in people_list]},
            timeout=15,
        )
        return r.json().get("matches", [])
    except Exception:
        return []


# ---------------------------------------------------------------------------
# NEWS
# ---------------------------------------------------------------------------
def get_latest_news_link(company_name, city=None):
    q   = f"{company_name} {city}" if city else company_name
    rss = f"https://news.google.com/rss/search?q={quote_plus(q)}&hl=en-US&gl=US&ceid=US:en"
    try:
        r = requests.get(rss, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code != 200: return None, None
        root  = ET.fromstring(r.content)
        items = root.findall("./channel/item")
        if not items: return None, None
        return (items[0].findtext("title") or "").strip(), \
               (items[0].findtext("link")  or "").strip()
    except Exception:
        return None, None


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------
def _email_matches_domain(email: str, company_domain: str) -> bool:
    """Return True if the email's domain plausibly belongs to the company domain.
    Used to reject Apollo bulk_enrich returning a same-name person at a different company."""
    if not email or not company_domain:
        return True
    try:
        e_dom = email.split("@")[-1].lower().lstrip("www.")
        c_dom = company_domain.lower().lstrip("www.")
        return (e_dom == c_dom
                or e_dom.endswith("." + c_dom)
                or c_dom.endswith("." + e_dom))
    except Exception:
        return True


# ---------------------------------------------------------------------------
# WORKER
# ---------------------------------------------------------------------------
def process_single_company(org, specific_niche, strat_code):
    comp_name = org.get("name")

    if not is_buyable_structure(org, strat_code)[0]:            return None
    if is_obvious_mismatch(org, specific_niche, strat_code)[0]: return None

    desc = org.get("short_description") or org.get("headline") or ""
    tags = org.get("keywords") or []
    if not check_relevance_gpt4o(comp_name, desc, tags, specific_niche, strat_code)[0]:
        return None

    domain = clean_domain(org.get("website_url"))
    org_id = org.get("id")

    # Strategy A: web-based PE/VC ownership check before expensive contact work
    if strat_code == "A":
        is_pe_vc, pe_reason = check_pe_vc_web(comp_name, domain)
        if is_pe_vc:
            return None

    row = {
        "Company":        comp_name,
        "Website":        org.get("website_url"),
        "City":           org.get("city"),
        "State":          org.get("state"),
        "LinkedIn":       org.get("linkedin_url"),
        "Employees":      org.get("estimated_num_employees"),
        "CEO/Owner Name": "N/A",
        "Title":          "N/A",
        "Email":          "N/A",
        "Phone":          "N/A",
        "Source":         "Not Found",
        "Notes":          "",
        "Confidence":     "Low",
        "Latest News":    "N/A",
    }

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as inner:
        apollo_future = inner.submit(get_people_apollo_robust, comp_name, domain, org_id)
        web_future    = inner.submit(spider_for_contact, comp_name, domain)
        apollo_people                                 = apollo_future.result()
        web_person, web_source, web_email, web_phone = web_future.result()

    found_person = None

    if web_person:
        found_person      = web_person
        row["Source"]     = web_source or "Web Spider"
        row["Confidence"] = "Medium"   # named person found — upgrade to High if verified
        full = f"{found_person.get('first_name','')} {found_person.get('last_name','')}".strip()
        if " " not in full:
            rep = repair_single_name(full, apollo_people)
            if rep: found_person = rep; row["Source"] = "Web → Apollo Repaired"
        if domain:
            matches = bulk_enrich_names([found_person], domain)
            if matches and matches[0]:
                enriched       = matches[0]
                enr_email      = enriched.get("email")
                # Reject if Apollo matched a same-name person at a different company
                if not enr_email or _email_matches_domain(enr_email, domain):
                    found_person      = enriched
                    row["Source"]    += " → Verified"
                    row["Confidence"] = "High"

    if not found_person and apollo_people:
        best, method = select_best_apollo_contact(apollo_people)
        if best:
            found_person      = best
            row["Source"]     = method
            row["Confidence"] = "Medium"

    if found_person:
        row["CEO/Owner Name"] = (
            f"{found_person.get('first_name','')} "
            f"{found_person.get('last_name','')}").strip()
        row["Title"] = found_person.get("title") or "N/A"
        a_email = found_person.get("email")
        # Discard email if it clearly belongs to a different company's domain
        if a_email and domain and not _email_matches_domain(a_email, domain):
            a_email = None
        row["Email"] = a_email if a_email else (web_email or "N/A")
        pnums   = found_person.get("phone_numbers") or []
        a_phone = pnums[0].get("sanitized_number") if pnums else None
        row["Phone"] = a_phone if a_phone else (web_phone or "N/A")
        if found_person.get("notes"): row["Notes"] = found_person["notes"]
    else:
        if web_email: row["Email"] = web_email
        if web_phone: row["Phone"] = web_phone
        if web_email or web_phone:
            row["Confidence"] = "Medium"
            row["Source"]     = "Web (contact only)"

    t, u = get_latest_news_link(comp_name, org.get("city"))
    if u: row["Latest News"] = f"{t} | {u}" if t else u

    # Final PE/VC gate for Strategy A: check news headline for funding/acquisition signals
    if strat_code == "A" and check_news_for_pe_vc(t):
        return None

    return row


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------
st.title("🚀 NCP Sourcing Engine")
st.caption("Describe your target → AI suggests search fields → source at scale.")

# ── Step 1: Niche input + Suggest Fields ────────────────────────────────────
st.markdown("### Step 1 — What are you looking for?")
n1, n2 = st.columns([5, 1])
niche_raw = n1.text_input(
    "Niche",
    placeholder="e.g.  PACE programs for elderly  |  commercial HVAC contractors  |  veterinary practices",
    label_visibility="collapsed",
    key="niche_raw_input",
)
suggest_clicked = n2.button("🔍 Suggest Fields", use_container_width=True)

if suggest_clicked:
    if not (niche_raw or "").strip():
        st.warning("Please describe your target niche first.")
    else:
        with st.spinner(f"Analysing '{niche_raw}' and mapping to Apollo parameters…"):
            s = suggest_search_params(niche_raw)
            st.session_state["s_industries"] = s["industries"]
            st.session_state["s_keywords"]   = s["keywords"]
            st.session_state["s_niche"]      = niche_raw
        st.rerun()

# Banner — show what was suggested (only after Suggest Fields has run)
if "s_industries" in st.session_state:
    ind_str = ", ".join(st.session_state["s_industries"])
    kw_str  = st.session_state.get("s_keywords") or "(none — broaden if needed)"
    st.success(
        f"**Industries:** {ind_str}\n\n"
        f"**Keywords:** {kw_str}\n\n"
        f"Review and adjust below, then click **Start Sourcing**."
    )

st.divider()

# ── Step 2: Review / adjust fields ──────────────────────────────────────────
st.markdown("### Step 2 — Review, adjust, and run")

# Initialise session state defaults so widgets don't crash on first load
for k, v in [("s_industries", ["Hospital & Health Care"]),
             ("s_keywords",   ""),
             ("s_niche",      "")]:
    if k not in st.session_state:
        st.session_state[k] = v

r1a, r1b = st.columns(2)
industries = r1a.multiselect(
    "Apollo Industry Categories",
    options=APOLLO_INDUSTRIES,
    key="s_industries",
)
specific_niche = r1b.text_input(
    "Specific Niche (AI Filter)",
    key="s_niche",
    help="Plain-English description used by the AI relevance filter — be specific.",
)

r2a, r2b, r2c = st.columns(3)
target_geo = r2a.text_input("Geography", value="North Carolina, United States")
mode = r2b.selectbox("Strategy", [
    "A - Acquire  (Strict: small private operators only)",
    "B - Prospect (Broad: competitors & referral/sales targets, all sizes)",
])
apollo_keywords_raw = r2c.text_input(
    "Apollo Keyword Tags",
    key="s_keywords",
    help=(
        "Short 1–4 word tags only — longer phrases match nothing. "
        "Leave blank to search by industry alone (broadest results). "
        "Examples: pace program, adult day care, home health"
    ),
)

st.caption(
    "**Search strategy:** Each industry is swept up to 1,000 results (no keyword filter) so "
    "broadly-classified companies aren't missed. Keywords run a *separate* sweep across ALL "
    "industries to catch companies Apollo has placed in unexpected categories. "
    "A Google discovery pass then scrapes page 1 of Google to catch companies that "
    "Apollo doesn't have at all. The AI filter screens every candidate for true niche relevance."
)

if st.button("🚀 Start Sourcing", type="primary"):
    if not industries:
        st.error("Please select at least one industry, or click **Suggest Fields** first.")
        st.stop()

    strat_code   = "A" if "A -" in mode else "B"
    keyword_tags = [k.strip() for k in apollo_keywords_raw.split(",") if k.strip()] or None

    kw_display = f" + keyword sweep ({', '.join(keyword_tags)})" if keyword_tags else ""
    st.info(
        f"🔎 Searching **{len(industries)} industries**{kw_display} "
        f"in **{target_geo}** (up to 1,000 results per industry)…"
    )

    try:
        orgs = search_organizations(industries, target_geo, keyword_tags=keyword_tags)
    except Exception as e:
        st.error(f"Apollo API error: {e}")
        st.stop()

    # Build dedup sets for Google discovery pass
    seen_domains = set()
    seen_names   = set()
    for o in orgs:
        d = clean_domain(o.get("website_url"))
        if d: seen_domains.add(d)
        n = (o.get("name") or "").strip().lower()
        if n: seen_names.add(n)

    # Pass 3: Web scrape to catch companies Apollo missed
    google_orgs = []
    with st.spinner("Checking Google/DuckDuckGo/Bing for companies Apollo may have missed…"):
        try:
            google_orgs = web_discovery_pass(specific_niche, target_geo,
                                             seen_domains, seen_names)
        except Exception as e:
            st.warning(f"Web discovery pass failed (continuing with Apollo results): {e}")
    if google_orgs:
        orgs.extend(google_orgs)
        st.info(f"🔍 Web discovery added **{len(google_orgs)}** companies not in Apollo.")

    if not orgs:
        st.error(
            "No companies found in Apollo or Google. "
            "Try broadening the industry selection or clearing the Keywords field."
        )
        st.stop()

    st.success(
        f"Found **{len(orgs)}** unique candidates ({len(orgs) - len(google_orgs) if google_orgs else len(orgs)} "
        f"Apollo + {len(google_orgs) if google_orgs else 0} Google) — "
        f"running AI filter with 5 parallel workers…"
    )
    progress_bar = st.progress(0)
    status_text  = st.empty()
    final_data   = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
        futures = {
            ex.submit(process_single_company, org, specific_niche, strat_code): org
            for org in orgs
        }
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            try:
                result = future.result()
                if result: final_data.append(result)
            except Exception:
                pass  # skip companies that error out
            progress_bar.progress((i + 1) / len(orgs))
            status_text.caption(
                f"Processed {i+1}/{len(orgs)} | {len(final_data)} passed so far…"
            )

    status_text.write("✅ Sourcing complete!")

    if final_data:
        df  = pd.DataFrame(final_data)
        st.dataframe(df)
        csv   = df.to_csv(index=False).encode("utf-8")
        fname = (
            f"NCP_{'_'.join(industries[:2])}_{target_geo}.csv"
            .replace(" ", "_").replace(",", "")
        )
        st.download_button(
            "Download CSV", data=csv, file_name=fname, mime="text/csv", type="primary"
        )
    else:
        st.warning(
            "No targets passed the filters.\n\n"
            "**Tips:**\n"
            "- Clear the Keywords field and retry (keywords narrow Apollo results)\n"
            "- Click **Suggest Fields** for AI-recommended parameters\n"
            "- Switch to **Mode B** for a broader sweep\n"
            "- Add more industry categories"
        )
