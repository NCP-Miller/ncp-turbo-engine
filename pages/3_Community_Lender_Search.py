import streamlit as st
import pandas as pd
import requests
import json
import concurrent.futures
import threading
import time as _time
from urllib.parse import urlparse
from openai import OpenAI

st.set_page_config(page_title="NCP Community Lender Search", page_icon="🏦", layout="wide")

# ---------------------------------------------------------------------------
# RATE LIMITERS
# ---------------------------------------------------------------------------
class _RateLimiter:
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

_apollo_limiter = _RateLimiter(4)
_firecrawl_limiter = _RateLimiter(2)

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
    _oai = OpenAI(api_key=st.secrets["OPENAI_API_KEY"], timeout=30.0)
except (FileNotFoundError, KeyError):
    st.error("OPENAI_API_KEY missing.")
    st.stop()

try:
    FIRECRAWL_API_KEY = st.secrets["FIRECRAWL_API_KEY"]
except (FileNotFoundError, KeyError):
    st.error("FIRECRAWL_API_KEY missing.")
    st.stop()

# ---------------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------------
_STATES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
    "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming", "DC": "District of Columbia",
}
_STATE_NAME_TO_ABBREV = {v.lower(): k for k, v in _STATES.items()}

ROLE_BUCKETS = {
    "CLO / Head of Commercial Banking": [
        "chief lending officer", "chief credit officer",
        "head of commercial banking", "director of commercial banking",
        "evp commercial banking", "svp commercial banking",
        "commercial banking director", "chief banking officer",
    ],
    "Market President / Regional President": [
        "market president", "regional president", "city president",
        "area president", "community president", "division president",
        "market executive", "regional executive",
    ],
    "Head of Private Banking / Wealth / Trust": [
        "private banking", "wealth management director",
        "head of wealth", "trust officer", "trust director",
        "private client", "trust services director",
        "wealth advisor", "private banking director",
    ],
    "Senior Commercial Lenders": [
        "commercial relationship manager", "senior relationship manager",
        "svp commercial", "senior commercial banker",
        "commercial banker", "senior lender",
        "commercial loan officer", "relationship manager",
    ],
}

_IB_SCAN_PATHS = [
    "/investment-banking", "/capital-markets", "/corporate-finance",
    "/services", "/our-services", "/what-we-do",
    "/about", "/about-us",
]

# ---------------------------------------------------------------------------
# LOCATION PARSING
# ---------------------------------------------------------------------------
def _resolve_state(text):
    text = text.strip()
    if len(text) == 2 and text.upper() in _STATES:
        return text.upper()
    return _STATE_NAME_TO_ABBREV.get(text.lower())


def _parse_location(text):
    """Parse free-text location into (city_or_None, state_abbrev_or_None)."""
    text = text.strip()
    if not text:
        return None, None
    if "," in text:
        parts = [p.strip() for p in text.split(",", 1)]
        city = parts[0] if parts[0] else None
        state_code = _resolve_state(parts[1])
        return city, state_code
    state_code = _resolve_state(text)
    if state_code:
        return None, state_code
    return text, None


def _clean_domain(url):
    if not url or not isinstance(url, str):
        return None
    url = url.strip()
    if not url.startswith("http"):
        url = "http://" + url
    try:
        d = urlparse(url).netloc
        return d[4:] if d.startswith("www.") else d
    except Exception:
        return None


# ---------------------------------------------------------------------------
# STAGE 1A: FDIC BANK DISCOVERY
# ---------------------------------------------------------------------------
@st.cache_data(ttl=3600, show_spinner=False)
def _search_fdic(state_code, city=None):
    """Query FDIC BankFind API for active banks."""
    url = "https://banks.data.fdic.gov/api/institutions"
    filters = f'STALP:"{state_code}" AND ACTIVE:1'
    if city:
        filters += f' AND CITY:"{city.upper()}"'
    params = {
        "filters": filters,
        "fields": "CERT,INSTNAME,CITY,STALP,WEBADDR,ASSET",
        "limit": 5000,
        "sort_by": "ASSET",
        "sort_order": "DESC",
    }
    try:
        r = requests.get(url, params=params, timeout=15)
        if r.status_code != 200:
            return []
        items = r.json().get("data", [])
        results = []
        for item in items:
            d = item.get("data", item)
            results.append({
                "name": (d.get("INSTNAME") or "").strip(),
                "city": (d.get("CITY") or "").strip(),
                "state": d.get("STALP", state_code),
                "website": (d.get("WEBADDR") or "").strip(),
                "assets_thousands": d.get("ASSET") or 0,
                "cert": d.get("CERT", ""),
                "type": "Bank",
            })
        return results
    except Exception:
        return []


# ---------------------------------------------------------------------------
# STAGE 1B: NCUA CREDIT UNION DISCOVERY
# ---------------------------------------------------------------------------
@st.cache_data(ttl=3600, show_spinner=False)
def _search_ncua(state_code, city=None):
    """Query NCUA for credit unions. Endpoint is best-effort."""
    endpoints = [
        "https://mapping.ncua.gov/api/CreditUnionLocator",
        "https://mapping.ncua.gov/api/CreditUnion/Search",
    ]
    params_options = [
        {"StateCode": state_code},
        {"State": state_code},
    ]

    for base_url in endpoints:
        for params in params_options:
            if city:
                params_with_city = {**params, "CityName": city, "City": city}
            else:
                params_with_city = params
            try:
                r = requests.get(
                    base_url, params=params_with_city, timeout=15,
                    headers={"Accept": "application/json"},
                )
                if r.status_code != 200:
                    continue
                data = r.json()
                if isinstance(data, list):
                    items = data
                elif isinstance(data, dict):
                    items = (
                        data.get("data")
                        or data.get("results")
                        or data.get("creditUnions")
                        or data.get("list")
                        or []
                    )
                else:
                    continue

                if not items:
                    continue

                results = []
                for item in items:
                    cu_name = (
                        item.get("CUName")
                        or item.get("Name")
                        or item.get("name")
                        or item.get("CreditUnionName")
                        or ""
                    )
                    cu_city = (
                        item.get("City")
                        or item.get("city")
                        or item.get("PhysicalAddressCity")
                        or ""
                    )
                    cu_website = (
                        item.get("Website")
                        or item.get("URL")
                        or item.get("url")
                        or item.get("WebsiteAddress")
                        or ""
                    )
                    cu_assets = (
                        item.get("TotalAssets")
                        or item.get("Assets")
                        or item.get("assets")
                        or 0
                    )
                    cu_id = (
                        item.get("CUNumber")
                        or item.get("CharterNumber")
                        or item.get("ID")
                        or ""
                    )
                    if city and city.lower() not in cu_city.lower():
                        continue
                    results.append({
                        "name": cu_name.strip(),
                        "city": cu_city.strip(),
                        "state": state_code,
                        "website": cu_website.strip(),
                        "assets_thousands": cu_assets,
                        "cert": cu_id,
                        "type": "Credit Union",
                    })
                return results
            except Exception:
                continue
    return []


# ---------------------------------------------------------------------------
# STAGE 2: FIRECRAWL + GPT IB CLASSIFICATION
# ---------------------------------------------------------------------------
def _firecrawl_scrape(url):
    """Scrape a URL via Firecrawl. Returns markdown text or empty string."""
    _firecrawl_limiter.wait()
    try:
        r = requests.post(
            "https://api.firecrawl.dev/v1/scrape",
            headers={
                "Authorization": f"Bearer {FIRECRAWL_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "url": url,
                "formats": ["markdown"],
                "onlyMainContent": True,
                "timeout": 10000,
            },
            timeout=20,
        )
        if r.status_code == 200:
            data = r.json()
            if data.get("success"):
                return (data.get("data", {}).get("markdown") or "")[:4000]
        return ""
    except Exception:
        return ""


def _scrape_institution(institution):
    """Scrape key pages from an institution's website."""
    domain = _clean_domain(institution.get("website"))
    if not domain:
        return ""
    base = f"https://{domain}"
    combined = []
    for path in _IB_SCAN_PATHS:
        content = _firecrawl_scrape(base + path)
        if content:
            combined.append(f"--- PAGE: {path} ---\n{content}")
        if len(combined) >= 4:
            break
    if not combined:
        content = _firecrawl_scrape(base)
        if content:
            combined.append(f"--- PAGE: / ---\n{content}")
    return "\n\n".join(combined)[:12000]


_IB_CLASSIFY_SYSTEM = """You analyze bank and credit union website content to determine
if the institution has an IN-HOUSE investment banking or M&A advisory capability.

Classify as exactly one of:
- "in_house" — The institution has its own investment banking division, M&A advisory
  team, capital markets practice, or corporate finance advisory services operated by
  the institution's own employees.
- "third_party" — The institution offers investment or brokerage services ONLY through
  a third-party affiliation (LPL Financial, Raymond James, Cetera, Ameriprise, etc.).
  This does NOT count as in-house IB.
- "none" — No evidence of investment banking, M&A, or capital markets services.
- "unknown" — Insufficient information to make a determination.

IMPORTANT DISTINCTIONS:
- Wealth management, trust services, and private banking are NOT investment banking.
- Selling insurance or annuities is NOT investment banking.
- SBA lending, commercial lending, and treasury management are NOT investment banking.
- A broker-dealer affiliation (LPL, Raymond James, etc.) is NOT in-house IB.
- Only classify as "in_house" if there is clear evidence of M&A advisory, investment
  banking, capital markets, or corporate finance advisory by the institution's own team.

Respond with ONLY a JSON object:
{"status": "in_house|third_party|none|unknown", "evidence": "brief explanation"}"""


def _classify_ib(name, scraped_content):
    if not scraped_content.strip():
        return {"status": "unknown", "evidence": "No website content available"}
    try:
        resp = _oai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": _IB_CLASSIFY_SYSTEM},
                {"role": "user", "content": f"Institution: {name}\n\nWebsite content:\n{scraped_content}"},
            ],
            temperature=0,
            max_tokens=150,
        )
        text = resp.choices[0].message.content.strip()
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
            text = text.strip()
        return json.loads(text)
    except Exception:
        return {"status": "unknown", "evidence": "Classification error"}


_IB_ASSET_THRESHOLD = 500_000  # $500M in thousands — below this, auto-qualify

def _check_institution_ib(institution):
    assets = institution.get("assets_thousands") or 0
    if isinstance(assets, (int, float)) and 0 < assets < _IB_ASSET_THRESHOLD:
        return {
            **institution,
            "ib_status": "none",
            "ib_evidence": f"Auto-qualified (${assets / 1000:,.0f}M assets — below IB threshold)",
        }
    content = _scrape_institution(institution)
    result = _classify_ib(institution["name"], content)
    return {
        **institution,
        "ib_status": result.get("status", "unknown"),
        "ib_evidence": result.get("evidence", ""),
    }


# ---------------------------------------------------------------------------
# STAGE 3: APOLLO ENRICHMENT
# ---------------------------------------------------------------------------
def _find_apollo_org(name, domain=None):
    url = "https://api.apollo.io/v1/organizations/search"
    headers = {"Content-Type": "application/json", "X-Api-Key": APOLLO_API_KEY}

    if domain:
        _apollo_limiter.wait()
        try:
            r = requests.post(url, headers=headers, json={
                "q_organization_domains": domain, "per_page": 1,
            }, timeout=10)
            if r.status_code == 200:
                orgs = r.json().get("organizations", [])
                if orgs:
                    return orgs[0]
        except Exception:
            pass

    _apollo_limiter.wait()
    try:
        r = requests.post(url, headers=headers, json={
            "q_organization_name": name, "per_page": 3,
        }, timeout=10)
        if r.status_code == 200:
            orgs = r.json().get("organizations", [])
            if orgs:
                name_lower = name.lower()
                for org in orgs:
                    org_name = (org.get("name") or "").lower()
                    if name_lower in org_name or org_name in name_lower:
                        return org
                return orgs[0]
    except Exception:
        pass
    return None


def _search_people_at_org(org_id, role_titles, city=None):
    url = "https://api.apollo.io/api/v1/mixed_people/api_search"
    headers = {"Content-Type": "application/json", "X-Api-Key": APOLLO_API_KEY}
    payload = {
        "organization_ids": [org_id],
        "person_titles": role_titles,
        "person_seniorities": ["owner", "founder", "c_suite", "partner", "vp", "director"],
        "per_page": 25,
    }
    if city:
        payload["person_locations"] = [city]

    _apollo_limiter.wait()
    try:
        r = requests.post(url, headers=headers, json=payload, timeout=10)
        if r.status_code == 200:
            return r.json().get("people", [])
    except Exception:
        pass
    return []


def _enrich_person(person_id):
    url = "https://api.apollo.io/v1/people/match"
    headers = {"Content-Type": "application/json", "X-Api-Key": APOLLO_API_KEY}
    _apollo_limiter.wait()
    try:
        r = requests.post(url, headers=headers, json={"id": person_id}, timeout=10)
        if r.status_code == 200:
            return r.json().get("person")
    except Exception:
        pass
    return None


def _enrich_institution_contacts(institution, role_titles, search_city=None):
    domain = _clean_domain(institution.get("website"))
    org = _find_apollo_org(institution["name"], domain)
    if not org:
        return []
    org_id = org.get("id")
    if not org_id:
        return []

    people = _search_people_at_org(org_id, role_titles, search_city)
    if not people:
        return []

    contacts = []
    for p in people[:15]:
        pid = p.get("id")
        if not pid:
            continue
        enriched = _enrich_person(pid)
        if not enriched or not enriched.get("first_name"):
            continue
        name = f"{enriched.get('first_name', '')} {enriched.get('last_name', '')}".strip()
        phone_nums = enriched.get("phone_numbers") or []
        phone = phone_nums[0].get("sanitized_number") if phone_nums else "N/A"
        contacts.append({
            "Institution": institution["name"],
            "Inst. Type": institution["type"],
            "Name": name,
            "Title": enriched.get("title") or "N/A",
            "Email": enriched.get("email") or "N/A",
            "Phone": phone or "N/A",
            "LinkedIn": enriched.get("linkedin_url") or "N/A",
            "City": enriched.get("city") or institution.get("city", "N/A"),
            "State": enriched.get("state") or institution.get("state", "N/A"),
        })
    return contacts


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------
st.title("NCP Community Lender Search")
st.caption(
    "Find community and regional banks and credit unions without in-house "
    "investment banking — potential referral partners for proprietary deal flow."
)

col1, col2 = st.columns([3, 2])
location = col1.text_input(
    "Location",
    placeholder="e.g. Birmingham, AL  |  Tennessee  |  Charlotte, NC",
)
inst_types = col2.multiselect(
    "Institution Type",
    ["Banks", "Credit Unions"],
    default=["Banks", "Credit Unions"],
)

selected_roles = st.multiselect(
    "Contact Roles to Search",
    list(ROLE_BUCKETS.keys()),
    default=list(ROLE_BUCKETS.keys()),
)

if st.button("Search", type="primary"):
    if not location.strip():
        st.error("Please enter a location.")
        st.stop()

    city, state_code = _parse_location(location)
    if not state_code:
        st.error(
            f'Could not determine state from "{location}". '
            "Try format: **City, ST** or a full state name."
        )
        st.stop()

    state_name = _STATES.get(state_code, state_code)
    loc_label = f"{city}, {state_code}" if city else state_name

    # ── Stage 1: Institution Discovery ────────────────────────
    institutions = []
    with st.spinner(f"Searching for institutions in **{loc_label}**…"):
        if "Banks" in inst_types:
            banks = _search_fdic(state_code, city)
            institutions.extend(banks)
        if "Credit Unions" in inst_types:
            cus = _search_ncua(state_code, city)
            if not cus:
                st.warning(
                    "Credit union search returned 0 results — "
                    "the NCUA endpoint may need adjustment. Banks unaffected."
                )
            institutions.extend(cus)

    if not institutions:
        st.warning("No institutions found for this location.")
        st.stop()

    bank_ct = sum(1 for i in institutions if i["type"] == "Bank")
    cu_ct = sum(1 for i in institutions if i["type"] == "Credit Union")
    st.info(
        f"**Stage 1 — Discovery:** {len(institutions)} institutions "
        f"({bank_ct} banks, {cu_ct} credit unions). "
        "Now checking for investment banking services…"
    )

    # ── Stage 2: IB Classification ────────────────────────────
    progress = st.progress(0)
    status = st.empty()
    qualified = []
    disqualified = []
    flagged_ct = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as ex:
        futures = {
            ex.submit(_check_institution_ib, inst): inst
            for inst in institutions
        }
        for i, fut in enumerate(concurrent.futures.as_completed(futures)):
            try:
                result = fut.result()
                if result["ib_status"] == "in_house":
                    disqualified.append(result)
                else:
                    if result["ib_status"] == "unknown":
                        flagged_ct += 1
                    qualified.append(result)
            except Exception:
                pass
            progress.progress((i + 1) / len(institutions))
            status.caption(
                f"IB check: {i + 1}/{len(institutions)} | "
                f"{len(qualified)} qualified, {len(disqualified)} disqualified"
            )

    st.success(
        f"**Stage 2 — IB Filter:** {len(qualified)} institutions qualify "
        f"({flagged_ct} flagged as unknown). "
        f"{len(disqualified)} disqualified (in-house IB)."
    )

    if disqualified:
        with st.expander(f"Disqualified — In-House IB ({len(disqualified)})"):
            for d in sorted(disqualified, key=lambda x: x["name"]):
                st.markdown(
                    f"**{d['name']}** ({d['city']}, {d['state']}) — "
                    f"_{d.get('ib_evidence', 'N/A')}_"
                )

    if not qualified:
        st.warning("No qualifying institutions found.")
        st.stop()

    if not selected_roles:
        st.warning("No contact roles selected. Select at least one role bucket above.")
        st.stop()

    # ── Stage 3: Apollo Enrichment ────────────────────────────
    role_titles = []
    for bucket in selected_roles:
        role_titles.extend(ROLE_BUCKETS[bucket])

    status.caption(f"Stage 3: Enriching contacts at {len(qualified)} institutions…")
    progress.progress(0)

    all_contacts = []
    inst_with_contacts = set()

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as ex:
        futures = {
            ex.submit(_enrich_institution_contacts, inst, role_titles, city): inst
            for inst in qualified
        }
        for i, fut in enumerate(concurrent.futures.as_completed(futures)):
            try:
                contacts = fut.result()
                if contacts:
                    inst_with_contacts.add(contacts[0]["Institution"])
                all_contacts.extend(contacts)
            except Exception:
                pass
            progress.progress((i + 1) / len(qualified))
            status.caption(
                f"Apollo: {i + 1}/{len(qualified)} institutions | "
                f"{len(all_contacts)} contacts"
            )

    status.write("Done!")

    # ── Results ───────────────────────────────────────────────
    # Always show institution summary
    with st.expander("Institution Summary", expanded=not all_contacts):
        inst_df = pd.DataFrame([{
            "Name": q["name"],
            "Type": q["type"],
            "City": q["city"],
            "State": q["state"],
            "Website": q.get("website") or "N/A",
            "Assets ($K)": f"{q.get('assets_thousands', 0):,.0f}"
                if q.get("assets_thousands") else "N/A",
            "IB Status": q["ib_status"],
            "IB Evidence": q.get("ib_evidence", ""),
            "Contacts Found": "Yes" if q["name"] in inst_with_contacts else "No",
        } for q in qualified])
        st.dataframe(inst_df, use_container_width=True)

    if not all_contacts:
        st.warning(
            f"{len(qualified)} institutions qualified but no matching contacts "
            "found in Apollo. Try broadening the role selection."
        )
        st.stop()

    st.success(
        f"**{len(all_contacts)}** contacts found at "
        f"**{len(inst_with_contacts)}** institutions."
    )

    df = pd.DataFrame(all_contacts)
    st.dataframe(df, use_container_width=True)

    csv = df.to_csv(index=False).encode("utf-8")
    fname = f"NCP_Community_Lenders_{loc_label.replace(' ', '_').replace(',', '')}.csv"
    st.download_button(
        "Download CSV", data=csv, file_name=fname, mime="text/csv", type="primary",
    )
