import streamlit as st
import pandas as pd
import requests
import concurrent.futures

st.set_page_config(page_title="NCP Intermediary Sourcing Tool", layout="wide")

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
            "wealth", "financial advisor", "financial planner", "portfolio",
            "investment advisor", "private client", "asset management",
        ],
        "person_titles": [
            "wealth advisor", "wealth manager", "financial advisor",
            "financial planner", "private wealth", "investment advisor",
            "portfolio manager", "private client",
        ],
    },
    "Tax Advisors (High Net Worth)": {
        "industries": ["Accounting", "Financial Services"],
        "keywords": [
            "tax advisory", "tax services", "public accounting",
            "tax planning", "cpa", "certified public accountant",
        ],
        "title_filter": [
            "tax", "cpa", "accountant", "accounting",
        ],
        "person_titles": [
            "tax partner", "tax director", "tax advisor",
            "tax manager", "cpa", "tax principal",
        ],
    },
    "Estate Planning Attorneys": {
        "industries": ["Law Practice", "Legal Services"],
        "keywords": [
            "estate planning", "trusts and estates", "probate",
            "elder law", "trust administration", "wealth transfer",
        ],
        "title_filter": [
            "estate", "trust", "probate", "elder law", "wealth transfer",
            "attorney", "counsel", "partner", "lawyer",
        ],
        "person_titles": [
            "estate planning", "trusts and estates", "probate",
            "elder law", "wealth transfer", "estate attorney",
        ],
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
            "corporate finance", "deal", "managing director",
        ],
        "person_titles": [
            "investment banker", "managing director", "m&a",
            "corporate finance", "capital markets", "mergers",
        ],
    },
    "Business Brokers": {
        "industries": [
            "Financial Services", "Management Consulting", "Real Estate",
            "Accounting",
        ],
        "keywords": [
            "business brokerage", "business broker", "business sales",
            "business valuation", "business transfer", "business intermediary",
        ],
        "title_filter": [
            "broker", "intermediary", "business sales", "valuation",
            "transaction", "deal", "m&a",
        ],
        "person_titles": [
            "business broker", "business intermediary", "business valuation",
            "transaction advisor", "deal advisor",
        ],
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
                import time; time.sleep(1); continue
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
            r = requests.post(url, headers=headers, json={"id": person_id}, timeout=10)
            if r.status_code == 200:
                return r.json().get("person")
            elif r.status_code == 429:
                import time; time.sleep(1); continue
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
def get_senior_people(org_id, org_name, domain=None):
    search_url = "https://api.apollo.io/api/v1/mixed_people/api_search"
    enrich_url = "https://api.apollo.io/v1/people/match"
    headers = {"Content-Type": "application/json", "X-Api-Key": APOLLO_API_KEY}

    # Step 1: Search to get person IDs (returns obfuscated data)
    payload = {
        "organization_ids": [org_id],
        "person_seniorities": SENIORITY_LEVELS,
        "per_page": 10,
    }
    people_ids = []
    for attempt in range(3):
        try:
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
                import time; time.sleep(1); continue
            else:
                return [{"_debug": True, "status": r.status_code, "body": r.text[:200]}]
        except Exception as e:
            if attempt < 2:
                import time; time.sleep(1); continue
            return [{"_debug": True, "error": str(e)}]

    if not people_ids:
        return []

    # Step 2: Enrich each person by ID to get full name, email, phone
    enriched = []
    for p in people_ids[:10]:
        for attempt in range(2):
            try:
                r = requests.post(enrich_url, headers=headers, json={"id": p["id"]}, timeout=10)
                if r.status_code == 200:
                    person = r.json().get("person")
                    if person and person.get("first_name") and person.get("last_name"):
                        if not person.get("organization"):
                            person["organization"] = {"name": org_name}
                        enriched.append(person)
                    break
                elif r.status_code == 429:
                    import time; time.sleep(1); continue
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
    return any(f in t for f in filters)


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

    people = get_senior_people(org_id, org_name, domain)

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

    st.info(f"Pass 1: Found **{len(direct_people)}** people by title. Enriching contacts...")
    progress_bar = st.progress(0)
    status_text = st.empty()

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
        futures = {
            ex.submit(enrich_person, p["id"], headers): p
            for p in direct_people
        }
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            try:
                person = future.result()
                if person and person.get("first_name") and person.get("last_name"):
                    pid = person.get("id")
                    if pid:
                        seen_person_ids.add(pid)

                    title = person.get("title") or ""
                    if not is_senior_enough(title):
                        continue
                    if not _person_matches_location(person, city):
                        continue

                    name = f"{person.get('first_name', '')} {person.get('last_name', '')}".strip()
                    email = person.get("email") or "N/A"
                    phone_nums = person.get("phone_numbers") or []
                    phone = phone_nums[0].get("sanitized_number") if phone_nums else "N/A"
                    company = (person.get("organization") or {}).get("name") or "N/A"

                    all_rows.append({
                        "Name": name,
                        "Title": title,
                        "Company": company,
                        "Email": email,
                        "Phone": phone or "N/A",
                        "City": person.get("city") or "N/A",
                        "State": person.get("state") or "N/A",
                        "LinkedIn": person.get("linkedin_url") or "N/A",
                    })
            except Exception:
                pass
            progress_bar.progress((i + 1) / len(direct_people))
            status_text.caption(
                f"Enriched {i + 1}/{len(direct_people)} | {len(all_rows)} contacts so far..."
            )

    pass1_count = len(all_rows)

    # ── Pass 2: Org-based search (supplementary — catches different firms) ─
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

    status_text.write("Done!")

    # Debug info
    with st.expander("Debug Info"):
        st.write(f"Pass 1 (people-direct): {pass1_count} contacts")
        st.write(f"Pass 2 (org-based): {len(all_rows) - pass1_count} additional contacts")
        st.write(f"Total before dedup: {len(all_rows)}")
        st.write(f"Unique contacts: {len(unique)}")

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
