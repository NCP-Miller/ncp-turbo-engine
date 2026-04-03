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
    },
}

SENIORITY_LEVELS = ["owner", "founder", "c_suite", "partner", "vp", "director"]

# ---------------------------------------------------------------------------
# STEP 1: FIND ORGANIZATIONS (proven approach from sourcing engine)
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

    # Pass 1: Industry sweep by location
    for industry in cat["industries"]:
        if len(all_orgs) >= MAX_ORGS:
            break
        _fetch_pages({
            "organization_locations": [city],
            "q_organization_industries": [industry],
        })

    # Pass 2: Keyword sweep (catches firms in unexpected industries)
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
    url = "https://api.apollo.io/v1/mixed_people/search"
    headers = {"Content-Type": "application/json", "X-Api-Key": APOLLO_API_KEY}

    # Single fast query — org_id + seniority filter
    payload = {
        "organization_ids": [org_id],
        "person_seniority": SENIORITY_LEVELS,
        "per_page": 10,
    }
    try:
        r = requests.post(url, headers=headers, json=payload, timeout=8)
        if r.status_code == 200:
            people = r.json().get("people", [])
            for p in people:
                if not p.get("organization"):
                    p["organization"] = {"name": org_name}
            return [p for p in people if p.get("first_name") and p.get("last_name")]
    except Exception:
        pass

    return []


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


def process_org(org, category_key):
    org_id = org.get("id")
    org_name = org.get("name", "Unknown")
    domain = clean_domain(org.get("website_url"))

    people = get_senior_people(org_id, org_name, domain)
    rows = []

    for p in people:
        title = p.get("title") or ""
        if not is_senior_enough(title):
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

    return rows


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

    with st.spinner(f"Finding **{category.lower()}** firms in **{city}**..."):
        orgs = search_organizations(category, city)

    if not orgs:
        st.warning(
            f"No firms found for {category} in {city}.\n\n"
            "**Tips:** Try a larger metro area or broader city name."
        )
        st.stop()

    st.info(f"Found **{len(orgs)}** firms. Now finding senior contacts at each...")
    progress_bar = st.progress(0)
    status_text = st.empty()

    all_rows = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as ex:
        futures = {
            ex.submit(process_org, org, category): org for org in orgs
        }
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            try:
                rows = future.result()
                if rows:
                    all_rows.extend(rows)
            except Exception:
                pass
            progress_bar.progress((i + 1) / len(orgs))
            status_text.caption(
                f"Processed {i + 1}/{len(orgs)} firms | {len(all_rows)} contacts found..."
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

    if not unique:
        st.warning(
            "No senior contacts found.\n\n"
            "**Tips:** Try a larger city or different category."
        )
        st.stop()

    st.success(f"**{len(unique)}** senior contacts found across **{len(orgs)}** firms.")

    df = pd.DataFrame(unique)
    st.dataframe(df, use_container_width=True)

    csv = df.to_csv(index=False).encode("utf-8")
    fname = f"NCP_{category.replace(' ', '_')}_{city.replace(' ', '_').replace(',', '')}.csv"
    st.download_button(
        "Download CSV", data=csv, file_name=fname, mime="text/csv", type="primary"
    )
