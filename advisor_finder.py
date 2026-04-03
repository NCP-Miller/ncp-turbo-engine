import streamlit as st
import pandas as pd
import requests
import concurrent.futures
from urllib.parse import quote_plus

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
        "titles": [
            "wealth advisor", "wealth manager", "private wealth",
            "financial advisor", "private client", "wealth planning",
        ],
        "industries": ["Financial Services", "Investment Management", "Banking"],
        "keywords": ["wealth management", "private wealth", "financial planning"],
    },
    "Tax Advisors (High Net Worth)": {
        "titles": [
            "tax partner", "tax director", "tax managing director",
            "private client tax", "tax principal", "tax counsel",
        ],
        "industries": ["Accounting", "Financial Services"],
        "keywords": ["tax advisory", "high net worth", "private client"],
    },
    "Estate Planning Attorneys": {
        "titles": [
            "estate planning", "trusts and estates", "estate attorney",
            "probate", "wealth transfer", "estate partner",
        ],
        "industries": ["Law Practice", "Legal Services"],
        "keywords": ["estate planning", "trusts and estates", "wealth transfer"],
    },
    "Investment Bankers": {
        "titles": [
            "investment banker", "managing director", "director of investment banking",
            "vice president investment banking", "m&a", "mergers and acquisitions",
        ],
        "industries": ["Investment Banking", "Capital Markets", "Financial Services"],
        "keywords": ["investment banking", "mergers acquisitions", "capital markets"],
    },
    "Business Brokers": {
        "titles": [
            "business broker", "business intermediary", "m&a advisor",
            "deal maker", "business sales", "transaction advisor",
        ],
        "industries": ["Financial Services", "Management Consulting", "Real Estate"],
        "keywords": ["business brokerage", "business sales", "business transfer"],
    },
}

# Seniority levels — mid to senior only
SENIORITY_LEVELS = ["owner", "founder", "c_suite", "partner", "vp", "director"]

# ---------------------------------------------------------------------------
# APOLLO PEOPLE SEARCH
# ---------------------------------------------------------------------------
def search_people(category_key, city, max_pages=5):
    cat = CATEGORIES[category_key]
    url = "https://api.apollo.io/v1/mixed_people/search"
    headers = {"Content-Type": "application/json", "X-Api-Key": APOLLO_API_KEY}

    all_people, seen_ids = [], set()

    # Build search variations for broader coverage
    search_configs = []

    # Config 1: Title keywords + location + seniority
    for title_kw in cat["titles"]:
        search_configs.append({
            "q_keywords": title_kw,
            "person_locations": [city],
            "person_seniority": SENIORITY_LEVELS,
            "per_page": 100,
        })

    # Config 2: Industry + keyword tags + location + seniority
    search_configs.append({
        "person_locations": [city],
        "person_seniority": SENIORITY_LEVELS,
        "q_organization_industries": cat["industries"],
        "q_organization_keyword_tags": cat["keywords"],
        "per_page": 100,
    })

    for config in search_configs:
        for page in range(1, max_pages + 1):
            payload = {**config, "page": page}
            try:
                r = requests.post(url, headers=headers, json=payload, timeout=15)
                if r.status_code != 200:
                    break
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
            except Exception:
                break

    return all_people


def is_senior_enough(title):
    if not title:
        return False
    t = title.lower()
    senior_signals = [
        "partner", "managing director", "principal", "owner", "founder",
        "president", "ceo", "chief", "director", "vp ", "vice president",
        "senior vice", "svp", "evp", "head of", "team lead", "group head",
        "senior managing", "senior advisor", "senior wealth",
        "senior financial", "of counsel",
    ]
    junior_signals = [
        "analyst", "associate", "intern", "assistant", "coordinator",
        "junior", "trainee", "entry", "clerk", "receptionist",
        "administrative", "support", "secretary",
    ]
    if any(j in t for j in junior_signals):
        return False
    if any(s in t for s in senior_signals):
        return True
    # Default: include if seniority filter already applied via API
    return True


def format_results(people, category_key):
    rows = []
    for p in people:
        title = p.get("title") or ""
        if not is_senior_enough(title):
            continue

        name = f"{p.get('first_name', '')} {p.get('last_name', '')}".strip()
        if not name or name == "":
            continue

        email = p.get("email") or "N/A"
        phone_nums = p.get("phone_numbers") or []
        phone = phone_nums[0].get("sanitized_number") if phone_nums else "N/A"
        company = p.get("organization", {}).get("name") if p.get("organization") else "N/A"

        rows.append({
            "Name": name,
            "Title": title,
            "Company": company or "N/A",
            "Email": email,
            "Phone": phone or "N/A",
            "City": p.get("city") or "N/A",
            "State": p.get("state") or "N/A",
            "LinkedIn": p.get("linkedin_url") or "N/A",
        })

    # Deduplicate by name + company
    seen = set()
    unique = []
    for r in rows:
        key = (r["Name"].lower(), r["Company"].lower())
        if key not in seen:
            seen.add(key)
            unique.append(r)

    return unique


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

    with st.spinner(f"Searching Apollo for **{category}** in **{city}**..."):
        people = search_people(category, city)

    if not people:
        st.warning(
            f"No results found for {category} in {city}.\n\n"
            "**Tips:** Try a larger metro area or broader city name (e.g. 'New York' instead of 'Manhattan')."
        )
        st.stop()

    st.info(f"Found **{len(people)}** Apollo results. Filtering for senior contacts...")
    results = format_results(people, category)

    if not results:
        st.warning("No senior-level contacts found after filtering. Try a different city or category.")
        st.stop()

    st.success(f"**{len(results)}** senior contacts found.")

    df = pd.DataFrame(results)
    st.dataframe(df, use_container_width=True)

    csv = df.to_csv(index=False).encode("utf-8")
    fname = f"NCP_{category.replace(' ', '_')}_{city.replace(' ', '_').replace(',', '')}.csv"
    st.download_button(
        "Download CSV", data=csv, file_name=fname, mime="text/csv", type="primary"
    )
