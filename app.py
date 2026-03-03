import streamlit as st
import pandas as pd
import requests
import json
import re
import concurrent.futures
import xml.etree.ElementTree as ET
from urllib.parse import urlparse, urljoin, quote_plus
from openai import OpenAI

st.set_page_config(page_title="NCP Sourcing Engine", page_icon="🚀", layout="wide")

# ---------------------------------------------------------------------------
# AUTH
# ---------------------------------------------------------------------------
def check_password():
    def on_submit():
        st.session_state["auth"] = (st.session_state.get("pw_input") == "NCP2026")
    if not st.session_state.get("auth"):
        st.text_input("Enter Password", type="password", key="pw_input", on_change=on_submit)
        if "auth" in st.session_state and not st.session_state["auth"]:
            st.error("😕 Incorrect password")
        st.stop()

check_password()

# ---------------------------------------------------------------------------
# SECRETS
# ---------------------------------------------------------------------------
try:
    APOLLO_API_KEY    = st.secrets["APOLLO_API_KEY"]
    OPENAI_API_KEY    = st.secrets["OPENAI_API_KEY"]
    FIRECRAWL_API_KEY = st.secrets["FIRECRAWL_API_KEY"]
except (FileNotFoundError, KeyError):
    st.error("❌ API Keys missing. Set them in `.streamlit/secrets.toml`.")
    st.stop()

client = OpenAI(api_key=OPENAI_API_KEY)

# ---------------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------------
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

# Full executive title phrases (Apollo stores full titles, not acronyms)
_TIER1_TITLES = [
    "owner","founder","principal","managing partner","managing member","managing director",
    "chief executive","chief operating","chief financial","chief medical","chief nursing",
    "chief clinical","chief strategy","chief people","chief growth",
    " ceo"," coo"," cfo"," cmo"," cno",
    "president","partner","architect","administrator",
    "executive director","medical director","director of",
]
_TIER2_TITLES = ["vice president"," vp ","manager","associate","coordinator","operations"]

# ---------------------------------------------------------------------------
# AI — NICHE → APOLLO PARAMETERS
# ---------------------------------------------------------------------------
def suggest_search_params(niche_description: str) -> dict:
    """Map a plain-English niche to Apollo industry categories + search keywords."""
    prompt = f"""You are configuring a company search on Apollo.io for this target niche:
"{niche_description}"

Apollo.io organizes companies into these industry category labels:
{json.dumps(APOLLO_INDUSTRIES)}

Task 1 — Industries:
Pick the 3-5 Apollo categories MOST LIKELY to contain companies in this niche.
Think carefully — niche operators often appear in unexpected categories:
- PACE (Program of All-Inclusive Care for the Elderly) → "Individual & Family Services",
  "Non-Profit Organization Management", "Hospital & Health Care"
- Commercial HVAC contractors → "Construction", "Facilities Services",
  "Mechanical or Industrial Engineering"
- Veterinary practices → "Veterinary", "Consumer Services"
- Specialty dental groups → "Medical Practice", "Hospital & Health Care",
  "Health, Wellness and Fitness"
Never rely on just one category for niche operators.

Task 2 — Keywords:
Generate 4-6 short keyword phrases that would appear in an Apollo company profile
for this niche. Be specific, not generic.

Return JSON only:
{{"industries": ["Category A", "Category B", "Category C"],
  "keywords": "term1, term2, term3, term4"}}"""
    try:
        resp = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            timeout=20,
        )
        data  = json.loads(resp.choices[0].message.content)
        valid = [i for i in (data.get("industries") or []) if i in APOLLO_INDUSTRIES]
        return {
            "industries": valid or ["Hospital & Health Care"],
            "keywords":   data.get("keywords", ""),
        }
    except:
        return {"industries": ["Hospital & Health Care"], "keywords": ""}

# ---------------------------------------------------------------------------
# APOLLO — ORGANIZATIONS
# ---------------------------------------------------------------------------
def search_organizations(industries, location_input, keyword_tags=None, max_pages=2):
    url     = "https://api.apollo.io/v1/organizations/search"
    headers = {"Content-Type": "application/json", "X-Api-Key": APOLLO_API_KEY}
    all_orgs, seen_ids = [], set()

    for page in range(1, max_pages + 1):
        payload = {"organization_locations": [location_input], "page": page, "per_page": 100}
        if industries:    payload["q_organization_industries"] = industries
        if keyword_tags:  payload["q_organization_keyword_tags"] = keyword_tags
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=15)
            if r.status_code != 200: break
            orgs = r.json().get("organizations", [])
            if not orgs: break
            for o in orgs:
                oid = o.get("id")
                if oid and oid not in seen_ids:
                    seen_ids.add(oid); all_orgs.append(o)
            if len(orgs) < 100: break
        except: break

    return all_orgs

# ---------------------------------------------------------------------------
# FILTER 1 — STRUCTURE
# ---------------------------------------------------------------------------
def is_buyable_structure(org, mode):
    """
    Mode A: private, non-PE, <=7,500 employees.
            Uses exact status match so 'non_profit_public' is NOT caught.
    Mode B: only blocks genuine mega-corps (>100k employees).
    """
    emp    = org.get("estimated_num_employees", 0) or 0
    status = str(org.get("ownership_status") or "").strip().lower()
    tags   = [t.lower() for t in (org.get("keywords") or [])]

    if mode == "A":
        if status == "public":     return False, "Publicly Traded"
        if status == "subsidiary": return False, "Subsidiary"
        if "private equity" in tags or "venture capital" in tags:
            return False, "PE/VC Backed"
        if emp > 7500: return False, f"Too Large ({emp})"
    else:
        if emp > 100000: return False, f"Mega-Corp ({emp})"

    return True, "OK"

# ---------------------------------------------------------------------------
# FILTER 2 — OBVIOUS NAME MISMATCHES  (name-only; no tag filtering)
# ---------------------------------------------------------------------------
# Tag-based filtering intentionally omitted: Apollo frequently mis-tags
# legitimate operators with 'technology', 'software', 'staffing', etc.
_UNIVERSAL_BLOCKS = [
    "university", "college", "food service", "catering",
    "staffing solutions", "temp agency",
]
_MODE_A_BLOCKS = [
    "consulting group", "advisory group", " billing services",
    "software inc", "software llc",
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

# ---------------------------------------------------------------------------
# FILTER 3 — AI GATEKEEPER
# ---------------------------------------------------------------------------
def check_relevance_gpt4o(company_name, description, keywords, target_niche, mode):
    """
    Mode A — strict acquisition filter.
      Uses target_niche as the anchor; no niche-specific examples hardcoded.
      'When uncertain → FAIL' keeps the list tight.

    Mode B — sector-match filter.
      Must be in the same care/service sector and customer population.
      Rejects companies in adjacent-but-different sectors (pharma for elder-care,
      dental for behavioral health, etc.).
    """
    if mode == "A":
        prompt = f"""You are a strict acquisition filter for a private equity investor.
Target niche: "{target_niche}"

Company: "{company_name}"
Description: "{description}"
Keywords: {keywords}

PASS only if the company clearly fits ONE of these:
1. Direct operator in the exact niche described above.
2. Operator in a directly adjacent sub-sector that serves THE SAME primary
   customer or patient population as the niche (not just the same broad industry).
3. Company name alone strongly implies a matching operator AND the description
   is empty or too sparse to judge.

FAIL if any of these apply:
- Serves a DIFFERENT primary population than the target niche
  (wrong age group, wrong customer type, wrong service category)
- Large enterprise, national chain, or institution not suitable for acquisition
- Software, analytics, EMR, or technology vendor (no direct service delivery)
- Consulting, billing, staffing, CRO, or outsourcing firm
- Insurance carrier or payer with no direct service/care delivery
- Pharma, biotech, or laboratory company
- Completely unrelated industry

IMPORTANT: When uncertain → FAIL. This is a strict buy-side filter.
Return JSON only: {{"match": true/false, "reason": "one sentence"}}"""

    else:
        prompt = f"""You are identifying potential buyers or strategic partners for
companies in this niche: "{target_niche}"

Company: "{company_name}"
Description: "{description}"
Keywords: {keywords}

PASS if the company operates in the same sector or serves the same customer/patient
population and could plausibly acquire, invest in, or partner with a company in the niche:
- Direct competitors or adjacent operators in the same sector
- Health systems, insurers, or managed-care orgs serving the same population
- Operators in closely adjacent sub-sectors (same customer type)

FAIL if:
- Pharma, biotech, drug manufacturing, or CRO company
- Clinical laboratory or diagnostics company
- Dental, aesthetics, ophthalmology, or specialty unrelated to the niche
- Health IT, analytics, or software with no direct care delivery
- Medical device manufacturer with no patient/customer care operations
- Companies clearly in construction, finance, retail, food, or other
  unrelated sectors

Base the decision on the SPECIFIC niche, not on broad industry membership.
Return JSON only: {{"match": true/false, "reason": "one sentence"}}"""

    try:
        resp = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            timeout=20,
        )
        data = json.loads(resp.choices[0].message.content)
        return data.get("match"), data.get("reason")
    except:
        return True, "AI Error"

# ---------------------------------------------------------------------------
# WEB SPIDER
# ---------------------------------------------------------------------------
def firecrawl_scrape(url):
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
    except: pass
    return None

def extract_relevant_links(md, base_url):
    if not md: return []
    high = ["leadership","executive","our team","care team","management",
            "principals","partners","architects","providers","medical staff"]
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
        seen.add(full); out.append((score, full))
    out.sort(key=lambda x: x[0], reverse=True)
    return [u for _, u in out[:4]]

def extract_names_openai(text, company_name):
    prompt = f"""From the website text of "{company_name}", extract:
1. The PRIMARY LEADER: CEO, Owner, Founder, President, Principal,
   Administrator, Executive Director, or Medical Director.
2. Any contact email visible on the page.
3. Any contact phone number visible on the page.

Return JSON only (use the string "None" when not found):
{{"name": "Full Name or First Only or None",
  "title": "Their Title or None",
  "email": "email@example.com or None",
  "phone": "555-1234 or None"}}

Text:
{text[:15000]}"""
    try:
        resp = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            timeout=20,
        )
        return json.loads(resp.choices[0].message.content)
    except: return None

# ---------------------------------------------------------------------------
# APOLLO — PEOPLE  (org_id first — most reliable lookup key)
# ---------------------------------------------------------------------------
def clean_domain(url):
    if not url or not isinstance(url, str): return None
    try:
        if not url.startswith("http"): url = "http://" + url
        d = urlparse(url).netloc
        return d[4:] if d.startswith("www.") else d
    except: return None

def clean_company_name_for_search(name):
    if not name: return ""
    c = name.replace(",", "").replace(".", "")
    for s in [" inc"," llc"," group"," ltd"," corp"," p.c."," pc",
              " architects"," architecture"]:
        if c.lower().endswith(s): c = c[:-len(s)]
    return c.strip()

def get_people_apollo_robust(company_name, domain, org_id=None):
    url     = "https://api.apollo.io/v1/mixed_people/search"
    headers = {"Content-Type": "application/json", "X-Api-Key": APOLLO_API_KEY}
    name    = clean_company_name_for_search(company_name)
    senior  = ["owner", "founder", "c_suite", "president"]

    attempts = []
    # Apollo org ID is the most reliable key — use it first
    if org_id:
        attempts += [
            {"organization_ids": [org_id], "person_seniority": senior, "per_page": 10},
            {"organization_ids": [org_id], "per_page": 25},
        ]
    if domain:
        attempts += [
            {"q_organization_domains": [domain], "person_seniority": senior, "per_page": 10},
            {"q_organization_domains": [domain], "per_page": 25},
        ]
    attempts += [
        {"q_organization_names": [name], "person_seniority": senior, "per_page": 10},
        {"q_organization_names": [name], "per_page": 15},
    ]

    for payload in attempts:
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=10)
            if r.status_code == 200:
                people = r.json().get("people", [])
                if people: return people
        except: pass
    return []

def select_best_apollo_contact(people):
    if not people: return None, "None"
    valid = [p for p in people
             if p.get("first_name") and p.get("last_name")
             and str(p.get("last_name", "")).strip().lower() not in ("none", "n/a", "")]
    if not valid: return None, "None"

    top = backup = None
    for p in valid:
        if any(x in (p.get("title") or "").lower() for x in _TIER1_TITLES):
            top = p; break

    if top and not top.get("email"):
        for p in valid:
            if any(x in (p.get("title") or "").lower() for x in _TIER2_TITLES) \
               and p.get("email"):
                backup = p; break

    if top:
        src = "Apollo (Top)"
        if not top.get("email"):
            src += " [No Email]"
            if backup:
                top = dict(top)
                top["notes"] = f"Alt: {backup.get('first_name')} — {backup.get('email')}"
        return top, src

    with_email = [p for p in valid if p.get("email")]
    return (with_email[0] if with_email else valid[0]), "Apollo (Best Available)"

def repair_single_name(first_name, people_list):
    if not first_name or not people_list: return None
    target = first_name.split()[0].lower()
    for p in people_list:
        if target in (p.get("first_name") or "").lower(): return p
    return None

def bulk_enrich_names(people_list, domain):
    if not people_list or not domain: return []
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
    except: return []

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
    except: return None, None

# ---------------------------------------------------------------------------
# WORKER
# ---------------------------------------------------------------------------
def process_single_company(org, specific_niche, strat_code):
    comp_name = org.get("name")

    if not is_buyable_structure(org, strat_code)[0]:          return None
    if is_obvious_mismatch(org, specific_niche, strat_code)[0]: return None

    desc = org.get("short_description") or org.get("headline") or ""
    tags = org.get("keywords") or []
    if not check_relevance_gpt4o(comp_name, desc, tags, specific_niche, strat_code)[0]:
        return None

    domain = clean_domain(org.get("website_url"))
    org_id = org.get("id")   # Apollo internal ID — most reliable for people lookup

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

    found_person = apollo_cache = web_email = web_phone = None

    # ── Web spider ──────────────────────────────────────────────────────────
    if domain:
        queue, visited = [f"https://{domain}"], set()
        for url in queue[:6]:
            if url in visited: continue
            visited.add(url)
            content = firecrawl_scrape(url)
            if not content: continue
            ai = extract_names_openai(content, comp_name)
            if ai:
                e, p = ai.get("email"), ai.get("phone")
                if e and e != "None": web_email = e
                if p and p != "None": web_phone = p
                n = ai.get("name", "None")
                if n and n != "None":
                    if " " in n and len(n) > 3:
                        found_person = {
                            "first_name":    n.split()[0],
                            "last_name":     " ".join(n.split()[1:]),
                            "title":         ai.get("title"),
                            "email":         web_email,
                            "phone_numbers": [{"sanitized_number": web_phone}] if web_phone else [],
                        }
                        row["Source"] = "Web Spider"; break
                    elif len(n) > 1:
                        if not apollo_cache:
                            apollo_cache = get_people_apollo_robust(comp_name, domain, org_id)
                        rep = repair_single_name(n, apollo_cache)
                        if rep: found_person = rep; row["Source"] = "Web → Repaired"; break
            for lnk in extract_relevant_links(content, url):
                if lnk not in visited: queue.insert(1, lnk)

    # ── Apollo fallback ─────────────────────────────────────────────────────
    if not found_person:
        if not apollo_cache:
            apollo_cache = get_people_apollo_robust(comp_name, domain, org_id)
        best, method = select_best_apollo_contact(apollo_cache)
        if best: found_person = best; row["Source"] = method

    # ── Populate contact fields ─────────────────────────────────────────────
    if found_person:
        row["CEO/Owner Name"] = (
            f"{found_person.get('first_name','')} "
            f"{found_person.get('last_name','')}").strip()
        row["Title"] = found_person.get("title") or "N/A"

        if "Web" in row["Source"] and domain:
            matches = bulk_enrich_names([found_person], domain)
            if matches and matches[0]:
                found_person        = matches[0]
                row["Source"]      += " → Verified"
                row["Confidence"]   = "High"
        elif "Apollo" in row["Source"]:
            row["Confidence"] = "Medium"

        a_email = found_person.get("email")
        row["Email"] = a_email if a_email else (web_email or "N/A")
        pnums   = found_person.get("phone_numbers") or []
        a_phone = pnums[0].get("sanitized_number") if pnums else None
        row["Phone"] = a_phone if a_phone else (web_phone or "N/A")
        if found_person.get("notes"): row["Notes"] = found_person["notes"]
    else:
        if web_email: row["Email"] = web_email
        if web_phone: row["Phone"] = web_phone

    t, u = get_latest_news_link(comp_name, org.get("city"))
    if u: row["Latest News"] = f"{t} | {u}" if t else u

    return row

# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------
st.title("🚀 NCP Sourcing Engine")
st.caption("Describe your target → get AI-suggested search fields → source.")

# ── STEP 1: NICHE DISCOVERY ──────────────────────────────────────────────────
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
        with st.spinner(f"Mapping '{niche_raw}' to Apollo parameters…"):
            s = suggest_search_params(niche_raw)
            st.session_state["s_industries"] = s["industries"]
            st.session_state["s_keywords"]   = s["keywords"]
            st.session_state["s_niche"]      = niche_raw
        st.rerun()

if "s_industries" in st.session_state:
    st.success(
        f"✅  Industries: **{', '.join(st.session_state['s_industries'])}**   |   "
        f"Keywords: **{st.session_state['s_keywords']}**   "
        "— review below and click **Start Sourcing**."
    )

st.divider()

# ── STEP 2: CONFIGURE + RUN ──────────────────────────────────────────────────
st.markdown("### Step 2 — Review, adjust, and run")

# Initialise session_state defaults for first load
for k, v in [("s_industries", ["Hospital & Health Care"]),
              ("s_keywords",   ""),
              ("s_niche",      "")]:
    if k not in st.session_state:
        st.session_state[k] = v

r1a, r1b = st.columns(2)
industries = r1a.multiselect(
    "Apollo Industry Categories",
    options=APOLLO_INDUSTRIES,
    key="s_industries",   # reads & writes st.session_state["s_industries"]
)
specific_niche = r1b.text_input(
    "Specific Niche (AI Filter)",
    key="s_niche",
)

r2a, r2b, r2c = st.columns(3)
target_geo = r2a.text_input("Geography", value="North Carolina, United States")
mode = r2b.selectbox("Strategy", [
    "A - Buy/Private  (Strict: small private operators only)",
    "B - Sell/Scout   (Broad: same sector, all sizes)",
])
apollo_keywords_raw = r2c.text_input(
    "Apollo Keywords (optional — add or adjust)",
    key="s_keywords",
)

if st.button("🚀 Start Sourcing", type="primary"):
    if not industries:
        st.error("Please select at least one industry, or click **Suggest Fields** first.")
        st.stop()

    strat_code   = "A" if "A -" in mode else "B"
    keyword_tags = [k.strip() for k in apollo_keywords_raw.split(",") if k.strip()] or None

    st.info(f"🔎 Searching **{', '.join(industries)}** in **{target_geo}**…")
    orgs = search_organizations(industries, target_geo, keyword_tags=keyword_tags)

    if not orgs:
        st.error(
            "Apollo returned no companies. Try broadening the industry selection, "
            "removing keyword filters, or checking the geography spelling."
        )
        st.stop()

    st.success(f"Found **{len(orgs)}** candidates — filtering with 5 parallel workers…")
    progress_bar = st.progress(0)
    status_text  = st.empty()
    final_data   = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
        futures = {
            ex.submit(process_single_company, org, specific_niche, strat_code): org
            for org in orgs
        }
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            result = future.result()
            if result: final_data.append(result)
            progress_bar.progress((i + 1) / len(orgs))
            status_text.caption(
                f"Processed {i+1}/{len(orgs)} | {len(final_data)} passed so far…"
            )

    status_text.write("✅ Sourcing complete!")

    if final_data:
        df  = pd.DataFrame(final_data)
        st.dataframe(df)
        csv = df.to_csv(index=False).encode("utf-8")
        fname = (
            f"NCP_{'_'.join(industries[:2])}_{target_geo}.csv"
            .replace(" ", "_").replace(",", "")
        )
        st.download_button("Download CSV", data=csv, file_name=fname,
                           mime="text/csv", type="primary")
    else:
        st.warning(
            "No targets passed the filters.\n\n"
            "**Tips:**\n"
            "- Click **Suggest Fields** and use the AI-recommended industries + keywords\n"
            "- Switch to **Mode B** for a broader sweep\n"
            "- Add more industry categories (niche operators often appear under unexpected ones)\n"
            "- Broaden the Specific Niche description in Step 2"
        )
