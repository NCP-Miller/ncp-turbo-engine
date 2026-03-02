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

def check_password():
    def password_entered():
        if st.session_state["password"] == "NCP2026":
            st.session_state["password_correct"] = True
            del st.session_state["password"]
        else:
            st.session_state["password_correct"] = False
    if "password_correct" not in st.session_state:
        st.text_input("Enter Password", type="password", on_change=password_entered, key="password")
        return False
    elif not st.session_state["password_correct"]:
        st.text_input("Enter Password", type="password", on_change=password_entered, key="password")
        st.error("😕 Password incorrect")
        return False
    return True

if not check_password():
    st.stop()

try:
    APOLLO_API_KEY = st.secrets["APOLLO_API_KEY"]
    OPENAI_API_KEY = st.secrets["OPENAI_API_KEY"]
    FIRECRAWL_API_KEY = st.secrets["FIRECRAWL_API_KEY"]
except FileNotFoundError:
    st.error("❌ API Keys missing. Please set them in `.streamlit/secrets.toml`.")
    st.stop()

client = OpenAI(api_key=OPENAI_API_KEY)

# ---------------------------------------------------------------------------
# UTILITY
# ---------------------------------------------------------------------------

def clean_domain(url):
    if not url or not isinstance(url, str): return None
    try:
        if not url.startswith('http'): url = 'http://' + url
        domain = urlparse(url).netloc
        return domain[4:] if domain.startswith('www.') else domain
    except:
        return None

def clean_company_name_for_search(name):
    if not name: return ""
    clean = name.replace(',', '').replace('.', '')
    for suffix in [' inc', ' llc', ' group', ' ltd', ' corp', ' p.c.', ' pc',
                   ' architects', ' architecture']:
        if clean.lower().endswith(suffix):
            clean = clean[:-len(suffix)]
    return clean.strip()

# ---------------------------------------------------------------------------
# APOLLO — ORGANIZATIONS
# ---------------------------------------------------------------------------

def search_organizations(industries, location_input, keyword_tags=None, max_pages=2):
    """
    industries: list of Apollo industry label strings.
    Searches all selected industries in a single API call per page.
    """
    url = "https://api.apollo.io/v1/organizations/search"
    headers = {'Content-Type': 'application/json', 'X-Api-Key': APOLLO_API_KEY}
    all_orgs = []
    seen_ids = set()

    for page in range(1, max_pages + 1):
        payload = {"organization_locations": [location_input], "page": page, "per_page": 100}
        if industries:
            payload["q_organization_industries"] = industries
        if keyword_tags:
            payload["q_organization_keyword_tags"] = keyword_tags
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=15)
            if r.status_code != 200: break
            orgs = r.json().get('organizations', [])
            if not orgs: break
            for org in orgs:
                oid = org.get('id')
                if oid and oid not in seen_ids:
                    seen_ids.add(oid)
                    all_orgs.append(org)
            if len(orgs) < 100: break
        except:
            break
    return all_orgs

# ---------------------------------------------------------------------------
# FILTER LAYER 1 — STRUCTURE
# ---------------------------------------------------------------------------

def is_buyable_structure(org, mode):
    """
    Mode A: strictly private/small — block public, subsidiary, PE-backed, >7500 emp.
    Mode B: only block genuine mega-corps (>100k). Health systems, insurers, etc.
             are valid sell-side prospects and are NOT blocked.
    Note: use exact equality on ownership_status so that values like
    'non_profit_public' or 'government_public' don't get accidentally blocked.
    """
    emp = org.get('estimated_num_employees', 0) or 0
    status = str(org.get('ownership_status') or '').strip().lower()
    tags = [t.lower() for t in (org.get('keywords') or [])]

    if mode == 'A':
        if status == 'public':     return False, "Publicly Traded"
        if status == 'subsidiary': return False, "Subsidiary"
        if 'private equity' in tags or 'venture capital' in tags:
            return False, "PE/VC Backed"
        if emp > 7500: return False, f"Too Large ({emp} emp)"
    else:
        if emp > 100000: return False, f"Mega-Corp ({emp} emp)"

    return True, "OK"

# ---------------------------------------------------------------------------
# FILTER LAYER 2 — OBVIOUS NAME MISMATCHES (name-only; no tag filtering)
# ---------------------------------------------------------------------------
# Tag-based filtering is intentionally omitted: Apollo frequently mis-tags
# legitimate care operators with 'technology', 'software', 'staffing', etc.

_UNIVERSAL_NAME_BLOCKS = [
    'university', 'college', 'food service', 'catering',
    'staffing solutions', 'temp agency',
]
_MODE_A_NAME_BLOCKS = [
    'consulting group', 'advisory group', ' billing services',
    'software inc', 'software llc',
]

def is_obvious_mismatch(org, target_niche, mode):
    name = (org.get('name') or '').lower()
    for frag in _UNIVERSAL_NAME_BLOCKS:
        if frag in name: return True, f"Universal block: '{frag}'"
    if mode == 'B':
        return False, "Pass"
    extras = list(_MODE_A_NAME_BLOCKS)
    if 'architect' in target_niche.lower():
        extras += ['realty', 'real estate', 'lumber', 'golf course']
    for frag in extras:
        if frag in name: return True, f"Mode A block: '{frag}'"
    return False, "Pass"

# ---------------------------------------------------------------------------
# FILTER LAYER 3 — AI GATEKEEPER
# ---------------------------------------------------------------------------

def check_relevance_gpt4o(company_name, description, keywords, target_niche, mode):
    """
    Mode A: must be a direct or closely adjacent OPERATOR in the niche.
    Mode B: must be in the same CARE SECTOR / PATIENT POPULATION — not just
            any healthcare company. This rejects pharma CROs, clinical labs,
            dental companies, analytics firms, etc. for elder-care niches.
    """
    if mode == 'A':
        prompt = f"""You help a private equity investor find companies to ACQUIRE in this niche:
"{target_niche}"

Company: "{company_name}"
Description: "{description}"
Keywords: {keywords}

PASS (match=true) if the company is a DIRECT OPERATOR or CLOSELY ADJACENT OPERATOR:
- Directly delivers the core service/product in the niche, OR
- Operates in an adjacent sub-sector serving the same population
  (e.g. for an elder-care niche: adult day health, home health for elderly/disabled,
  assisted living, memory care, senior living, managed care for dual-eligible populations)
- Company name strongly suggests a care operator, even if description is sparse.

FAIL (match=false) only if clearly one of:
- Large regional/national health system or hospital network
- Pure software, EMR, analytics, or technology vendor (no direct care)
- Consulting, billing, CRO (contract research org), or outsourcing firm
- Insurance carrier with NO direct care delivery
- Pharma, biotech, or clinical laboratory company
- Completely unrelated industry

If uncertain about a small/mid-size care company in the same sector → match=true.
Reply ONLY with JSON: {{"match": true/false, "reason": "one sentence"}}"""

    else:
        prompt = f"""You are identifying companies that operate in the same CARE SECTOR or serve
the same PATIENT / CUSTOMER POPULATION as companies in this niche:
"{target_niche}"

Company: "{company_name}"
Description: "{description}"
Keywords: {keywords}

PASS (match=true) if the company:
- Operates in the same or closely adjacent care sector (same patient population, similar services)
- Is a health system, senior care, or home health operator that serves similar patients
- Is an insurer or managed care org covering the same patient population
- Is a direct competitor or adjacent operator in the niche

FAIL (match=false) if the company:
- Is in pharmaceutical development, clinical research, or drug manufacturing (pharma/CRO)
- Is in clinical laboratory or diagnostics services
- Is in dental, aesthetics, ophthalmology, or an unrelated medical specialty
- Is a health IT, analytics, or software company with no direct care delivery
- Is a medical device or equipment manufacturer with no patient care operations
- Is in a completely unrelated sector (construction, finance, retail, food, etc.)

Base your evaluation on the SPECIFIC NICHE, not on healthcare broadly.
A pharma company does NOT belong in an elder-care niche. A dental company does NOT belong
in a behavioral health niche. Be specific.

Reply ONLY with JSON: {{"match": true/false, "reason": "one sentence"}}"""

    try:
        resp = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            timeout=20,
        )
        data = json.loads(resp.choices[0].message.content)
        return data.get('match'), data.get('reason')
    except:
        return True, "AI Error"

# ---------------------------------------------------------------------------
# WEB SPIDER
# ---------------------------------------------------------------------------

def firecrawl_scrape(url):
    try:
        r = requests.post(
            "https://api.firecrawl.dev/v1/scrape",
            headers={"Authorization": f"Bearer {FIRECRAWL_API_KEY}", "Content-Type": "application/json"},
            json={"url": url, "formats": ["markdown"]},
            timeout=20,
        )
        if r.status_code == 200:
            data = r.json()
            md = (data.get('data') or {}).get('markdown') or data.get('markdown')
            return md[:50000] if md else None
    except:
        pass
    return None

def extract_relevant_links(markdown_text, base_url):
    if not markdown_text: return []
    high = ["leadership", "executive", "our team", "care team", "management",
            "principals", "partners", "architects", "providers", "medical staff"]
    med  = ["about", "who we are", "meet", "staff", "firm", "studio", "people", "team", "contact"]
    skip = ['linkedin', 'facebook', 'twitter', 'pdf', 'jpg', 'login', 'mailto']
    candidates = []
    for text, link in re.findall(r'\[([^\]]+)\]\(([^)]+)\)', markdown_text):
        t = text.lower()
        score = 3 if any(x in t for x in high) else (1 if any(x in t for x in med) else 0)
        if not score: continue
        if link.startswith('/'): link = urljoin(base_url, link)
        elif not link.startswith('http'): continue
        if any(s in link for s in skip): continue
        candidates.append((score, link))
    candidates.sort(key=lambda x: x[0], reverse=True)
    seen, final = set(), []
    for _, u in candidates:
        if u not in seen:
            seen.add(u); final.append(u)
    return final[:4]

def extract_names_openai(text, company_name):
    prompt = f"""Analyze text from the website of "{company_name}".

Find: (1) the PRIMARY LEADER — CEO, Owner, Founder, President, Principal, Administrator,
Executive Director, or Medical Director. (2) Any contact email. (3) Any contact phone.

Return JSON only (use "None" string when not found):
{{"name": "Full Name or First Only", "title": "Their Title", "email": "email@example.com", "phone": "555-1234"}}

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
    except:
        return None

# ---------------------------------------------------------------------------
# APOLLO — PEOPLE
# ---------------------------------------------------------------------------

# Expanded tier-1 titles. Apollo stores titles as full strings like
# "Chief Executive Officer" — the old list only checked for "ceo" which is
# NOT a substring of "Chief Executive Officer".
_TIER1_TITLES = [
    'owner', 'founder', 'principal', 'managing partner', 'managing member',
    'managing director',
    # C-suite by full phrase (Apollo returns full titles, not acronyms)
    'chief executive', 'chief operating', 'chief financial', 'chief medical',
    'chief nursing', 'chief clinical', 'chief strategy', 'chief growth',
    # Acronyms (Apollo sometimes stores these)
    ' ceo', ' coo', ' cfo', ' cmo', ' cno',
    'president', 'partner', 'architect',
    'administrator', 'executive director', 'medical director', 'director of',
]
_TIER2_TITLES = [
    'vice president', ' vp ', 'manager', 'associate', 'coordinator', 'operations',
]

def get_people_apollo_robust(company_name, domain):
    url = "https://api.apollo.io/v1/mixed_people/search"
    headers = {'Content-Type': 'application/json', 'X-Api-Key': APOLLO_API_KEY}
    clean_name = clean_company_name_for_search(company_name)
    c_suite = ["owner", "founder", "c_suite", "president"]

    attempts = []
    if domain:
        attempts.append({"q_organization_domains": [domain], "person_seniority": c_suite, "per_page": 10})
        attempts.append({"q_organization_domains": [domain], "per_page": 35})
    attempts.append({"q_organization_names": [clean_name], "person_seniority": c_suite, "per_page": 10})
    attempts.append({"q_organization_names": [clean_name], "per_page": 15})

    for payload in attempts:
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=10)
            if r.status_code == 200:
                people = r.json().get('people', [])
                if people: return people
        except:
            pass
    return []

def select_best_apollo_contact(people):
    if not people: return None, "None"
    # Remove entries with no name or clearly placeholder last names
    valid = [p for p in people
             if p.get('first_name') and p.get('last_name')
             and str(p.get('last_name', '')).strip().lower() not in ('none', 'n/a', '')]
    if not valid: return None, "None"

    top = backup = None
    for p in valid:
        t = (p.get('title') or '').lower()
        if any(x in t for x in _TIER1_TITLES):
            top = p; break

    if top and not top.get('email'):
        for p in valid:
            t = (p.get('title') or '').lower()
            if any(x in t for x in _TIER2_TITLES) and p.get('email'):
                backup = p; break

    if top:
        src = "Apollo (Top)"
        if not top.get('email'):
            src += " [No Email]"
            if backup:
                top = dict(top)
                top['notes'] = f"Alt contact: {backup.get('first_name')} — {backup.get('email')}"
        return top, src

    # Fallback: first person with any name, prefer one with email
    with_email = [p for p in valid if p.get('email')]
    chosen = with_email[0] if with_email else valid[0]
    return chosen, "Apollo (Best Available)"

def repair_single_name(first_name, people_list):
    if not first_name or not people_list: return None
    target = first_name.split()[0].lower()
    for p in people_list:
        pf = (p.get('first_name') or '').lower()
        if pf == target or (len(target) > 2 and target in pf):
            return p
    return None

def bulk_enrich_names(people_list, domain):
    if not people_list or not domain: return []
    try:
        r = requests.post(
            "https://api.apollo.io/v1/people/bulk_match",
            headers={'Content-Type': 'application/json', 'X-Api-Key': APOLLO_API_KEY},
            json={"details": [{"first_name": p.get('first_name'),
                               "last_name": p.get('last_name'),
                               "domain": domain} for p in people_list]},
            timeout=15,
        )
        return r.json().get('matches', [])
    except:
        return []

# ---------------------------------------------------------------------------
# NEWS
# ---------------------------------------------------------------------------

def get_latest_news_link(company_name, city=None):
    q = f"{company_name} {city}" if city else company_name
    rss = f"https://news.google.com/rss/search?q={quote_plus(q)}&hl=en-US&gl=US&ceid=US:en"
    try:
        r = requests.get(rss, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code != 200: return None, None
        root = ET.fromstring(r.content)
        items = root.findall('./channel/item')
        if not items: return None, None
        return (items[0].findtext('title') or '').strip(), (items[0].findtext('link') or '').strip()
    except:
        return None, None

# ---------------------------------------------------------------------------
# WORKER
# ---------------------------------------------------------------------------

def process_single_company(org, specific_niche, strat_code):
    name = org.get('name')

    if not is_buyable_structure(org, strat_code)[0]: return None
    if is_obvious_mismatch(org, specific_niche, strat_code)[0]: return None

    desc = org.get('short_description') or org.get('headline') or ''
    tags = org.get('keywords') or []
    if not check_relevance_gpt4o(name, desc, tags, specific_niche, strat_code)[0]: return None

    domain = clean_domain(org.get('website_url'))
    row = {
        "Company": name,
        "Website": org.get('website_url'),
        "City": org.get('city'),
        "State": org.get('state'),
        "LinkedIn": org.get('linkedin_url'),
        "Employees": org.get('estimated_num_employees'),
        "CEO/Owner Name": "N/A", "Title": "N/A",
        "Email": "N/A", "Phone": "N/A",
        "Source": "Not Found", "Notes": "", "Confidence": "Low",
        "Latest News": "N/A",
    }

    found_person = apollo_cache = web_email = web_phone = None

    # --- Web spider ---
    if domain:
        queue = [f"https://{domain}"]
        visited = set()
        for url in queue[:6]:
            if url in visited: continue
            visited.add(url)
            content = firecrawl_scrape(url)
            if not content: continue
            ai = extract_names_openai(content, name)
            if ai:
                e, p = ai.get('email'), ai.get('phone')
                if e and e != 'None': web_email = e
                if p and p != 'None': web_phone = p
                n = ai.get('name', 'None')
                if n and n != 'None':
                    if ' ' in n and len(n) > 3:
                        found_person = {
                            'first_name': n.split()[0],
                            'last_name': ' '.join(n.split()[1:]),
                            'title': ai.get('title'),
                            'email': web_email,
                            'phone_numbers': [{'sanitized_number': web_phone}] if web_phone else [],
                        }
                        row['Source'] = 'Web Spider'
                        break
                    elif len(n) > 1:
                        if not apollo_cache:
                            apollo_cache = get_people_apollo_robust(name, domain)
                        repaired = repair_single_name(n, apollo_cache)
                        if repaired:
                            found_person = repaired
                            row['Source'] = 'Web → Repaired'
                            break
            for lnk in extract_relevant_links(content, url):
                if lnk not in visited: queue.insert(1, lnk)

    # --- Apollo fallback ---
    if not found_person:
        if not apollo_cache:
            apollo_cache = get_people_apollo_robust(name, domain)
        best, method = select_best_apollo_contact(apollo_cache)
        if best:
            found_person = best
            row['Source'] = method

    # --- Populate contact fields ---
    if found_person:
        row['CEO/Owner Name'] = f"{found_person.get('first_name','')} {found_person.get('last_name','')}".strip()
        row['Title'] = found_person.get('title') or 'N/A'

        if 'Web' in row['Source'] and domain:
            matches = bulk_enrich_names([found_person], domain)
            if matches and matches[0]:
                found_person = matches[0]
                row['Source'] += ' → Verified'
                row['Confidence'] = 'High'
        elif 'Apollo' in row['Source']:
            row['Confidence'] = 'Medium'

        a_email = found_person.get('email')
        row['Email'] = a_email if a_email else (web_email or 'N/A')
        pnums = found_person.get('phone_numbers') or []
        a_phone = pnums[0].get('sanitized_number') if pnums else None
        row['Phone'] = a_phone if a_phone else (web_phone or 'N/A')
        if found_person.get('notes'): row['Notes'] = found_person['notes']
    else:
        if web_email: row['Email'] = web_email
        if web_phone: row['Phone'] = web_phone

    # --- News ---
    t, u = get_latest_news_link(name, org.get('city'))
    if u: row['Latest News'] = f"{t} | {u}" if t else u

    return row

# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

APOLLO_INDUSTRIES = [
    "Accounting", "Airlines/Aviation", "Alternative Dispute Resolution", "Alternative Medicine",
    "Animation", "Apparel & Fashion", "Architecture & Planning", "Arts and Crafts", "Automotive",
    "Aviation & Aerospace", "Banking", "Biotechnology", "Broadcast Media", "Building Materials",
    "Business Supplies and Equipment", "Capital Markets", "Chemicals", "Civic & Social Organization",
    "Civil Engineering", "Commercial Real Estate", "Computer & Network Security", "Computer Games",
    "Computer Hardware", "Computer Networking", "Computer Software", "Construction",
    "Consumer Electronics", "Consumer Goods", "Consumer Services", "Cosmetics", "Dairy",
    "Defense & Space", "Design", "Education Management", "E-Learning",
    "Electrical/Electronic Manufacturing", "Entertainment", "Environmental Services",
    "Events Services", "Executive Office", "Facilities Services", "Farming", "Financial Services",
    "Fine Art", "Food & Beverages", "Food Production", "Fund-Raising", "Furniture",
    "Gambling & Casinos", "Glass, Ceramics & Concrete", "Government Administration",
    "Government Relations", "Graphic Design", "Health, Wellness and Fitness", "Higher Education",
    "Hospital & Health Care", "Hospitality", "Human Resources", "Import and Export",
    "Individual & Family Services", "Industrial Automation", "Information Services",
    "Information Technology and Services", "Insurance", "International Affairs",
    "International Trade and Development", "Internet", "Investment Banking",
    "Investment Management", "Judiciary", "Law Enforcement", "Law Practice", "Legal Services",
    "Legislative Office", "Leisure, Travel & Tourism", "Libraries", "Logistics and Supply Chain",
    "Luxury Goods & Jewelry", "Machinery", "Management Consulting", "Maritime", "Market Research",
    "Marketing and Advertising", "Mechanical or Industrial Engineering", "Media Production",
    "Medical Devices", "Medical Practice", "Mental Health Care", "Military", "Mining & Metals",
    "Motion Pictures and Film", "Museums and Institutions", "Music", "Nanotechnology",
    "Newspapers", "Non-Profit Organization Management", "Oil & Energy", "Online Media",
    "Outsourcing/Offshoring", "Package/Freight Delivery", "Packaging and Containers",
    "Paper & Forest Products", "Performing Arts", "Pharmaceuticals", "Philanthropy",
    "Photography", "Plastics", "Political Organization", "Primary/Secondary Education",
    "Printing", "Professional Training & Coaching", "Program Development", "Public Policy",
    "Public Relations and Communications", "Public Safety", "Publishing", "Railroad Manufacture",
    "Ranching", "Real Estate", "Recreational Facilities and Services", "Religious Institutions",
    "Renewables & Environment", "Research", "Restaurants", "Retail",
    "Security and Investigations", "Semiconductors", "Shipbuilding", "Sporting Goods", "Sports",
    "Staffing and Recruiting", "Supermarkets", "Telecommunications", "Textiles", "Think Tanks",
    "Tobacco", "Translation and Localization", "Transportation/Trucking/Railroad", "Utilities",
    "Venture Capital & Private Equity", "Veterinary", "Warehousing", "Wholesale",
    "Wine and Spirits", "Wireless", "Writing and Editing",
]

st.title("🚀 NCP Sourcing Engine (Turbo)")
st.markdown("Automated Deal Sourcing with Multi-Threaded AI.")

with st.form("sourcing_form"):
    col1, col2 = st.columns(2)

    # *** CHANGED: multiselect so users can combine industries ***
    broad_industries = col1.multiselect(
        "1. Apollo Industries (select one or more)",
        options=APOLLO_INDUSTRIES,
        default=["Hospital & Health Care"],
        help="For niche targets like PACE, combine multiple categories — e.g. "
             "Hospital & Health Care + Individual & Family Services + "
             "Non-Profit Organization Management",
    )
    specific_niche = col2.text_input(
        "2. Specific Niche (AI Filter)",
        value="Program for All-Inclusive Care for the Elderly (PACE)",
    )

    col3, col4 = st.columns(2)
    target_geo = col3.text_input("3. Geography", value="North Carolina, United States")
    mode = col4.selectbox("4. Strategy", [
        "A - Buy/Private  (Strict: direct operators, private company, ≤7,500 employees)",
        "B - Sell/Scout   (Broad: same care sector, all sizes including health systems)",
    ])

    apollo_keywords_raw = st.text_input(
        "5. Apollo Keywords (optional — comma-separated to narrow the Apollo search)",
        value="",
        placeholder="e.g. PACE, elderly care, adult day care",
    )

    submitted = st.form_submit_button("Start Sourcing 💎", type="primary")

if submitted:
    if not broad_industries:
        st.error("Please select at least one Apollo Industry.")
        st.stop()

    strat_code = "A" if "A -" in mode else "B"
    keyword_tags = [k.strip() for k in apollo_keywords_raw.split(',') if k.strip()] or None

    st.info(f"🔎 Searching Apollo for **{', '.join(broad_industries)}** in **{target_geo}**...")
    orgs = search_organizations(broad_industries, target_geo, keyword_tags=keyword_tags)

    if not orgs:
        st.error(
            "No companies found. Try a broader industry selection, remove keyword filters, "
            "or verify the geography spelling."
        )
        st.stop()

    st.success(
        f"Found **{len(orgs)}** candidates. Running 5 parallel workers through filters..."
    )
    progress_bar = st.progress(0)
    status_text = st.empty()
    final_data = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(process_single_company, org, specific_niche, strat_code): org
            for org in orgs
        }
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            result = future.result()
            if result: final_data.append(result)
            progress_bar.progress((i + 1) / len(orgs))
            status_text.caption(
                f"Processed {i+1}/{len(orgs)} | {len(final_data)} passed so far..."
            )

    status_text.write("✅ Sourcing complete!")

    if final_data:
        df = pd.DataFrame(final_data)
        st.dataframe(df)
        csv = df.to_csv(index=False).encode('utf-8')
        fname = f"NCP_{'_'.join(broad_industries[:2])}_{target_geo}.csv".replace(" ", "_").replace(",", "")
        st.download_button("Download CSV", data=csv, file_name=fname, mime="text/csv", type="primary")
    else:
        industries_hint = " + ".join(broad_industries)
        st.warning(
            f"No targets passed the filters for **{industries_hint}**.\n\n"
            "**Tips to get results:**\n"
            "- Add more industry categories (e.g. *Individual & Family Services*, "
            "*Non-Profit Organization Management*, *Health, Wellness and Fitness*)\n"
            "- Add Apollo Keywords (field 5) such as the niche name or key service terms\n"
            "- Switch to **Mode B** for a broader sweep\n"
            "- Broaden the Specific Niche description in field 2"
        )
