import streamlit as st
import pandas as pd
import requests
import json
import re
import time
import concurrent.futures
import xml.etree.ElementTree as ET
from urllib.parse import urlparse, urljoin, quote_plus
from openai import OpenAI

# --- PAGE CONFIGURATION ---
st.set_page_config(page_title="NCP Sourcing Engine", page_icon="🚀", layout="wide")

# --- PASSWORD PROTECTION ---
def check_password():
    """Returns `True` if the user had the correct password."""

    def password_entered():
        """Checks whether a password entered by the user is correct."""
        if st.session_state["password"] == "NCP2026":
            st.session_state["password_correct"] = True
            del st.session_state["password"]  # don't store password
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        st.text_input(
            "Enter Password", type="password", on_change=password_entered, key="password"
        )
        return False
    elif not st.session_state["password_correct"]:
        st.text_input(
            "Enter Password", type="password", on_change=password_entered, key="password"
        )
        st.error("😕 Password incorrect")
        return False
    else:
        return True

if not check_password():
    st.stop()

# --- SECRETS MANAGEMENT ---
try:
    APOLLO_API_KEY = st.secrets["APOLLO_API_KEY"]
    OPENAI_API_KEY = st.secrets["OPENAI_API_KEY"]
    FIRECRAWL_API_KEY = st.secrets["FIRECRAWL_API_KEY"]
except FileNotFoundError:
    st.error("❌ API Keys missing! Please set them in `.streamlit/secrets.toml`.")
    st.stop()

client = OpenAI(api_key=OPENAI_API_KEY)

# --- CORE FUNCTIONS ---

def clean_domain(url):
    if not url or not isinstance(url, str): return None
    try:
        if not url.startswith('http'): url = 'http://' + url
        parsed = urlparse(url)
        domain = parsed.netloc
        if domain.startswith('www.'): domain = domain[4:]
        return domain
    except: return None

def search_organizations(industry, location_input, keyword_tags=None, max_pages=2):
    """
    FIX #1: Use q_organization_industries (not q_organization_keyword_tags) for the
    industry dropdown so Apollo filters by actual industry taxonomy.
    FIX #2: Accept optional keyword_tags for specific term filtering within that industry.
    FIX #3: Paginate up to max_pages (default 2 = up to 200 companies).
    """
    url = "https://api.apollo.io/v1/organizations/search"
    headers = {'Content-Type': 'application/json', 'X-Api-Key': APOLLO_API_KEY}
    all_orgs = []

    for page in range(1, max_pages + 1):
        payload = {
            "organization_locations": [location_input],
            "page": page,
            "per_page": 100,
        }
        if industry:
            payload["q_organization_industries"] = [industry]
        if keyword_tags:
            payload["q_organization_keyword_tags"] = keyword_tags

        try:
            response = requests.post(url, headers=headers, json=payload, timeout=15)
            if response.status_code != 200:
                break
            orgs = response.json().get('organizations', [])
            if not orgs:
                break
            all_orgs.extend(orgs)
            if len(orgs) < 100:  # fewer than a full page — no point paginating further
                break
        except:
            break

    return all_orgs

def is_obvious_mismatch(org, target_niche, mode):
    """
    FIX #4: Skip poison pill filter entirely for Mode B (any ownership).
    Mode A retains strict filtering.
    """
    if mode == 'B':
        return False, "Pass (Mode B — skipped)"

    name = (org.get('name') or "").lower()
    tags = [t.lower() for t in org.get('keywords', [])]

    poison = ['consulting', 'staffing', 'recruiting', 'software', 'technology', 'saas',
              'billing', 'platform', 'marketing', 'agency']
    if "architect" in target_niche.lower():
        poison.extend(['realty', 'real estate', 'tax', 'accounting', 'legal', 'law',
                       'supplies', 'material', 'wood', 'lumber', 'golf', 'naval', 'marine'])

    for p in poison:
        if p in name: return True, f"Bad Name ('{p}')"
        if p in tags: return True, f"Bad Tag ('{p}')"
    return False, "Pass"

def check_relevance_gpt4o(company_name, description, keywords, target_niche, mode):
    if mode == 'A':
        prompt = f"""
        I am a private equity investor looking to acquire founder-owned businesses in this niche: "{target_niche}".

        Candidate: "{company_name}"
        Description: "{description}"
        Keywords: {keywords}

        Is this an OPERATOR that directly delivers services/products in this niche?
        - YES if it is an operator (care provider, clinic, facility, service company) in the niche.
        - If the description is empty or vague, evaluate the COMPANY NAME carefully:
            - If the name is clearly consistent with the niche, say YES.
            - If the name suggests a different business (e.g. fitness, real estate, software), say NO.
        - NO if it is clearly a Service Provider to the industry (consulting, legal, software vendor).
        - NO if it is clearly a Supplier (equipment, materials).
        - NO if it is clearly in a completely different sector.
        - Be especially careful with niche acronyms — e.g. "PACE" means Program for All-Inclusive Care
          for the Elderly (a Medicare/Medicaid program), NOT pace/speed/fitness.

        Answer ONLY with JSON: {{ "match": true/false, "reason": "short reason" }}
        """
    else:  # Mode B — permissive
        prompt = f"""
        I am a private equity investor looking for any business loosely related to: "{target_niche}".

        Candidate: "{company_name}"
        Description: "{description}"
        Keywords: {keywords}

        Is this company potentially relevant to the "{target_niche}" space?
        - YES unless it is completely unrelated (e.g., a restaurant chain when searching healthcare).
        - If the description is empty, evaluate whether the company NAME is plausibly related to the niche.
        - NO only if it is clearly in a totally different industry.

        Answer ONLY with JSON: {{ "match": true/false, "reason": "short reason" }}
        """
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            timeout=20,
        )
        data = json.loads(response.choices[0].message.content)
        return data.get('match'), data.get('reason')
    except:
        return True, "AI Error"

def is_buyable_structure(org, mode):
    if mode == 'A':
        status = str(org.get('ownership_status', '')).lower()
        tags = [t.lower() for t in org.get('keywords', [])]
        if 'public' in status: return False, "Publicly Traded"
        if 'subsidiary' in status: return False, "Subsidiary"
        if 'private equity' in tags: return False, "PE/VC Backed"
        emp_count = org.get('estimated_num_employees', 0) or 0
        if emp_count > 2000: return False, f"Too Large ({emp_count} employees)"
    return True, "Valid Structure"

def firecrawl_scrape(url):
    api_url = "https://api.firecrawl.dev/v1/scrape"
    headers = {"Authorization": f"Bearer {FIRECRAWL_API_KEY}", "Content-Type": "application/json"}
    payload = {"url": url, "formats": ["markdown"]}
    try:
        res = requests.post(api_url, headers=headers, json=payload, timeout=20)
        if res.status_code == 200:
            data = res.json()
            # Handle both response shapes Firecrawl has used
            markdown = (data.get('data') or {}).get('markdown') or data.get('markdown')
            if markdown:
                return markdown[:50000]
    except:
        pass
    return None

def extract_relevant_links(markdown_text, base_url):
    if not markdown_text: return []
    links = re.findall(r'\[([^\]]+)\]\(([^)]+)\)', markdown_text)
    # FIX #6: Added healthcare-specific link keywords
    high = ["leadership", "executive", "our team", "care team", "management", "principals",
            "partners", "architects", "providers", "staff directory", "medical staff"]
    med = ["about", "who we are", "meet", "staff", "firm", "studio", "people", "team", "contact"]
    candidates = []
    for text, link in links:
        score = 0
        t, l = text.lower(), link.lower()
        if any(x in t for x in high): score = 3
        elif any(x in t for x in med): score = 1
        if score > 0:
            if link.startswith('/'): full = urljoin(base_url, link)
            elif link.startswith('http'): full = link
            else: continue
            if any(j in full for j in ['linkedin', 'facebook', 'twitter', 'pdf', 'jpg', 'login']): continue
            candidates.append((score, full))
    candidates.sort(key=lambda x: x[0], reverse=True)
    seen = set(); final = []
    for s, u in candidates:
        if u not in seen: seen.add(u); final.append(u)
    return final[:4]

def extract_names_openai(text, company_name):
    prompt = f"""
    Analyze the text from the website of "{company_name}".

    1. Identify the PRIMARY LEADER (CEO, Owner, Founder, President, Principal, Administrator, Executive Director, Medical Director).
    2. Extract any contact email address visible on the page.
    3. Extract any contact phone number visible on the page.

    Rules:
    - name: Full name if found. First name only if that is all that appears. "None" if not found.
    - title: Their title, "None" if not found.
    - email: An email address found on the page, "None" if not found.
    - phone: A phone number found on the page, "None" if not found.

    Return JSON only:
    {{ "name": "John Doe", "title": "CEO", "email": "john@example.com", "phone": "704-555-1234" }}

    Text: {text[:15000]}
    """
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            timeout=20,
        )
        return json.loads(response.choices[0].message.content)
    except:
        return None

def clean_company_name_for_search(name):
    if not name: return ""
    clean = name.replace(',', '').replace('.', '')
    for s in [' inc', ' llc', ' group', ' ltd', ' corp', ' p.c.', ' pc', ' architects', ' architecture']:
        if clean.lower().endswith(s): clean = clean[:-len(s)]
    return clean.strip()

def get_people_apollo_robust(company_name, domain):
    url = "https://api.apollo.io/v1/mixed_people/search"
    headers = {'Content-Type': 'application/json', 'X-Api-Key': APOLLO_API_KEY}
    clean_name = clean_company_name_for_search(company_name)
    c_suite_seniority = ["owner", "founder", "c_suite", "president"]

    if domain:
        try:
            # First pass: C-suite/owner only
            res = requests.post(url, headers=headers,
                                json={"q_organization_domains": [domain],
                                      "person_seniority": c_suite_seniority,
                                      "per_page": 10}, timeout=10)
            if res.status_code == 200:
                people = res.json().get('people', [])
                if people:
                    return people
        except:
            pass
        try:
            # Second pass: no seniority filter — broader net
            res = requests.post(url, headers=headers,
                                json={"q_organization_domains": [domain], "per_page": 35}, timeout=10)
            if res.status_code == 200:
                return res.json().get('people', [])
        except:
            pass
    else:
        # No domain — fall back to name-based lookup
        try:
            res = requests.post(url, headers=headers,
                                json={"q_organization_names": [clean_name],
                                      "person_seniority": c_suite_seniority,
                                      "per_page": 10}, timeout=10)
            if res.status_code == 200:
                return res.json().get('people', [])
        except:
            pass
    return []

def select_best_apollo_contact(people):
    if not people: return None, "None"
    valid = [p for p in people if p.get('first_name') and p.get('last_name')
             and "Bill" not in p.get('first_name', '') and "None" not in p.get('last_name', '')]
    if not valid: return None, "None"
    # FIX #7: Added healthcare leadership titles (administrator, executive director, etc.)
    tier1 = ['owner', 'principal', 'founder', 'ceo', 'president', 'partner', 'architect',
             'administrator', 'executive director', 'medical director', 'director of']
    tier2 = ['associate', 'manager', 'vp', 'operations', 'coordinator']
    top = None; backup = None
    for p in valid:
        if any(x in (p.get('title') or "").lower() for x in tier1):
            top = p; break
    if top and not top.get('email'):
        for p in valid:
            if (any(x in (p.get('title') or "").lower() for x in tier2)
                    or 'assistant' in (p.get('title') or "").lower()) and p.get('email'):
                backup = p; break
    if top:
        src = "Apollo (Top)"
        if not top.get('email'):
            src += " [No Email]"
            if backup: top['notes'] = f"Alt: {backup.get('first_name')} - {backup.get('email')}"
        return top, src
    return valid[0], "Apollo (Best Available)"

def repair_single_name(first_name, people_list):
    if not first_name or not people_list: return None
    target = first_name.split()[0].lower().strip()
    for p in people_list:
        p_first = (p.get('first_name') or "").lower().strip()
        if p_first == target or (len(target) > 2 and target in p_first): return p
    return None

def get_latest_news_link(company_name, city=None):
    """Search Google News RSS for the most recent article about the company.
    Returns (title, url) or (None, None) if nothing found."""
    query = company_name
    if city:
        query = f"{company_name} {city}"
    rss_url = f"https://news.google.com/rss/search?q={quote_plus(query)}&hl=en-US&gl=US&ceid=US:en"
    try:
        res = requests.get(rss_url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        if res.status_code != 200:
            return None, None
        root = ET.fromstring(res.content)
        # RSS items live at channel > item
        items = root.findall('./channel/item')
        if not items:
            return None, None
        first = items[0]
        title = first.findtext('title') or ""
        link = first.findtext('link') or ""
        # Google News RSS wraps the real URL in the <link> tag directly
        return title.strip(), link.strip()
    except:
        return None, None

def bulk_enrich_names(people_list, domain):
    if not people_list or not domain: return []
    url = "https://api.apollo.io/v1/people/bulk_match"
    try:
        res = requests.post(
            url,
            headers={'Content-Type': 'application/json', 'X-Api-Key': APOLLO_API_KEY},
            json={"details": [{"first_name": p.get('first_name'), "last_name": p.get('last_name'),
                               "domain": domain} for p in people_list]},
            timeout=15,
        )
        return res.json().get('matches', [])
    except:
        return []

# --- MULTI-THREADED WORKER ---
def process_single_company(org, specific_niche, strat_code):
    """The full logic for one company, isolated for threading."""
    comp_name = org.get('name')

    # 1. STRUCTURE CHECK
    is_valid, reason = is_buyable_structure(org, strat_code)
    if not is_valid: return None

    # 2. POISON PILLS (FIX: pass mode so Mode B skips this)
    is_bad, reason = is_obvious_mismatch(org, specific_niche, strat_code)
    if is_bad: return None

    # 3. AI GATEKEEPER (FIX: pass mode for prompt selection)
    desc = org.get('short_description') or org.get('headline') or ""
    tags = org.get('keywords') or []
    is_relevant, reason = check_relevance_gpt4o(comp_name, desc, tags, specific_niche, strat_code)
    if not is_relevant: return None

    # --- START PROCESSING VALID TARGET ---
    domain = clean_domain(org.get('website_url'))
    row = {
        "Company": comp_name,
        "Website": org.get('website_url'),
        "City": org.get('city'),
        "State": org.get('state'),
        "LinkedIn": org.get('linkedin_url'),
        "Employees": org.get('estimated_num_employees'),
        "CEO/Owner Name": "N/A", "Title": "N/A", "Email": "N/A", "Phone": "N/A",
        "Source": "Pending", "Notes": "", "Confidence": "Low", "Latest News": "N/A"
    }

    found_person = None
    apollo_cache = None
    web_email = None
    web_phone = None

    # 4. WEB SPIDER
    if domain:
        home_url = f"https://{domain}"
        queue = [home_url]
        visited = set()

        for url in queue[:6]:
            if url in visited: continue
            visited.add(url)
            content = firecrawl_scrape(url)
            if content:
                ai_data = extract_names_openai(content, comp_name)
                if ai_data:
                    # Capture email/phone from web even if name not found
                    extracted_email = ai_data.get('email')
                    extracted_phone = ai_data.get('phone')
                    if extracted_email and extracted_email != "None":
                        web_email = extracted_email
                    if extracted_phone and extracted_phone != "None":
                        web_phone = extracted_phone

                    name = ai_data.get('name', 'None')
                    if name and name != "None":
                        if " " in name and len(name) > 3:
                            found_person = {"first_name": name.split()[0],
                                            "last_name": " ".join(name.split()[1:]),
                                            "title": ai_data.get('title'),
                                            "email": web_email,
                                            "phone_numbers": [{"sanitized_number": web_phone}] if web_phone else []}
                            row['Source'] = "Web Spider"; break
                        elif len(name) > 1:
                            if not apollo_cache: apollo_cache = get_people_apollo_robust(comp_name, domain)
                            repaired = repair_single_name(name, apollo_cache)
                            if repaired:
                                found_person = repaired
                                row['Source'] = "Web -> Repaired"; break
                links = extract_relevant_links(content, url)
                for l in links:
                    if l not in visited: queue.insert(1, l)

    # 5. APOLLO BACKUP
    if not found_person:
        if not apollo_cache: apollo_cache = get_people_apollo_robust(comp_name, domain)
        best, method = select_best_apollo_contact(apollo_cache)
        if best:
            found_person = best
            row['Source'] = method

    # 6. ENRICH & SAVE
    if found_person:
        row['CEO/Owner Name'] = f"{found_person.get('first_name', '')} {found_person.get('last_name', '')}".strip()
        row['Title'] = found_person.get('title') or 'N/A'
        if "Web" in row['Source'] and domain:
            matches = bulk_enrich_names([found_person], domain)
            if matches and matches[0]:
                found_person = matches[0]
                row['Source'] += " -> Verified"
                row['Confidence'] = "High"
        elif "Apollo" in row['Source']:
            row['Confidence'] = "Medium"

        # Email: prefer Apollo enriched, fall back to web-extracted
        apollo_email = found_person.get('email')
        row['Email'] = apollo_email if apollo_email else (web_email or 'N/A')

        # Phone: prefer Apollo enriched, fall back to web-extracted
        pnums = found_person.get('phone_numbers', [])
        apollo_phone = pnums[0].get('sanitized_number') if pnums else None
        row['Phone'] = apollo_phone if apollo_phone else (web_phone or 'N/A')

        if found_person.get('notes'): row['Notes'] = found_person.get('notes')
    else:
        # No person found at all — still surface any web-scraped contact info
        row['CEO/Owner Name'] = 'N/A'
        if web_email: row['Email'] = web_email
        if web_phone: row['Phone'] = web_phone

    # 7. LATEST NEWS
    news_title, news_url = get_latest_news_link(comp_name, org.get('city'))
    if news_url:
        row['Latest News'] = f"{news_title} | {news_url}" if news_title else news_url

    return row

# --- UI LAYOUT ---
st.title("🚀 NCP Sourcing Engine (Turbo)")
st.markdown("Automated Deal Sourcing with Multi-Threaded AI.")

# Apollo's Standard Industry List
APOLLO_INDUSTRIES = [
    "Accounting", "Airlines/Aviation", "Alternative Dispute Resolution", "Alternative Medicine", "Animation", "Apparel & Fashion",
    "Architecture & Planning", "Arts and Crafts", "Automotive", "Aviation & Aerospace", "Banking", "Biotechnology", "Broadcast Media",
    "Building Materials", "Business Supplies and Equipment", "Capital Markets", "Chemicals", "Civic & Social Organization", "Civil Engineering",
    "Commercial Real Estate", "Computer & Network Security", "Computer Games", "Computer Hardware", "Computer Networking", "Computer Software",
    "Construction", "Consumer Electronics", "Consumer Goods", "Consumer Services", "Cosmetics", "Dairy", "Defense & Space", "Design",
    "Education Management", "E-Learning", "Electrical/Electronic Manufacturing", "Entertainment", "Environmental Services", "Events Services",
    "Executive Office", "Facilities Services", "Farming", "Financial Services", "Fine Art", "Food & Beverages", "Food Production", "Fund-Raising",
    "Furniture", "Gambling & Casinos", "Glass, Ceramics & Concrete", "Government Administration", "Government Relations", "Graphic Design",
    "Health, Wellness and Fitness", "Higher Education", "Hospital & Health Care", "Hospitality", "Human Resources", "Import and Export",
    "Individual & Family Services", "Industrial Automation", "Information Services", "Information Technology and Services", "Insurance",
    "International Affairs", "International Trade and Development", "Internet", "Investment Banking", "Investment Management", "Judiciary",
    "Law Enforcement", "Law Practice", "Legal Services", "Legislative Office", "Leisure, Travel & Tourism", "Libraries", "Logistics and Supply Chain",
    "Luxury Goods & Jewelry", "Machinery", "Management Consulting", "Maritime", "Market Research", "Marketing and Advertising",
    "Mechanical or Industrial Engineering", "Media Production", "Medical Devices", "Medical Practice", "Mental Health Care", "Military",
    "Mining & Metals", "Motion Pictures and Film", "Museums and Institutions", "Music", "Nanotechnology", "Newspapers",
    "Non-Profit Organization Management", "Oil & Energy", "Online Media", "Outsourcing/Offshoring", "Package/Freight Delivery",
    "Packaging and Containers", "Paper & Forest Products", "Performing Arts", "Pharmaceuticals", "Philanthropy", "Photography", "Plastics",
    "Political Organization", "Primary/Secondary Education", "Printing", "Professional Training & Coaching", "Program Development", "Public Policy",
    "Public Relations and Communications", "Public Safety", "Publishing", "Railroad Manufacture", "Ranching", "Real Estate",
    "Recreational Facilities and Services", "Religious Institutions", "Renewables & Environment", "Research", "Restaurants", "Retail",
    "Security and Investigations", "Semiconductors", "Shipbuilding", "Sporting Goods", "Sports", "Staffing and Recruiting", "Supermarkets",
    "Telecommunications", "Textiles", "Think Tanks", "Tobacco", "Translation and Localization", "Transportation/Trucking/Railroad", "Utilities",
    "Venture Capital & Private Equity", "Veterinary", "Warehousing", "Wholesale", "Wine and Spirits", "Wireless", "Writing and Editing"
]

with st.form("sourcing_form"):
    col1, col2 = st.columns(2)
    broad_industry = col1.selectbox("1. Broad Apollo Industry", options=APOLLO_INDUSTRIES, index=56)  # Hospital & Health Care
    specific_niche = col2.text_input("2. Specific Niche (AI Filter)", value="Program for All-Inclusive Care for the Elderly (PACE)")

    col3, col4 = st.columns(2)
    target_geo = col3.text_input("3. Geography", value="North Carolina, United States")
    mode = col4.selectbox("4. Strategy", ["A - Buy/Private (Strict)", "B - Sell/Any (Loose)"])

    # FIX #10: New optional field — lets users pass specific search terms to Apollo
    # (e.g. "PACE", "elder care", "senior care") to narrow results within the industry
    apollo_keywords_raw = st.text_input(
        "5. Apollo Keywords (optional — comma-separated terms to narrow the search within the industry above)",
        value="",
        placeholder="e.g. PACE, elderly care, adult day care"
    )

    submitted = st.form_submit_button("Start Sourcing 💎", type="primary")

if submitted:
    strat_code = "A" if "A -" in mode else "B"

    # Parse optional keyword tags
    apollo_keyword_tags = [k.strip() for k in apollo_keywords_raw.split(',') if k.strip()] or None

    st.info(f"🔎 Searching Apollo for **{broad_industry}** in **{target_geo}**...")

    orgs = search_organizations(broad_industry, target_geo, keyword_tags=apollo_keyword_tags)

    if not orgs:
        st.error(
            "No companies found via Apollo. "
            "Try a broader industry category, remove keyword tags, or check your geography spelling."
        )
    else:
        st.success(f"Found **{len(orgs)}** candidates from Apollo. Running 5x Parallel Workers through AI filter...")

        progress_bar = st.progress(0)
        status_text = st.empty()
        final_data = []

        # Parallel Execution
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(process_single_company, org, specific_niche, strat_code): org for org in orgs}

            for i, future in enumerate(concurrent.futures.as_completed(futures)):
                result = future.result()
                if result:
                    final_data.append(result)

                progress = (i + 1) / len(orgs)
                progress_bar.progress(progress)
                status_text.caption(f"Processed {i+1}/{len(orgs)} companies | {len(final_data)} passed filters so far...")

        status_text.write("✅ **Sourcing Complete!**")

        if final_data:
            df = pd.DataFrame(final_data)
            st.dataframe(df)
            csv = df.to_csv(index=False).encode('utf-8')
            filename = f"NCP_{broad_industry}_{target_geo}.csv".replace(" ", "_").replace(",", "")
            st.download_button(label="Download CSV", data=csv, file_name=filename, mime="text/csv", type="primary")
        else:
            st.warning(
                "No valid targets passed the filters. "
                "Try switching to **Mode B** for broader results, or adjust your Specific Niche description."
            )
