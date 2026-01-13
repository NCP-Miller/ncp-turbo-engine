import streamlit as st
import pandas as pd
import requests
import json
import re
import time
import concurrent.futures
from urllib.parse import urlparse, urljoin
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
        # First run, show input for password.
        st.text_input(
            "Enter Password", type="password", on_change=password_entered, key="password"
        )
        return False
    elif not st.session_state["password_correct"]:
        # Password not correct, show input + error.
        st.text_input(
            "Enter Password", type="password", on_change=password_entered, key="password"
        )
        st.error("😕 Password incorrect")
        return False
    else:
        # Password correct.
        return True

if not check_password():
    st.stop()  # Do not run the rest of the app if password is wrong.

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

def search_organizations(keyword, location_input):
    url = "https://api.apollo.io/v1/organizations/search"
    headers = {'Content-Type': 'application/json', 'X-Api-Key': APOLLO_API_KEY}
    # Fetch 100 to catch deep targets
    payload = {"q_organization_keyword_tags": [keyword], "organization_locations": [location_input], "page": 1, "per_page": 100}
    try:
        response = requests.post(url, headers=headers, json=payload)
        return response.json().get('organizations', [])
    except: return []

def is_obvious_mismatch(org, target_niche):
    name = (org.get('name') or "").lower()
    tags = [t.lower() for t in org.get('keywords', [])]
    
    poison = ['consulting', 'staffing', 'recruiting', 'software', 'technology', 'saas', 'billing', 'platform', 'marketing', 'agency']
    if "architect" in target_niche.lower():
        poison.extend(['realty', 'real estate', 'tax', 'accounting', 'legal', 'law', 'supplies', 'material', 'wood', 'lumber', 'golf', 'naval', 'marine'])
        
    for p in poison:
        if p in name: return True, f"Bad Name ('{p}')"
        if p in tags: return True, f"Bad Tag ('{p}')"
    return False, "Pass"

def check_relevance_gpt4o(company_name, description, keywords, target_niche):
    prompt = f"""
    I am a private equity sourcer looking to buy companies in this niche: "{target_niche}".
    
    Candidate: "{company_name}"
    Description: "{description}"
    Keywords: {keywords}
    
    Task: Is this a VALID acquisition target?
    - Strict NO if it is a Service Provider (Tax, Legal, Realty).
    - Strict NO if it is a Supplier (wood, software).
    - Strict NO if wrong sub-sector (e.g. Golf Architect vs Residential).
    
    Answer ONLY with JSON: {{ "match": true/false, "reason": "short reason" }}
    """
    try:
        response = client.chat.completions.create(
            model="gpt-4o", messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"}
        )
        data = json.loads(response.choices[0].message.content)
        return data.get('match'), data.get('reason')
    except: return True, "AI Error"

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
        res = requests.post(api_url, headers=headers, json=payload)
        if res.status_code == 200:
            return res.json()['data']['markdown'][:50000]
    except: pass
    return None

def extract_relevant_links(markdown_text, base_url):
    if not markdown_text: return []
    links = re.findall(r'\[([^\]]+)\]\(([^)]+)\)', markdown_text)
    high = ["leadership", "executive", "our team", "care team", "management", "principals", "partners", "architects"]
    med = ["about", "who we are", "meet", "staff", "firm", "studio", "people"]
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
    Analyze text for "{company_name}". Identify PRIMARY LEADER (Owner, Principal, Founder, CEO).
    Rules: Return Full Name if found. Return First Name if only First Name found.
    Return JSON: {{ "name": "John Doe", "title": "Principal" }} or {{ "name": "None", "title": "None" }}
    Text: {text[:15000]}
    """
    try:
        response = client.chat.completions.create(
            model="gpt-4o", messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"}
        )
        return json.loads(response.choices[0].message.content)
    except: return None

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
    try:
        requests.post(url, headers=headers, json={"q_organization_names": [clean_name], "per_page": 35})
    except: pass
    if domain:
        try:
            res = requests.post(url, headers=headers, json={"q_organization_domains": [domain], "per_page": 35})
            if res.status_code == 200: return res.json().get('people', [])
        except: pass
    return []

def select_best_apollo_contact(people):
    if not people: return None, "None"
    valid = [p for p in people if "Bill" not in p.get('first_name','') and "None" not in p.get('last_name','')]
    if not valid: return None, "None"
    tier1 = ['owner', 'principal', 'founder', 'ceo', 'president', 'partner', 'architect']
    tier2 = ['associate', 'manager', 'director', 'vp', 'operations']
    top = None; backup = None
    for p in valid:
        if any(x in (p.get('title') or "").lower() for x in tier1): top = p; break
    if top and not top.get('email'):
        for p in valid:
            if (any(x in (p.get('title') or "").lower() for x in tier2) or 'assistant' in (p.get('title') or "").lower()) and p.get('email'): backup = p; break
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

def bulk_enrich_names(people_list, domain):
    if not people_list or not domain: return []
    url = "https://api.apollo.io/v1/people/bulk_match"
    try:
        res = requests.post(url, headers={'Content-Type': 'application/json', 'X-Api-Key': APOLLO_API_KEY}, 
                            json={"details": [{"first_name": p.get('first_name'), "last_name": p.get('last_name'), "domain": domain} for p in people_list]})
        return res.json().get('matches', [])
    except: return []

# --- MULTI-THREADED WORKER ---
def process_single_company(org, specific_niche, strat_code):
    """The full logic for one company, isolated for threading."""
    comp_name = org.get('name')
    
    # 1. STRUCTURE CHECK
    is_valid, reason = is_buyable_structure(org, strat_code)
    if not is_valid: return None

    # 2. POISON PILLS
    is_bad, reason = is_obvious_mismatch(org, specific_niche)
    if is_bad: return None

    # 3. AI GATEKEEPER
    desc = org.get('short_description') or org.get('headline') or ""
    tags = org.get('keywords') or []
    is_relevant, reason = check_relevance_gpt4o(comp_name, desc, tags, specific_niche)
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
        "Manager Name": "N/A", "Title": "N/A", "Email": "N/A", "Phone": "N/A", 
        "Source": "Pending", "Notes": "", "Confidence": "Low"
    }
    
    found_person = None
    apollo_cache = None
    
    # 4. WEB SPIDER
    if domain:
        home_url = f"http://{domain}"
        queue = [home_url, f"http://{domain}/about", f"http://{domain}/firm", f"http://{domain}/studio", f"http://{domain}/people", f"http://{domain}/team"]
        visited = set()
        
        for url in queue[:5]:
            if url in visited: continue
            visited.add(url)
            content = firecrawl_scrape(url)
            if content:
                ai_data = extract_names_openai(content, comp_name)
                if ai_data and ai_data.get('name') != "None":
                    name = ai_data.get('name')
                    if " " in name and len(name) > 3:
                        found_person = {"first_name": name.split()[0], "last_name": " ".join(name.split()[1:]), "title": ai_data.get('title')}
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
        row['Manager Name'] = f"{found_person.get('first_name')} {found_person.get('last_name')}"
        row['Title'] = found_person.get('title')
        if "Web" in row['Source'] and domain:
            matches = bulk_enrich_names([found_person], domain)
            if matches and matches[0]:
                found_person = matches[0]
                row['Source'] += " -> Verified"
                row['Confidence'] = "High"
        elif "Apollo" in row['Source']:
            row['Confidence'] = "Medium"
        
        row['Email'] = found_person.get('email') or 'N/A'
        pnums = found_person.get('phone_numbers', [])
        row['Phone'] = pnums[0].get('sanitized_number') if pnums else 'N/A'
        if found_person.get('notes'): row['Notes'] = found_person.get('notes')
    
    return row

# --- UI LAYOUT ---
st.title("🚀 NCP Sourcing Engine (Turbo)")
st.markdown("Automated Deal Sourcing with Multi-Threaded AI.")

with st.form("sourcing_form"):
    col1, col2 = st.columns(2)
    broad_keyword = col1.text_input("1. Broad Apollo Category", value="Architecture")
    specific_niche = col2.text_input("2. Specific Niche (AI Filter)", value="Luxury Residential Architect")
    
    col3, col4 = st.columns(2)
    target_geo = col3.text_input("3. Geography", value="Birmingham, AL")
    mode = col4.selectbox("4. Strategy", ["A - Buy/Private (Strict)", "B - Sell/Any (Loose)"])
    
    submitted = st.form_submit_button("Start Sourcing 💎", type="primary")

if submitted:
    strat_code = "A" if "A -" in mode else "B"
    st.info(f"🔎 Searching for **{specific_niche}** in **{target_geo}**...")
    
    orgs = search_organizations(broad_keyword, target_geo)
    
    if not orgs:
        st.error("No companies found via Apollo. Try a broader category.")
    else:
        st.success(f"Found {len(orgs)} candidates. Running 5x Parallel Workers...")
        
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
                
                # Update UI
                progress = (i + 1) / len(orgs)
                progress_bar.progress(progress)
                status_text.caption(f"Processed {i+1}/{len(orgs)} companies...")

        status_text.write("✅ **Sourcing Complete!**")
        
        if final_data:
            df = pd.DataFrame(final_data)
            st.dataframe(df)
            csv = df.to_csv(index=False).encode('utf-8')
            filename = f"NCP_{broad_keyword}_{target_geo}.csv"
            st.download_button(label="Download Excel/CSV", data=csv, file_name=filename, mime="text/csv", type="primary")
        else:
            st.warning("No valid targets passed the filters.")