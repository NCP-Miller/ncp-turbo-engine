"""Contact discovery: Firecrawl scraping, web spidering, and Apollo people search.

All functions accept API clients/keys as parameters. No Streamlit, no globals.
"""

import re
import json
import requests
from urllib.parse import urlparse, urljoin

from lib.constants import OPENAI_MODEL, _TITLE_SCORES, _CONTACT_PATHS, DEFAULT_HTTP_USER_AGENT


# ---------------------------------------------------------------------------
# DOMAIN / NAME UTILITIES
# ---------------------------------------------------------------------------
def clean_domain(url):
    """Normalize a website URL to a bare domain (no scheme, no www)."""
    if not url or not isinstance(url, str):
        return None
    try:
        if not url.startswith("http"):
            url = "http://" + url
        d = urlparse(url).netloc
        return d[4:] if d.startswith("www.") else d
    except Exception:
        return None


def clean_company_name_for_search(name):
    """Strip common suffixes so company-name searches match Apollo records."""
    if not name:
        return ""
    c = name.replace(",", "").replace(".", "")
    for s in [
        " inc", " llc", " group", " ltd", " corp", " p.c.", " pc",
        " architects", " architecture",
    ]:
        if c.lower().endswith(s):
            c = c[:-len(s)]
    return c.strip()


def _title_score(title):
    """Score a job title by seniority using the constants table."""
    t = (title or "").lower()
    for phrase, score in _TITLE_SCORES.items():
        if phrase in t:
            return score
    return 0


# ---------------------------------------------------------------------------
# FIRECRAWL SCRAPER
# ---------------------------------------------------------------------------
def firecrawl_scrape(firecrawl_api_key, url):
    """Scrape a URL via Firecrawl and return markdown content (up to 50KB)."""
    try:
        r = requests.post(
            "https://api.firecrawl.dev/v1/scrape",
            headers={
                "Authorization": f"Bearer {firecrawl_api_key}",
                "Content-Type": "application/json",
            },
            json={"url": url, "formats": ["markdown"]},
            timeout=20,
        )
        if r.status_code == 200:
            data = r.json()
            md = (data.get("data") or {}).get("markdown") or data.get("markdown")
            return md[:50000] if md else None
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# LINK EXTRACTION
# ---------------------------------------------------------------------------
def extract_relevant_links(md, base_url):
    """From markdown, return up to 4 high-value links (team/leadership/about pages)."""
    if not md:
        return []
    high = [
        "leadership", "executive", "our team", "care team", "management",
        "principals", "partners", "providers", "medical staff", "administration",
    ]
    med = ["about", "who we are", "meet", "staff", "firm", "studio", "people", "team", "contact"]
    skip = ["linkedin", "facebook", "twitter", "pdf", "jpg", "login", "mailto"]
    seen, out = set(), []
    for text, link in re.findall(r"\[([^\]]+)\]\(([^)]+)\)", md):
        t = text.lower()
        score = 3 if any(x in t for x in high) else (1 if any(x in t for x in med) else 0)
        if not score:
            continue
        full = (
            urljoin(base_url, link) if link.startswith("/")
            else (link if link.startswith("http") else None)
        )
        if not full or any(s in full for s in skip) or full in seen:
            continue
        seen.add(full)
        out.append((score, full))
    out.sort(key=lambda x: x[0], reverse=True)
    return [u for _, u in out[:4]]


# ---------------------------------------------------------------------------
# AI EXTRACTION OF PRIMARY LEADER
# ---------------------------------------------------------------------------
def extract_names_openai(client, text, company_name):
    """Use GPT-4o to extract the primary leader's name/title/email/phone from page text."""
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
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            timeout=20,
        )
        return _parse_contact(resp.choices[0].message.content)
    except Exception as e:
        msg = str(e).lower()
        if "content" not in msg and "filter" not in msg and "400" not in msg:
            return None

    # Attempt 2 — retry without response_format
    try:
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            timeout=20,
        )
        raw = resp.choices[0].message.content or ""
        match = re.search(r"\{.*\}", raw, re.DOTALL)
        if match:
            return _parse_contact(match.group())
    except Exception:
        pass

    return None


# ---------------------------------------------------------------------------
# WEB SPIDER FOR LEADERSHIP CONTACT
# ---------------------------------------------------------------------------
def spider_for_contact(client, firecrawl_api_key, company_name, domain, user_agent=None):
    """Search a company's site (known leadership paths + light spider) for the top leader.

    Returns: (person_dict_or_None, source_label_or_None, web_email_or_None, web_phone_or_None)
    """
    if not domain:
        return None, None, None, None

    ua = user_agent or DEFAULT_HTTP_USER_AGENT
    web_email = web_phone = None

    def _scrape(url):
        content = firecrawl_scrape(firecrawl_api_key, url)
        if content and len(content) >= 100:
            return content
        try:
            r = requests.get(
                url,
                headers={"User-Agent": ua},
                timeout=10, allow_redirects=True,
            )
            if r.status_code == 200 and len(r.text) > 200:
                return r.text[:40000]
        except Exception:
            pass
        return None

    base = f"https://{domain}"
    if not _scrape(base):
        base = f"http://{domain}"

    # Pass 1: known leadership paths
    for path in _CONTACT_PATHS:
        content = _scrape(base + path)
        if not content:
            continue
        ai = extract_names_openai(client, content, company_name)
        if not ai:
            continue
        if ai.get("email"):
            web_email = ai["email"]
        if ai.get("phone"):
            web_phone = ai["phone"]
        n = ai.get("name")
        if n and " " in n and len(n) > 3:
            person = {
                "first_name": n.split()[0],
                "last_name": " ".join(n.split()[1:]),
                "title": ai.get("title"),
                "email": web_email,
                "phone_numbers": [{"sanitized_number": web_phone}] if web_phone else [],
            }
            return person, f"Web Path ({path})", web_email, web_phone

    # Pass 2: light spider from base URL following relevant links
    visited, queue = set(), [base]
    for url in queue[:8]:
        if url in visited:
            continue
        visited.add(url)
        content = _scrape(url)
        if not content:
            continue
        ai = extract_names_openai(client, content, company_name)
        if ai:
            if ai.get("email"):
                web_email = ai["email"]
            if ai.get("phone"):
                web_phone = ai["phone"]
            n = ai.get("name")
            if n and " " in n and len(n) > 3:
                person = {
                    "first_name": n.split()[0],
                    "last_name": " ".join(n.split()[1:]),
                    "title": ai.get("title"),
                    "email": web_email,
                    "phone_numbers": [{"sanitized_number": web_phone}] if web_phone else [],
                }
                return person, "Web Spider", web_email, web_phone
        for lnk in extract_relevant_links(content, url):
            if lnk not in visited:
                queue.insert(1, lnk)

    return None, None, web_email, web_phone


# ---------------------------------------------------------------------------
# APOLLO PEOPLE SEARCH
# ---------------------------------------------------------------------------
def get_people_apollo_robust(apollo_api_key, company_name, domain, org_id=None):
    """Find leadership via Apollo's mixed_people search with multiple fallback queries."""
    url = "https://api.apollo.io/v1/mixed_people/search"
    headers = {"Content-Type": "application/json", "X-Api-Key": apollo_api_key}
    name = clean_company_name_for_search(company_name)

    top_seniority = ["owner", "founder", "c_suite", "president"]
    wide_seniority = ["owner", "founder", "c_suite", "president", "vp", "partner", "manager"]

    attempts = []
    if org_id:
        attempts += [
            {"organization_ids": [org_id], "person_seniority": top_seniority, "per_page": 10},
            {"organization_ids": [org_id], "person_seniority": wide_seniority, "per_page": 25},
            {"organization_ids": [org_id], "per_page": 25},
        ]
    if domain:
        domains = list({
            domain,
            f"www.{domain}",
            domain[4:] if domain.startswith("www.") else domain,
        })
        for d in domains:
            attempts += [
                {"q_organization_domains": [d], "person_seniority": top_seniority, "per_page": 10},
                {"q_organization_domains": [d], "person_seniority": wide_seniority, "per_page": 25},
            ]
    if name:
        attempts += [
            {"q_organization_names": [name], "person_seniority": top_seniority, "per_page": 10},
            {"q_organization_names": [name], "person_seniority": wide_seniority, "per_page": 15},
            {"q_organization_names": [name], "per_page": 15},
        ]

    all_people, seen_ids = [], set()
    for payload in attempts:
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=10)
            if r.status_code == 200:
                people = r.json().get("people", [])
                new = [
                    p for p in people
                    if p.get("id") and p["id"] not in seen_ids
                    and p.get("first_name") and p.get("last_name")
                ]
                for p in new:
                    seen_ids.add(p["id"])
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
    """Pick the highest-scoring Apollo person (preferring those with email)."""
    if not people:
        return None, "None"
    valid = [
        p for p in people
        if p.get("first_name") and p.get("last_name")
        and str(p.get("last_name", "")).strip().lower() not in ("none", "n/a", "")
    ]
    if not valid:
        return None, "None"
    scored = [(p, _title_score(p.get("title"))) for p in valid]
    scored.sort(key=lambda x: (x[1], bool(x[0].get("email"))), reverse=True)
    best, score = scored[0]
    label = "Apollo (Top)" if score >= 50 else "Apollo (Best Available)"
    if not best.get("email"):
        label += " [No Email]"
    return best, label


def repair_single_name(first_name, people_list):
    """If we only have a first name from web, find the matching full record in Apollo."""
    if not first_name or not people_list:
        return None
    target = first_name.split()[0].lower()
    for p in people_list:
        if target in (p.get("first_name") or "").lower():
            return p
    return None


def bulk_enrich_names(apollo_api_key, people_list, domain):
    """Run a list of people through Apollo's bulk_match for email enrichment."""
    if not people_list or not domain:
        return []
    try:
        r = requests.post(
            "https://api.apollo.io/v1/people/bulk_match",
            headers={"Content-Type": "application/json", "X-Api-Key": apollo_api_key},
            json={
                "details": [
                    {
                        "first_name": p.get("first_name"),
                        "last_name": p.get("last_name"),
                        "domain": domain,
                    }
                    for p in people_list
                ]
            },
            timeout=15,
        )
        return r.json().get("matches", [])
    except Exception:
        return []