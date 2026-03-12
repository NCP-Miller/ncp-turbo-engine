import re
import json
import requests
from urllib.parse import urlparse, urljoin
from config import (FIRECRAWL_API_KEY, APOLLO_API_KEY, HTTP_USER_AGENT,
                    client, OPENAI_MODEL, _TITLE_SCORES, _CONTACT_PATHS)


# ---------------------------------------------------------------------------
# CONTACT FINDING — FIRECRAWL
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
    except:
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
        resp = client.chat.completions.create(
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
        resp = client.chat.completions.create(
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

    web_email = web_phone = None

    # Try https first, then http — many small operators have no SSL or expired certs
    def _scrape(url):
        content = firecrawl_scrape(url)
        if content and len(content) >= 100:
            return content
        # Raw http fallback for sites Firecrawl can't reach
        try:
            r = requests.get(
                url,
                headers={"User-Agent": HTTP_USER_AGENT},
                timeout=10, allow_redirects=True,
            )
            if r.status_code == 200 and len(r.text) > 200:
                return r.text[:40000]
        except:
            pass
        return None

    # Determine working base URL (https preferred, http fallback)
    base = f"https://{domain}"
    if not _scrape(base):
        base = f"http://{domain}"

    for path in _CONTACT_PATHS:
        content = _scrape(base + path)
        if not content: continue
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
    for url in queue[:8]:
        if url in visited: continue
        visited.add(url)
        content = _scrape(url)
        if not content: continue
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
    except:
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
        except:
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
    except:
        return []
