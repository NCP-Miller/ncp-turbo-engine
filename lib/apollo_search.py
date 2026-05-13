"""Apollo organization search + multi-engine web discovery.

All functions accept API clients as parameters. No Streamlit, no globals.
"""

import json
import re
import requests
import concurrent.futures
from urllib.parse import quote_plus

from lib.constants import OPENAI_MODEL, DEFAULT_HTTP_USER_AGENT


# ---------------------------------------------------------------------------
# APOLLO — TWO-PASS ORGANIZATION SEARCH
# ---------------------------------------------------------------------------
def search_organizations(
    apollo_api_key,
    industries,
    location_input,
    keyword_tags=None,
    max_pages=10,
):
    """Two-pass Apollo company search for maximum candidate coverage.

    Pass 1 — Industry sweep (no keyword filter), per industry.
    Pass 2 — Keyword-only sweep (no industry filter).
    Both passes deduplicated by Apollo org ID.

    location_input can be a single string ("Virginia, United States")
    or a list of strings for multi-state searches.
    """
    url = "https://api.apollo.io/v1/organizations/search"
    headers = {"Content-Type": "application/json", "X-Api-Key": apollo_api_key}
    all_orgs, seen_ids = [], set()

    locations = location_input if isinstance(location_input, list) else [location_input]

    def _fetch_pages(base_payload):
        for page in range(1, max_pages + 1):
            payload = {**base_payload, "page": page, "per_page": 100}
            try:
                r = requests.post(url, headers=headers, json=payload, timeout=15)
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

    # Pass 1: broad industry sweeps
    for industry in (industries or [None]):
        base = {"organization_locations": locations}
        if industry:
            base["q_organization_industries"] = [industry]
        _fetch_pages(base)

    # Pass 2: keyword-only sweep
    if keyword_tags:
        _fetch_pages({
            "organization_locations": locations,
            "q_organization_keyword_tags": keyword_tags,
        })

    return all_orgs


# ---------------------------------------------------------------------------
# WEB DISCOVERY (multi-engine + AI extraction)
# ---------------------------------------------------------------------------
def web_discovery_pass(
    client,
    firecrawl_scrape_fn,
    clean_domain_fn,
    niche,
    geography,
    seen_domains,
    seen_names,
    user_agent=None,
):
    """Pass 3: Scrape Google/DuckDuckGo/Bing for niche operators.

    Args:
        client: Initialized OpenAI client.
        firecrawl_scrape_fn: Callable that scrapes a URL and returns text.
            Pass lib.contacts.firecrawl_scrape (curried with the firecrawl key).
        clean_domain_fn: Callable that normalizes a website URL to a domain.
            Pass lib.contacts.clean_domain.
        niche: Target niche string.
        geography: Geography string (e.g., "Birmingham, AL").
        seen_domains: Set of already-seen domains (mutated).
        seen_names: Set of already-seen lowercase names (mutated).
        user_agent: Optional User-Agent string for requests.

    Returns:
        list of org-like dicts compatible with the Apollo schema.
    """
    ua = user_agent or DEFAULT_HTTP_USER_AGENT
    geo_short = geography.split(",")[0].strip()
    q1 = f"{niche} {geo_short}"
    q2 = f"{niche} providers {geo_short}"

    search_urls = [
        f"https://www.google.com/search?q={quote_plus(q1)}&num=20",
        f"https://www.google.com/search?q={quote_plus(q1)}&tbm=lcl",
        f"https://html.duckduckgo.com/html/?q={quote_plus(q1)}",
        f"https://html.duckduckgo.com/html/?q={quote_plus(q2)}",
        f"https://www.bing.com/search?q={quote_plus(q1)}",
    ]

    def fetch_one(url):
        content = firecrawl_scrape_fn(url)
        if content and len(content) >= 200:
            return content
        try:
            r = requests.get(url, headers={"User-Agent": ua}, timeout=15)
            if r.status_code == 200 and len(r.text) > 500:
                return r.text[:30000]
        except Exception:
            pass
        return ""

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
        results = list(ex.map(fetch_one, search_urls))

    combined = "\n\n---NEW SOURCE---\n\n".join(r for r in results if r)
    if not combined or len(combined) < 200:
        return []

    extract_prompt = f"""From these search results (Google, DuckDuckGo, and Bing),
extract every company that appears to be an actual operator or provider in "{niche}"
located in or near "{geography}".

Local/Places listings may show a business name, address, phone number, and star rating
— include those even if no website URL is present.

Return JSON only:
{{"companies": [
  {{"name": "Company Name", "website": "https://example.com or blank",
    "city": "City", "state": "ST", "snippet": "What they do"}}
]}}

Rules:
- Only include actual operating companies in the niche (not vendors, consultants, or tech firms)
- Do NOT include directories, news articles, government agencies, Wikipedia, or ad listings
- Use the company's own website URL when visible; leave blank if not found
- Deduplicate — if the same company appears multiple times, include it only once
- Return {{"companies": []}} if none found

Search content:
{combined[:25000]}"""

    companies = []
    try:
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": extract_prompt}],
            response_format={"type": "json_object"},
            timeout=30,
        )
        data = json.loads(resp.choices[0].message.content)
        companies = data.get("companies") or []
    except Exception as e:
        msg = str(e).lower()
        if "content" not in msg and "filter" not in msg and "400" not in msg:
            return []
        try:
            resp = client.chat.completions.create(
                model=OPENAI_MODEL,
                messages=[{"role": "user", "content": extract_prompt}],
                timeout=30,
            )
            raw = resp.choices[0].message.content or ""
            match = re.search(r"\{.*\}", raw, re.DOTALL)
            if match:
                data = json.loads(match.group())
                companies = data.get("companies") or []
        except Exception:
            return []

    new_orgs = []
    for c in companies:
        if not isinstance(c, dict) or not c.get("name"):
            continue
        name_lower = c["name"].strip().lower()
        if name_lower in seen_names:
            continue
        domain = clean_domain_fn(c.get("website"))
        if domain and domain in seen_domains:
            continue

        org = {
            "id": None,
            "name": c["name"].strip(),
            "website_url": c.get("website"),
            "city": c.get("city"),
            "state": c.get("state"),
            "linkedin_url": None,
            "estimated_num_employees": None,
            "short_description": c.get("snippet") or "",
            "headline": "",
            "keywords": [],
            "ownership_status": None,
        }
        new_orgs.append(org)
        if domain:
            seen_domains.add(domain)
        seen_names.add(name_lower)

    return new_orgs