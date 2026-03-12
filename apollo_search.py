import json
import re
import requests
import concurrent.futures
from urllib.parse import quote_plus
from config import APOLLO_API_KEY, HTTP_USER_AGENT, client, OPENAI_MODEL
from contacts import firecrawl_scrape, clean_domain


# ---------------------------------------------------------------------------
# APOLLO — TWO-PASS ORGANIZATION SEARCH
# ---------------------------------------------------------------------------
def search_organizations(industries, location_input, keyword_tags=None, max_pages=10):
    """
    Two-pass search for maximum candidate coverage:

    Pass 1 — Industry sweep (NO keyword filter):
      Search each selected industry broadly so we don't miss companies
      that have the right industry tag but aren't tagged with our keywords.
      AI filter handles relevance.

    Pass 2 — Keyword-only sweep (NO industry filter):
      Search by keyword tags across ALL industries so we catch companies
      that Apollo has classified in an unexpected industry category.

    Both passes are deduplicated by Apollo org ID.
    """
    url     = "https://api.apollo.io/v1/organizations/search"
    headers = {"Content-Type": "application/json", "X-Api-Key": APOLLO_API_KEY}
    all_orgs, seen_ids = [], set()

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
            except:
                break

    # Pass 1: broad industry sweeps — no keyword restriction
    for industry in (industries or [None]):
        base = {"organization_locations": [location_input]}
        if industry:
            base["q_organization_industries"] = [industry]
        _fetch_pages(base)

    # Pass 2: keyword-only sweep — catches misclassified companies
    if keyword_tags:
        _fetch_pages({
            "organization_locations":        [location_input],
            "q_organization_keyword_tags":   keyword_tags,
        })

    return all_orgs


def web_discovery_pass(niche, geography, seen_domains, seen_names):
    """
    Pass 3: Scrape multiple search engines (Google, DuckDuckGo, Bing) with query
    variations to catch companies that Apollo doesn't have.
    Returns a list of org-like dicts compatible with process_single_company.
    """
    # Two query variations — full geography and short (state/region only)
    geo_short = geography.split(",")[0].strip()
    q1 = f"{niche} {geo_short}"
    q2 = f"{niche} providers {geo_short}"

    _UA = HTTP_USER_AGENT

    search_urls = [
        # Google — page 1 + Places tab (page 2 often blocked along with p1)
        f"https://www.google.com/search?q={quote_plus(q1)}&num=20",
        f"https://www.google.com/search?q={quote_plus(q1)}&tbm=lcl",
        # DuckDuckGo HTML — much more scraper-friendly than Google
        f"https://html.duckduckgo.com/html/?q={quote_plus(q1)}",
        f"https://html.duckduckgo.com/html/?q={quote_plus(q2)}",
        # Bing — independent index, catches different results
        f"https://www.bing.com/search?q={quote_plus(q1)}",
    ]

    def fetch_one(url):
        # Try Firecrawl first (JS rendering, handles some bot-protection)
        content = firecrawl_scrape(url)
        if content and len(content) >= 200:
            return content
        # Raw request fallback
        try:
            r = requests.get(url, headers={"User-Agent": _UA}, timeout=15)
            if r.status_code == 200 and len(r.text) > 500:
                return r.text[:30000]
        except:
            pass
        return ""

    # Fetch all five sources in parallel
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
        if "content" not in str(e).lower() and "filter" not in str(e).lower() and "400" not in str(e):
            return []
        try:
            resp = client.chat.completions.create(
                model=OPENAI_MODEL,
                messages=[{"role": "user", "content": extract_prompt}],
                timeout=30,
            )
            raw = resp.choices[0].message.content or ""
            match = re.search(r'\{.*\}', raw, re.DOTALL)
            if match:
                data = json.loads(match.group())
                companies = data.get("companies") or []
        except:
            return []

    # Deduplicate against Apollo results and build org-like dicts
    new_orgs = []
    for c in companies:
        if not isinstance(c, dict) or not c.get("name"):
            continue
        name_lower = c["name"].strip().lower()
        if name_lower in seen_names:
            continue
        domain = clean_domain(c.get("website"))
        if domain and domain in seen_domains:
            continue

        org = {
            "id":                       None,
            "name":                     c["name"].strip(),
            "website_url":              c.get("website"),
            "city":                     c.get("city"),
            "state":                    c.get("state"),
            "linkedin_url":             None,
            "estimated_num_employees":  None,
            "short_description":        c.get("snippet") or "",
            "headline":                 "",
            "keywords":                 [],
            "ownership_status":         None,
        }
        new_orgs.append(org)
        if domain:
            seen_domains.add(domain)
        seen_names.add(name_lower)

    return new_orgs
