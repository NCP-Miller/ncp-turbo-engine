"""Company quality filters and AI relevance scoring.

Includes:
  - is_buyable_structure: structural ownership/size filter
  - is_obvious_mismatch: name/description block lists
  - check_relevance_gpt4o: AI-based relevance to target niche
  - check_pe_vc_web: deep web-based PE/VC ownership check (Strategy A)
  - check_news_for_pe_vc: quick PE/VC signal scan in a news headline

All API clients/keys are passed in. No Streamlit, no globals.
"""

import json
import re

from lib.constants import OPENAI_MODEL
from lib.portfolio_cache import load_pe_firms, is_pe_backed_via_cache


# ---------------------------------------------------------------------------
# HARD SIZE CAPS
# ---------------------------------------------------------------------------
MAX_EMPLOYEES_HARD_CAP = 500
MIN_EMPLOYEES_HARD_CAP = 15


# ---------------------------------------------------------------------------
# PE/VC SIGNAL VOCABULARY
# ---------------------------------------------------------------------------
_PE_VC_SIGNALS = [
    "private equity", "venture capital", "portfolio company", "backed by",
    "pe-backed", "vc-backed", "pe backed", "vc backed", "growth equity",
    "buyout", "recapitalization", "capital partners", "equity partners",
    "investment partners", "apax", "bain capital", "carlyle", "kkr",
    "blackstone", "thoma bravo", "insight partners", "warburg pincus",
    "silver lake", "vista equity", "hellman & friedman", "permira",
    "advent international", "general atlantic", "summit partners",
    "series a", "series b", "series c", "series d", "series e",
    "funding round", "raised $", "investment from",
]

_KNOWN_PE_PORTFOLIOS = {
    "apax":                 "https://www.apax.com/portfolio/",
    "bain capital":         "https://www.baincapital.com/portfolio",
    "carlyle":              "https://www.carlyle.com/portfolio",
    "kkr":                  "https://www.kkr.com/businesses/private-equity/portfolio",
    "blackstone":           "https://www.blackstone.com/portfolio/",
    "thoma bravo":          "https://www.thomabravo.com/companies",
    "insight partners":     "https://www.insightpartners.com/portfolio/",
    "warburg pincus":       "https://www.warburgpincus.com/investments/",
    "silver lake":          "https://www.silverlake.com/portfolio/",
    "vista equity":         "https://www.vistaequitypartners.com/companies/",
    "hellman & friedman":   "https://www.hfriedman.com/portfolio",
    "permira":              "https://www.permira.com/portfolio",
    "advent international": "https://www.adventinternational.com/portfolio/",
    "general atlantic":     "https://www.generalatlantic.com/portfolio/",
    "summit partners":      "https://www.summitpartners.com/companies",
    "new capital partners": "https://www.newcapitalpartners.com/portfolio",
    "bv investment":        "https://www.bvinvestmentpartners.com/portfolio",
}


# ---------------------------------------------------------------------------
# STRUCTURAL FILTER — is this org a buyable company?
# ---------------------------------------------------------------------------
def is_buyable_structure(org, mode):
    """Filter out non-acquirable entity types based on Apollo metadata."""
    emp = org.get("estimated_num_employees", 0) or 0

    # Hard size caps — applied before any AI/web calls
    if emp > MAX_EMPLOYEES_HARD_CAP:
        return False, f"Above lower-middle-market size cap ({MAX_EMPLOYEES_HARD_CAP} employees)"
    if emp >= 1 and emp < MIN_EMPLOYEES_HARD_CAP:
        return False, f"Below minimum size threshold ({MIN_EMPLOYEES_HARD_CAP} employees)"
    status = str(org.get("ownership_status") or "").strip().lower()
    tags = [t.lower() for t in (org.get("keywords") or [])]
    name = (org.get("name") or "").lower()
    desc = (org.get("short_description") or "").lower()
    industry = (org.get("industry") or "").lower()

    _govt_name_signals = [
        "federal commission", "federal agency",
        "department of", "bureau of", "office of the",
    ]
    for sig in _govt_name_signals:
        if sig in name:
            return False, "Government Entity"
    if status in ("government",):
        return False, "Government Entity"
    if status in ("non-profit", "nonprofit"):
        return False, f"Non-Acquirable ({status})"
    _np_signals = ["non-profit organization", "nonprofit organization", "501(c)"]
    for sig in _np_signals:
        if sig in desc:
            return False, "Non-Profit"
    if "non-profit organization management" in industry:
        return False, "Non-Profit"
    if "government administration" in industry and emp > 50:
        return False, "Government Entity"

    if mode == "A":
        if status == "public":
            return False, "Publicly Traded"
        if status == "subsidiary":
            return False, "Subsidiary"
        if "private equity" in tags or "venture capital" in tags:
            return False, "PE/VC Backed (tags)"
        if emp > 7500:
            return False, f"Too Large ({emp})"

        # Deeper scan of Apollo fields for PE/VC signals
        tag_str = " ".join(tags)
        combined = f"{desc} {tag_str} {status}"
        for signal in _PE_VC_SIGNALS:
            if signal in combined:
                return False, f"PE/VC Backed ('{signal}' in Apollo data)"
    else:
        if "public" in status:
            return False, "Publicly Traded"
        if emp > 10000:
            return False, f"Too Large ({emp})"

    return True, "OK"


# ---------------------------------------------------------------------------
# NAME / DESCRIPTION BLOCK LISTS
# ---------------------------------------------------------------------------
_UNIVERSAL_BLOCKS = [
    "university", "college", "food service", "catering",
    "staffing solutions", "temp agency",
    " transportation", "non-emergency transport", "medical transport",
    "nemt ", " logistics",
    "eurest", "aramark", "sodexo", "compass group", "canteen",
]
_MODE_A_BLOCKS = [
    " hospital", "medical center", "health system",
    "law firm", " pllc", " llp",
    " forum", " blog", " news", " magazine", " journal", " media",
    " alliance", " coalition", " association", " society", " council",
    " consortium", " observatory", " institute", " foundation",
    "center of excellence",
    "federal ",
    "consulting group", "advisory group", " billing services",
    "software inc", "software llc",
    "healthtech",
]
_DESC_BLOCKS = [
    "non-profit", "nonprofit", "government agency",
    "trade association", "professional organization",
    "advocacy organization", "blog that", "media outlet",
    "news outlet", "law firm", "staffing agency",
    "recruitment and staffing", "recruiting agency",
]


def is_obvious_mismatch(org, target_niche, mode):
    """Block obvious mismatches based on the company name and description."""
    name = (org.get("name") or "").lower()
    desc = (org.get("short_description") or "").lower()
    for f in _UNIVERSAL_BLOCKS:
        if f in name:
            return True, f"Block: '{f}'"
    if mode == "B":
        return False, "Pass"
    extras = list(_MODE_A_BLOCKS)
    if "architect" in target_niche.lower():
        extras += ["realty", "real estate", "lumber", "golf course"]
    for f in extras:
        if f in name:
            return True, f"Mode-A block: '{f}'"
    for f in _DESC_BLOCKS:
        if f in desc:
            return True, f"Desc block: '{f}'"
    return False, "Pass"


# ---------------------------------------------------------------------------
# AI RELEVANCE CHECK
# ---------------------------------------------------------------------------
def check_relevance_gpt4o(client, company_name, description, keywords, target_niche, mode):
    """Use GPT-4o to score whether a company is relevant to the target niche."""
    if mode == "A":
        prompt = f"""You are an acquisition filter for a private equity investor.
Target niche: "{target_niche}"

Company: "{company_name}"
Description: "{description}"
Keywords: {keywords}

PASS only if the company's PRIMARY business is directly operating in or providing services
within the exact niche described above. The company must be a realistic private acquisition
target — a small-to-mid-size privately held operator or service provider.

FAIL if ANY of these apply:
- Government agency, department, commission, or publicly funded body
- Non-profit, trade association, professional society, advocacy org, or coalition
- Hospital, health system, or large medical institution (unless that IS the target niche)
- Law firm, legal practice, or PLLC (unless that IS the target niche)
- Blog, newsletter, news outlet, media company, podcast, or publishing platform
- Online forum, community, Discord server, or educational resource site
- Research institute, think tank, or academic institution
- The company merely WRITES ABOUT, REPORTS ON, or ADVOCATES for the niche rather than operating in it
- Staffing, recruiting, or temp agency
- Large national chain or enterprise not suitable for PE acquisition
- Completely unrelated industry — the company must actually DO what the niche describes,
  not just have a tangential keyword overlap
- "PACE" in the niche means Program for All-Inclusive Care for the Elderly — NOT fitness/pace

Be STRICT. A cybersecurity company is not a match for "managed IT services" and vice versa.
A company that merely mentions a keyword in its description is not a match if its primary
business is something else.
Return JSON only: {{"match": true/false, "reason": "one sentence"}}"""
    else:
        prompt = f"""You are identifying realistic sales prospects and direct competitors for companies in: "{target_niche}"

Company: "{company_name}"
Description: "{description}"
Keywords: {keywords}

PASS if the company fits ONE of these:
1. Direct competitor — operates in the same niche or a closely adjacent one (any size).
2. Realistic sales prospect — a similarly-scaled operator in the same sector who could refer
   clients to, partner with, or purchase services from companies in this niche.

FAIL if any of these apply:
- Government agency, department, commission, or publicly funded body
- Non-profit, trade association, professional society, advocacy org, or coalition
- Blog, newsletter, news outlet, media company, or publishing platform
- Research institute, think tank, or academic institution
- The company merely writes about or advocates for the niche rather than operating in it
- Staffing, recruiting, or temp agency
- Large institutional network, national chain, or conglomerate with 10,000+ employees, UNLESS
  it is a direct competitor operating in the exact same niche as the target
- Completely unrelated industry (manufacturing, unrelated finance, retail, food service, etc.)

Return JSON only: {{"match": true/false, "reason": "one sentence"}}"""

    def _parse_relevance(content):
        data = json.loads(content)
        return data.get("match"), data.get("reason")

    try:
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            timeout=20,
        )
        return _parse_relevance(resp.choices[0].message.content)
    except Exception as e:
        msg = str(e).lower()
        if "content" not in msg and "filter" not in msg and "400" not in msg:
            return True, "AI Error"

    try:
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            timeout=20,
        )
        raw = resp.choices[0].message.content or ""
        match = re.search(r"\{.*\}", raw, re.DOTALL)
        if match:
            return _parse_relevance(match.group())
    except Exception:
        pass

    return True, "AI Error"


# ---------------------------------------------------------------------------
# WEB-BASED PE/VC OWNERSHIP CHECK (Strategy A)
# ---------------------------------------------------------------------------
def check_pe_vc_web(client, firecrawl_scrape_fn, company_name, domain):
    """Web check for PE/VC ownership.

    Scrapes the company's about/investor pages, Crunchbase, and matched
    PE-firm portfolio pages, then asks GPT to make a final call.

    Args:
        client: OpenAI client.
        firecrawl_scrape_fn: Callable(url) -> str. Pass a curried firecrawl_scrape.
        company_name, domain: target company.

    Returns:
        (is_pe_vc: bool, reason: str). Conservative on failure.
    """
    snippets = []

    # 1. Company's own about/investor pages
    if domain:
        for path in ["/about", "/about-us", "/investors", "/company"]:
            content = firecrawl_scrape_fn(f"https://{domain}{path}")
            if content and len(content) >= 100:
                snippets.append(content[:8000])

    # 2. Crunchbase profile
    cb_slug = company_name.lower().replace(" ", "-").replace(",", "").replace(".", "")
    for slug in [cb_slug, cb_slug.split("-")[0]]:
        content = firecrawl_scrape_fn(f"https://www.crunchbase.com/organization/{slug}")
        if content and len(content) >= 200:
            snippets.append(f"CRUNCHBASE PROFILE:\n{content[:10000]}")
            break

    # 3. Cross-check with PE firm portfolio pages if their name appears
    snippet_text = " ".join(snippets).lower()
    for firm_name, portfolio_url in _KNOWN_PE_PORTFOLIOS.items():
        if firm_name in snippet_text:
            content = firecrawl_scrape_fn(portfolio_url)
            if content and len(content) >= 100:
                snippets.append(f"PE FIRM PORTFOLIO ({firm_name}):\n{content[:8000]}")

    if not snippets:
        return False, "No web data"

    combined = "\n---\n".join(snippets)
    prompt = f"""Determine whether "{company_name}" is owned by or has received significant
investment from a private equity firm or venture capital firm.

Look for evidence such as:
- "backed by [PE/VC firm name]"
- "portfolio company of [firm]"
- "acquired by [firm]"
- Company name appearing on a PE/VC firm's portfolio page
- Crunchbase listing showing PE/VC investors or funding rounds
- Series A/B/C/D funding rounds
- Recapitalization or leveraged buyout
- Any named PE or VC firm as an investor or owner

IMPORTANT: If "{company_name}" appears on any PE/VC firm's portfolio page, that is
definitive proof of PE/VC ownership — return true.

Web content:
{combined[:25000]}

Return JSON only:
{{"pe_vc_owned": true/false, "evidence": "one sentence explaining why, or 'No evidence found'"}}"""

    try:
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            timeout=20,
        )
        data = json.loads(resp.choices[0].message.content)
        return data.get("pe_vc_owned", False), data.get("evidence", "")
    except Exception:
        pass

    try:
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            timeout=20,
        )
        raw = resp.choices[0].message.content or ""
        match = re.search(r"\{.*\}", raw, re.DOTALL)
        if match:
            data = json.loads(match.group())
            return data.get("pe_vc_owned", False), data.get("evidence", "")
    except Exception:
        pass

    return False, "AI check failed"


# ---------------------------------------------------------------------------
# NEWS HEADLINE PE/VC SCAN
# ---------------------------------------------------------------------------
def check_news_for_pe_vc(news_title):
    """Quick keyword scan of a news headline for PE/VC ownership signals."""
    if not news_title:
        return False
    t = news_title.lower()
    signals = [
        "private equity", "venture capital", "pe-backed", "vc-backed",
        "acquired by", "acquisition", "buyout", "recapitalization",
        "series a", "series b", "series c", "series d",
        "funding round", "raises $", "raised $", "secures $",
        "investment from", "growth equity", "portfolio company",
    ]
    return any(s in t for s in signals)


# ---------------------------------------------------------------------------
# INTEGRATED PE DETECTION — cache-first, then news fallback
# ---------------------------------------------------------------------------
def check_pe_backed(client, candidate_name, news_snippets=None):
    """Check if a company is PE-backed. Cache-first, then news-enhanced fallback.

    Args:
        client: OpenAI client.
        candidate_name: Company name to check.
        news_snippets: Optional list of news headline strings to scan.

    Returns:
        dict: {"is_pe_backed": bool, "evidence": str}
    """
    # 1. Fast path — check portfolio cache
    cache_result = is_pe_backed_via_cache(client, candidate_name)
    if cache_result.get("is_pe_backed"):
        firm = cache_result.get("matched_firm", "unknown firm")
        method = cache_result.get("method", "cache")
        return {
            "is_pe_backed": True,
            "evidence": f"In portfolio of {firm} (cache, {method} match)",
        }

    # 2. Fallback — news-based check with PE firm name cross-reference
    if not news_snippets:
        return {"is_pe_backed": False, "evidence": "No PE signal found"}

    pe_firms = load_pe_firms()
    pe_firms_lower = [f.lower() for f in pe_firms]

    # Check if any news snippet mentions both candidate and a known PE firm
    matching_snippets = []
    for snippet in news_snippets:
        snippet_lower = snippet.lower()
        candidate_mentioned = candidate_name.lower() in snippet_lower
        firm_mentioned = any(f in snippet_lower for f in pe_firms_lower)
        if candidate_mentioned and firm_mentioned:
            matching_snippets.append(snippet)
        elif any(signal in snippet_lower for signal in [
            "acquired by", "investment from", "partnership with",
            "portfolio company of", "backed by", "majority investor",
        ]):
            matching_snippets.append(snippet)

    if not matching_snippets:
        return {"is_pe_backed": False, "evidence": "No PE signal found"}

    # Ask GPT for final classification
    combined = "\n".join(matching_snippets[:10])
    try:
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": (
                f"Does this evidence indicate that '{candidate_name}' is currently owned/backed "
                f"by a private equity firm? Return JSON: {{'is_pe_backed': true/false, "
                f"'pe_firm': 'name or null', 'rationale': 'brief'}}.\n\n"
                f"News evidence:\n{combined[:5000]}"
            )}],
            response_format={"type": "json_object"},
            timeout=15,
        )
        data = json.loads(resp.choices[0].message.content or "{}")
        if data.get("is_pe_backed"):
            pe_firm = data.get("pe_firm", "unknown")
            rationale = data.get("rationale", "")
            return {
                "is_pe_backed": True,
                "evidence": f"PE-backed by {pe_firm} ({rationale})",
            }
    except Exception:
        pass

    return {"is_pe_backed": False, "evidence": "No PE signal confirmed"}