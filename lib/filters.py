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

from lib import cache
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
    "updata partners", "salesforce ventures", "accel", "andreessen",
    "sequoia", "bessemer", "battery ventures", "jmi equity",
    "spectrum equity", "k1 investment", "long ridge equity",
    "norwest venture", "tiger global", "coatue", "iconiq",
    "susquehanna growth", "strategic investor",
    "y combinator", "yc-backed", "yc backed", "techstars",
    "500 startups", "seed round", "pre-seed", "angel round",
    "greylock", "benchmark", "founders fund", "lightspeed",
    "khosla ventures", "index ventures", "ribbit capital",
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
    "updata partners":      "https://www.updata.com/portfolio/",
    "jmi equity":           "https://jmiequity.com/portfolio/",
    "spectrum equity":      "https://www.spectrumequity.com/portfolio",
    "k1 investment":        "https://k1.com/portfolio/",
    "long ridge equity":    "https://www.longridgeep.com/portfolio",
    "battery ventures":     "https://www.battery.com/our-portfolio/",
    "accel":                "https://www.accel.com/portfolio",
    "norwest venture":      "https://www.nvp.com/portfolio/",
    "y combinator":         "https://www.ycombinator.com/companies",
    "a16z":                 "https://a16z.com/portfolio/",
    "andreessen horowitz":  "https://a16z.com/portfolio/",
    "bessemer":             "https://www.bvp.com/portfolio",
    "greylock":             "https://greylock.com/portfolio/",
    "index ventures":       "https://www.indexventures.com/companies/",
    "benchmark":            "https://www.benchmark.com/portfolio",
    "founders fund":        "https://foundersfund.com/portfolio/",
    "lightspeed":           "https://lsvp.com/portfolio/",
    "khosla ventures":      "https://www.khoslaventures.com/portfolio",
    "nea":                  "https://www.nea.com/portfolio",
    "ribbit capital":       "https://ribbitcap.com/companies/",
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

    # Subsidiary detection — "X, a Y company" / "a division of" / "a subsidiary of"
    _sub_patterns = [
        r",\s+a\s+\w[\w\s&.'-]+\s+company",
        r"a\s+division\s+of\s+",
        r"a\s+subsidiary\s+of\s+",
        r"an?\s+\w[\w\s&.'-]+\s+brand\b",
        r"part\s+of\s+\w[\w\s&.'-]+\s+group",
        r"wholly[- ]owned",
        r"owned\s+by\s+\w",
        r"product\s+(?:line\s+)?(?:of|by)\s+\w",
        r"powered\s+by\s+\w[\w\s&.'-]+",
    ]
    _combined_text = f"{name} {desc}"
    for pat in _sub_patterns:
        if re.search(pat, _combined_text):
            return False, f"Subsidiary ('{re.search(pat, _combined_text).group().strip()}')"

    # Product-line detection: "Quadient AR Automation" on quadient.com
    _org_domain = (org.get("website_url") or "").lower()
    if _org_domain:
        _dom_clean = re.sub(r"https?://(?:www\.)?", "", _org_domain).split("/")[0]
        _dom_base = _dom_clean.split(".")[0] if _dom_clean else ""
        _name_words = name.split()
        if (_dom_base and len(_name_words) >= 3
                and _dom_base == _name_words[0].lower().replace(",", "")):
            return False, f"Product line of {_name_words[0]} (website: {_dom_clean})"

    # Apollo public trading fields (catches companies even when ownership_status is wrong)
    ticker = org.get("publicly_traded_symbol") or org.get("ticker") or ""
    exchange = org.get("publicly_traded_exchange") or ""
    mkt_cap = org.get("market_cap") or 0
    if ticker or exchange:
        return False, f"Publicly Traded ({ticker or exchange})"
    try:
        if mkt_cap and float(mkt_cap) > 0:
            return False, "Publicly Traded (has market cap)"
    except (ValueError, TypeError):
        pass

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

        # Apollo funding data — institutional investors are a deal killer
        latest_stage = str(org.get("latest_funding_stage") or "").lower().replace("_", " ")
        _institutional_stages = [
            "series a", "series b", "series c", "series d", "series e",
            "private equity", "debt financing", "ipo", "post ipo",
            "grant", "secondary market", "seed", "pre seed",
        ]
        if any(s in latest_stage for s in _institutional_stages):
            return False, f"Institutional Funding ({latest_stage})"

        total_funding = org.get("total_funding") or 0
        try:
            val = float(total_funding) if isinstance(total_funding, str) else total_funding
            if isinstance(val, (int, float)) and val > 5_000_000:
                return False, f"Institutional Funding (${val:,.0f} raised)"
        except (ValueError, TypeError):
            pass

        num_rounds = org.get("number_of_funding_rounds") or 0
        try:
            val = int(num_rounds) if isinstance(num_rounds, str) else num_rounds
            if isinstance(val, int) and val >= 2:
                return False, f"Institutional Funding ({val} funding rounds)"
        except (ValueError, TypeError):
            pass

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
# CHEAP NICHE PRE-FILTER (zero API cost)
# ---------------------------------------------------------------------------
_CIVIC_PREFIXES = [
    "city of ", "county of ", "state of ", "town of ", "village of ",
    "borough of ", "township of ", "municipality of ",
    "mayor", "sheriff", "police department", "fire department",
    "school district", "school board", "public school",
    "chamber of commerce",
]

_WELL_KNOWN_NON_TARGETS = [
    "coca-cola", "coca cola", "pepsi", "pepsico",
    "walmart", "target corporation", "costco", "kroger", "publix",
    "amazon.com", "google llc", "microsoft", "apple inc", "meta platforms",
    "mcdonald's", "mcdonalds", "starbucks", "chick-fil-a",
    "hibbett", "dollar general", "dollar tree", "family dollar",
    "home depot", "lowe's", "lowes",
    "at&t", "verizon", "t-mobile", "sprint",
    "wells fargo", "bank of america", "jpmorgan", "regions bank",
    "blue cross", "blue shield", "unitedhealth", "cigna", "aetna",
    "fedex", "ups ", "usps",
]


_NICHE_STOPWORDS = {
    "management", "services", "service", "solutions", "group", "company",
    "companies", "firm", "firms", "business", "businesses", "money",
    "institution", "institutions", "provider", "providers", "industry",
    "focus", "focuses", "focused", "managing", "that", "with", "from",
    "their", "they", "this", "these", "those", "high", "more", "most",
    "some", "other", "also", "very", "just", "only", "such", "like",
    "well", "about", "into", "over", "after", "before", "between",
    "under", "through", "during", "does", "have", "been", "being",
    "were", "will", "would", "could", "should", "technology",
    "technologies", "tech", "platform", "software", "digital",
    "national", "international", "global", "local", "regional",
    "north", "south", "east", "west", "carolina", "georgia", "alabama",
    "florida", "texas", "virginia", "york",
}


def quick_niche_prefilter(org, target_niche, niche_keywords=None, niche_industries=None):
    """Zero-cost pre-filter: reject orgs with no niche relevance signal.

    Matches against AI-generated keyword phrases and industry classifications.
    Companies in a matching Apollo industry are passed to the AI check even
    without an exact keyword match (Apollo tagging is inconsistent).

    Args:
        org: Apollo organization dict.
        target_niche: e.g. "vet clinics".
        niche_keywords: list of keyword phrases from suggest_search_params().
        niche_industries: list of Apollo industry names from suggest_search_params().

    Returns:
        (passes: bool, reason: str)
    """
    name = (org.get("name") or "").lower()
    desc = (org.get("short_description") or org.get("headline") or "").lower()
    industry = (org.get("industry") or "").lower()
    tags = [t.lower() for t in (org.get("keywords") or [])]

    # --- Block well-known non-targets by name ---
    for prefix in _CIVIC_PREFIXES:
        if name.startswith(prefix):
            return False, f"Civic entity: '{prefix}'"
    for term in _WELL_KNOWN_NON_TARGETS:
        if term in name:
            return False, f"Well-known non-target: '{term}'"

    # --- If no metadata to check, let the AI decide ---
    has_metadata = bool(desc.strip()) or bool(industry.strip()) or bool(tags)
    if not has_metadata:
        return True, "Sparse metadata — passing to AI check"

    combined = f"{name} {desc} {industry} {' '.join(tags)}"

    # --- Check AI-generated keyword phrases (multi-word, specific) ---
    if niche_keywords:
        for kw in niche_keywords:
            kw_lower = kw.strip().lower()
            if len(kw_lower) >= 3 and kw_lower in combined:
                return True, f"Keyword match: '{kw_lower}'"

    # --- Check non-stopword niche words (only specific terms) ---
    niche_words = set()
    for word in target_niche.lower().split():
        cleaned = word.strip(",.;:-()\"'")
        if len(cleaned) >= 4 and cleaned not in _NICHE_STOPWORDS:
            niche_words.add(cleaned)

    if niche_words:
        for word in niche_words:
            if word in combined:
                return True, f"Niche word match: '{word}'"

    # --- Industry match: if Apollo's industry classification matches,
    #     let the AI relevance check make the final call ---
    if niche_industries and industry:
        for ni in niche_industries:
            if ni.lower() == industry:
                return True, f"Industry match: '{ni}'"

    return False, "No niche relevance signal in metadata"


# ---------------------------------------------------------------------------
# AI RELEVANCE CHECK
# ---------------------------------------------------------------------------
_RELEVANCE_CACHE_VERSION = 2

def check_relevance_gpt4o(client, company_name, description, keywords, target_niche, mode):
    """Use GPT-4o to score whether a company is relevant to the target niche."""
    cached = cache.get("relevance", _RELEVANCE_CACHE_VERSION, company_name, description, target_niche, mode)
    if cached is not None:
        return tuple(cached)

    _json_schema = '{"match": true/false, "category": "<primary_operator|vendor|consultant|media|nonprofit|government|unrelated>", "confidence": "<high|medium|low>", "reason": "one sentence with specific evidence from the description", "disqualifier": "<code or null>"}\n\nFor "disqualifier", use one of: "government", "nonprofit", "media", "staffing", "too_large", "pe_backed", "unrelated", "writes_about_not_operates", or null if match is true.'

    if mode == "A":
        prompt = f"""You are an acquisition filter for a private equity investor.
Target niche: "{target_niche}"

Company: "{company_name}"
Description: "{description}"
Keywords: {keywords}

PASS if the company's products, services, or operations are relevant to the described
niche. Include companies that:
- Directly operate in the niche
- Provide technology, software, or platforms that serve this niche
- Operate in a closely adjacent sub-segment a PE buyer would plausibly consider
- Could reasonably compete with or sell to companies in this niche

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
- Completely unrelated industry with no plausible connection to the niche
- "PACE" in the niche means Program for All-Inclusive Care for the Elderly — NOT fitness/pace

When the niche is very specific (e.g., "compliance tech for specialty lending"), be GENEROUS
with adjacent companies. A regtech company serving financial services IS relevant even if it
doesn't specifically mention the exact sub-segment. It's better to let a borderline company
through for deeper analysis than to miss a real target.
Return JSON only: {_json_schema}"""
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

Return JSON only: {_json_schema}"""

    _cache_args = (_RELEVANCE_CACHE_VERSION, company_name, description, target_niche, mode)

    def _parse_relevance(content):
        data = json.loads(content)
        parts = [data.get("reason", "")]
        cat = data.get("category")
        conf = data.get("confidence")
        disq = data.get("disqualifier")
        if cat:
            parts.append(f"[{cat}]")
        if conf:
            parts.append(f"({conf} confidence)")
        if disq:
            parts.append(f"disqualifier={disq}")
        return data.get("match"), " ".join(parts)

    def _cache_and_return(result):
        cache.put("relevance", *_cache_args, value=list(result))
        return result

    try:
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            timeout=20,
        )
        return _cache_and_return(_parse_relevance(resp.choices[0].message.content))
    except Exception as e:
        msg = str(e).lower()
        if "content" in msg or "filter" in msg or "400" in msg:
            return False, "Content filter triggered"

    try:
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            timeout=20,
        )
        raw = resp.choices[0].message.content or ""
        match = re.search(r"\{.*\}", raw, re.DOTALL)
        if match:
            return _cache_and_return(_parse_relevance(match.group()))
    except Exception:
        pass

    return False, "AI check failed — skipped"


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
    cached = cache.get("pe_vc_web", company_name, domain)
    if cached is not None:
        return tuple(cached)

    snippets = []

    # 1. Company's own about/investor pages
    if domain:
        for path in ["/about", "/about-us", "/investors", "/company", "/press"]:
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

    # 2b. Tracxn profile (backup funding source)
    tracxn_slug = company_name.lower().replace(" ", "-").replace(",", "").replace(".", "")
    content = firecrawl_scrape_fn(f"https://tracxn.com/d/companies/{tracxn_slug}")
    if content and len(content) >= 200:
        snippets.append(f"TRACXN PROFILE:\n{content[:8000]}")

    # 2c. Google search for investor/funding info AND public ownership
    from urllib.parse import quote as _url_quote
    _google_queries = [
        f"{company_name} investors funding",
        f"{company_name} raised series",
        f"{company_name} parent company publicly traded",
        f"{company_name} owned by acquired",
    ]
    _google_hits = 0
    for query in _google_queries:
        if _google_hits >= 2:
            break
        gurl = f"https://www.google.com/search?q={_url_quote(query)}"
        content = firecrawl_scrape_fn(gurl)
        if content and len(content) >= 100:
            snippets.append(f"GOOGLE SEARCH ({query}):\n{content[:8000]}")
            _google_hits += 1

    # 3. Cross-check with PE firm portfolio pages if their name appears
    snippet_text = " ".join(snippets).lower()
    for firm_name, portfolio_url in _KNOWN_PE_PORTFOLIOS.items():
        if firm_name in snippet_text:
            content = firecrawl_scrape_fn(portfolio_url)
            if content and len(content) >= 100:
                snippets.append(f"PE FIRM PORTFOLIO ({firm_name}):\n{content[:8000]}")

    if not snippets:
        result = (False, "No web data")
        cache.put("pe_vc_web", company_name, domain, value=list(result))
        return result

    combined = "\n---\n".join(snippets)
    prompt = f"""Determine whether "{company_name}" is ineligible for acquisition because it is:
(A) owned by or backed by a private equity firm or venture capital firm, OR
(B) owned by, a subsidiary of, a division of, or a product line of a publicly traded company.

Either condition makes the company ineligible — return true if EITHER applies.

Look for evidence such as:
- "backed by [PE/VC firm name]" or "portfolio company of [firm]"
- "acquired by [firm]" or "a [parent company] company"
- Company name appearing on a PE/VC firm's portfolio page
- Crunchbase listing showing PE/VC investors or funding rounds
- Series A/B/C/D funding rounds, recapitalization, or leveraged buyout
- Any named PE or VC firm as an investor or owner
- Company is a product, brand, or division of a publicly traded company
- Parent company has a stock ticker (NYSE, NASDAQ, Euronext, LSE, TSX, etc.)
- Company's website belongs to a larger publicly traded parent
- Company name starts with or contains a publicly traded company's name

IMPORTANT: If "{company_name}" appears on any PE/VC firm's portfolio page, that is
definitive proof — return true. If the company is clearly a product line or brand
of a publicly traded company, that is also definitive — return true.

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
        result = (data.get("pe_vc_owned", False), data.get("evidence", ""))
        cache.put("pe_vc_web", company_name, domain, value=list(result))
        return result
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
            result = (data.get("pe_vc_owned", False), data.get("evidence", ""))
            cache.put("pe_vc_web", company_name, domain, value=list(result))
            return result
    except Exception:
        pass

    return False, "AI check failed"


# ---------------------------------------------------------------------------
# AI EXCLUSION MATCHER — bulk-triage near-misses against a user exclusion
# ---------------------------------------------------------------------------
def match_exclusion_batch(client, exclusion, companies):
    """Ask GPT which companies match a user-supplied exclusion description.

    Args:
        client: OpenAI client.
        exclusion: free-text exclusion, e.g. "digital transformation consulting".
        companies: list of {"company": str, "description": str} dicts.

    Returns:
        set of company names that MATCH the exclusion (i.e. should be
        marked reviewed / dismissed). Conservative on failure — returns
        only confident matches, empty set on error.
    """
    matches = set()
    valid_names = {c["company"] for c in companies if c.get("company")}

    for i in range(0, len(companies), 25):
        chunk = companies[i:i + 25]
        listing = "\n".join(
            f'{idx + 1}. "{c["company"]}" — {(c.get("description") or "(no description)")[:400]}'
            for idx, c in enumerate(chunk)
        )
        prompt = f"""A private equity user is triaging acquisition candidates. They want to
dismiss every company that matches this exclusion description:

EXCLUSION: "{exclusion}"

For each company below, decide whether it MATCHES the exclusion — meaning
its core business fits what the user wants to exclude. Be reasonably strict:
only mark a company as matching when its description clearly fits the
exclusion. A passing mention is not enough; the exclusion should describe
what the company primarily does.

Companies:
{listing}

Return JSON only:
{{"matches": ["Exact Company Name 1", "Exact Company Name 2"]}}
Use the exact company names as written above. Return an empty list if none match."""

        try:
            resp = client.chat.completions.create(
                model=OPENAI_MODEL,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
                timeout=30,
            )
            data = json.loads(resp.choices[0].message.content or "{}")
            for name in data.get("matches") or []:
                if name in valid_names:
                    matches.add(name)
        except Exception:
            continue

    return matches


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
        "publicly traded", "stock exchange", "ipo", "nasdaq",
        "nyse", "euronext", "subsidiary of", "division of",
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
            "publicly traded", "subsidiary of", "division of",
            "parent company", "stock exchange", "nasdaq", "nyse",
            "euronext", "listed on",
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
                f"Does this evidence indicate that '{candidate_name}' is (A) currently owned/backed "
                f"by a private equity firm, or (B) a subsidiary/division/product of a publicly traded "
                f"company? Either makes it ineligible. Return JSON: {{'is_pe_backed': true/false, "
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