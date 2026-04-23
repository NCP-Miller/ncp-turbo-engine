import json
import re
from config import client, OPENAI_MODEL


def is_buyable_structure(org, mode):
    emp    = org.get("estimated_num_employees", 0) or 0
    status = str(org.get("ownership_status") or "").strip().lower()
    tags   = [t.lower() for t in (org.get("keywords") or [])]
    name   = (org.get("name") or "").lower()
    desc   = (org.get("short_description") or "").lower()
    industry = (org.get("industry") or "").lower()

    # ── Universal: non-acquirable entity types ──
    # Name-level government signals (safe — these indicate the org IS a govt body)
    _govt_name_signals = ["federal commission", "federal agency",
                          "department of", "bureau of", "office of the"]
    for sig in _govt_name_signals:
        if sig in name:                                         return False, "Government Entity"
    # Status-level checks
    if status in ("government",):                               return False, "Government Entity"
    if status in ("non-profit", "nonprofit"):                   return False, f"Non-Acquirable ({status})"
    # Description-level nonprofit signals
    _np_signals = ["non-profit organization", "nonprofit organization", "501(c)"]
    for sig in _np_signals:
        if sig in desc:                                         return False, "Non-Profit"
    if "non-profit organization management" in industry:        return False, "Non-Profit"
    if "government administration" in industry and emp > 50:    return False, "Government Entity"

    if mode == "A":
        if status == "public":                                  return False, "Publicly Traded"
        if status == "subsidiary":                              return False, "Subsidiary"
        if "private equity" in tags or "venture capital" in tags: return False, "PE/VC Backed"
        if emp > 7500:                                          return False, f"Too Large ({emp})"
    else:  # Mode B — block public companies and large conglomerates
        if "public" in status:                                  return False, "Publicly Traded"
        if emp > 10000:                                         return False, f"Too Large ({emp})"

    return True, "OK"


_UNIVERSAL_BLOCKS = [
    "university", "college", "food service", "catering",
    "staffing solutions", "temp agency",
    # Transportation / logistics — not care operators
    " transportation", "non-emergency transport", "medical transport",
    "nemt ", " logistics",
    # Large food-service management companies
    "eurest", "aramark", "sodexo", "compass group", "canteen",
]
_MODE_A_BLOCKS = [
    # Non-acquirable entity types
    " hospital", "medical center", "health system",
    "law firm", " pllc", " llp",
    " forum", " blog", " news", " magazine", " journal", " media",
    " alliance", " coalition", " association", " society", " council",
    " consortium", " observatory", " institute", " foundation",
    "center of excellence",
    "federal ",
    # Original blocks
    "consulting group", "advisory group", " billing services",
    "software inc", "software llc",
    "healthtech",
]

def is_obvious_mismatch(org, target_niche, mode):
    name = (org.get("name") or "").lower()
    desc = (org.get("short_description") or "").lower()
    for f in _UNIVERSAL_BLOCKS:
        if f in name: return True, f"Block: '{f}'"
    if mode == "B": return False, "Pass"
    extras = list(_MODE_A_BLOCKS)
    if "architect" in target_niche.lower():
        extras += ["realty", "real estate", "lumber", "golf course"]
    for f in extras:
        if f in name: return True, f"Mode-A block: '{f}'"
    # Description-level blocks for Strategy A — catch entities the name alone misses
    _desc_blocks = ["non-profit", "nonprofit", "government agency",
                    "trade association", "professional organization",
                    "advocacy organization", "blog that", "media outlet",
                    "news outlet", "law firm", "staffing agency",
                    "recruitment and staffing", "recruiting agency"]
    for f in _desc_blocks:
        if f in desc: return True, f"Desc block: '{f}'"
    return False, "Pass"


def check_relevance_gpt4o(company_name, description, keywords, target_niche, mode):
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

    # Attempt 1 — structured JSON output
    try:
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            timeout=20,
        )
        return _parse_relevance(resp.choices[0].message.content)
    except Exception as e:
        if "content" not in str(e).lower() and "filter" not in str(e).lower() and "400" not in str(e):
            return True, "AI Error"

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
            return _parse_relevance(match.group())
    except Exception:
        pass

    return True, "AI Error"
