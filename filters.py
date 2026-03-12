import json
import re
from config import client, OPENAI_MODEL


def is_buyable_structure(org, mode):
    emp    = org.get("estimated_num_employees", 0) or 0
    status = str(org.get("ownership_status") or "").strip().lower()
    tags   = [t.lower() for t in (org.get("keywords") or [])]

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
    "consulting group", "advisory group", " billing services",
    "software inc", "software llc",
    # Tech firms that slip through on healthcare-adjacent keywords
    "healthtech", " technologies", "tech solutions", " it services",
]

def is_obvious_mismatch(org, target_niche, mode):
    name = (org.get("name") or "").lower()
    for f in _UNIVERSAL_BLOCKS:
        if f in name: return True, f"Block: '{f}'"
    if mode == "B": return False, "Pass"
    extras = list(_MODE_A_BLOCKS)
    if "architect" in target_niche.lower():
        extras += ["realty", "real estate", "lumber", "golf course"]
    for f in extras:
        if f in name: return True, f"Mode-A block: '{f}'"
    return False, "Pass"


def check_relevance_gpt4o(company_name, description, keywords, target_niche, mode):
    if mode == "A":
        prompt = f"""You are an acquisition filter for a private equity investor.
Target niche: "{target_niche}"

Company: "{company_name}"
Description: "{description}"
Keywords: {keywords}

PASS if the company fits ONE of these:
1. A direct operator or service provider in the exact niche described above.
2. A highly adjacent operator serving the same core customer base in a directly related vertical.
3. Description is vague, but the company name and keywords strongly align with the target niche.

FAIL if any of these apply:
- Software, SaaS, analytics, HealthTech, or pure technology vendor — even if health-adjacent
- IT services, digital health platform, or software development firm
- Consulting, billing, staffing, marketing, or outsourced services firm
- Training, education, or coaching organization with no direct patient/client care operations
- Medical transportation, non-emergency medical transport (NEMT), or logistics company
- Large national chain or massive enterprise not suitable for acquisition
- Completely unrelated industry
- "PACE" in the niche means Program for All-Inclusive Care for the Elderly — NOT fitness/pace

If the company appears to be a legitimate local or regional operator in the target niche, PASS them.
Do not over-filter if the description is brief.
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
- Insurance company, managed care payer, financial services firm, or health insurer
- Large institutional network, national chain, or conglomerate with 10,000+ employees, UNLESS
  it is a direct competitor operating in the exact same niche as the target
- Technology company, IT firm, HealthTech, software developer, or digital health platform —
  even if their product serves the healthcare sector
- Medical transportation, non-emergency medical transport (NEMT), or logistics company
- Consulting, billing, staffing, training, or outsourced services firm
- Completely unrelated industry (manufacturing, finance, retail, food service, etc.)

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
