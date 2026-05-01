"""AI-driven niche-to-Apollo-parameter mapping.

Adaptive 2-tier strategy:
  1. Classify niche complexity (simple / moderate / complex)
  2. Route to simple (single-prompt) or complex (research + generate) tag generation

Pure functions: pass in the OpenAI client.
"""

import json
import re

from lib.constants import OPENAI_MODEL, APOLLO_INDUSTRIES

CLASSIFIER_MODEL = "gpt-4o-mini"


# ---------------------------------------------------------------------------
# RULE-BASED FALLBACK KEYWORDS
# ---------------------------------------------------------------------------
def _fallback_keywords(niche: str) -> str:
    """Rule-based keyword fallback used when the AI call returns nothing."""
    n = niche.lower()
    if any(x in n for x in ["pace", "program for all-inclusive", "all-inclusive care"]):
        return "pace program, adult day care, home health"
    if any(x in n for x in ["senior", "elderly", "elder care", "aging in place"]):
        return "senior living, home health, assisted living"
    if any(x in n for x in ["home health", "home care", "home-based"]):
        return "home health, home care, skilled nursing"
    if any(x in n for x in ["hospice", "palliative"]):
        return "hospice, palliative care, home health"
    if any(x in n for x in ["assisted living", "memory care", "dementia"]):
        return "assisted living, memory care, senior living"
    if any(x in n for x in ["skilled nursing", "nursing home", "snf"]):
        return "skilled nursing facility, long-term care"
    if any(x in n for x in ["behavioral health", "mental health", "psychiatr"]):
        return "behavioral health, mental health, outpatient"
    if any(x in n for x in ["substance", "addiction", "recovery", "rehab"]):
        return "substance abuse, addiction treatment, behavioral health"
    if any(x in n for x in ["urgent care", "walk-in"]):
        return "urgent care, ambulatory care, primary care"
    if any(x in n for x in ["hvac", "heating", "cooling", "air condition"]):
        return "hvac, mechanical contractor, facilities management"
    if any(x in n for x in ["veterinary", "vet ", "animal hospital"]):
        return "veterinary, animal hospital, pet care"
    if any(x in n for x in ["dental", "dentist"]):
        return "dental practice, dental care"
    if any(x in n for x in ["physical therapy", "occupational therapy", "pt clinic"]):
        return "physical therapy, outpatient rehab, sports medicine"
    return ""


# ---------------------------------------------------------------------------
# SHARED HELPERS
# ---------------------------------------------------------------------------
def _parse_suggest(content, niche_description):
    """Parse JSON from an LLM response into validated industries + keywords."""
    data = json.loads(content)
    valid = [i for i in (data.get("industries") or []) if i in APOLLO_INDUSTRIES]
    keywords = (data.get("keywords") or "").strip()
    if not keywords:
        keywords = _fallback_keywords(niche_description)
    return {
        "industries": valid or ["Hospital & Health Care"],
        "keywords": keywords,
    }


def _call_with_fallback(client, prompt, niche_description):
    """Try structured JSON output, then unstructured, then rule-based fallback."""
    # Attempt 1 — structured JSON output
    try:
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0,
            timeout=25,
        )
        return _parse_suggest(resp.choices[0].message.content, niche_description)
    except Exception as e:
        msg = str(e).lower()
        if "content" not in msg and "filter" not in msg and "400" not in msg:
            return {
                "industries": ["Hospital & Health Care"],
                "keywords": _fallback_keywords(niche_description),
            }

    # Attempt 2 — retry without response_format (avoids content-filter on structured output)
    try:
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            timeout=25,
        )
        raw = resp.choices[0].message.content or ""
        match = re.search(r"\{.*\}", raw, re.DOTALL)
        if match:
            return _parse_suggest(match.group(), niche_description)
    except Exception:
        pass

    return {
        "industries": ["Hospital & Health Care"],
        "keywords": _fallback_keywords(niche_description),
    }


# ---------------------------------------------------------------------------
# TIER 1 — COMPLEXITY CLASSIFIER
# ---------------------------------------------------------------------------
def _classify_complexity(client, niche_description: str) -> dict:
    """Classify a niche as simple, moderate, or complex using GPT-4o-mini."""
    prompt = f"""Classify the following niche description into exactly one complexity level.

Niche: "{niche_description}"

Definitions:
- "simple": A clear vertical with a single operator type (e.g., "veterinary clinics",
  "HVAC contractors", "PACE programs").
- "moderate": A specific niche with some nuance, geography, or sub-segment (e.g.,
  "specialty pediatric dental practices in the southeast").
- "complex": Multi-condition, requires actual research, has explicit constraints,
  named competitors to avoid, or asks for differentiation (e.g., "CMMC platform that
  is differentiated and not competitive with ControlCase, with options for staffing OR
  training OR self-assessment tech").

Return JSON only:
{{"complexity": "simple|moderate|complex", "rationale": "one sentence"}}"""

    try:
        resp = client.chat.completions.create(
            model=CLASSIFIER_MODEL,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0,
            timeout=15,
        )
        data = json.loads(resp.choices[0].message.content)
        if data.get("complexity") in ("simple", "moderate", "complex"):
            return data
    except Exception:
        pass

    return {"complexity": "simple", "rationale": "classifier failed; defaulting to simple"}


# ---------------------------------------------------------------------------
# TIER 2A — SIMPLE TAG GENERATION (single-prompt, pattern-based)
# ---------------------------------------------------------------------------
def _simple_tag_generation(client, niche_description: str) -> dict:
    """Single-prompt tag generation using pattern examples."""
    prompt = f"""You are configuring a B2B company database search on Apollo.io.
The analyst is targeting companies in this niche:

  "{niche_description}"

Study that niche carefully. Your choices must be driven by what kinds of companies
operate in that specific niche — not just obvious label matching.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PARAMETER 1 — INDUSTRY CATEGORIES (choose 3–5)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Pick from this exact list only. Choose the categories where companies in this niche
are ACTUALLY classified in Apollo — include non-obvious ones:

{json.dumps(APOLLO_INDUSTRIES)}

Below are examples that demonstrate the pattern of how to translate a niche into
Apollo industry categories. Study the technique — picking non-obvious categories
where operators are actually classified — then apply that same technique to the
user's niche even if it doesn't match any example exactly:

- PACE / senior day programs → "Individual & Family Services", "Non-Profit Organization Management",
  "Hospital & Health Care" (NOT "Pharmaceuticals" — that is drug manufacturers)
- Skilled nursing / assisted living → "Hospital & Health Care", "Individual & Family Services"
- Behavioral / mental health → "Mental Health Care", "Hospital & Health Care", "Individual & Family Services"
- Small clinics / practices → "Medical Practice" is usually more accurate than "Hospital & Health Care"
- Home services / HVAC → "Construction", "Facilities Services"
- CMMC / cybersecurity compliance → "Computer & Network Security",
  "Information Technology and Services", "Management Consulting"
  (NOT just "Defense & Space" — most CMMC specialists aren't classified there)
- CCA / CCP staffing for cybersecurity → "Staffing and Recruiting",
  "Computer & Network Security"
- Non-profit care operators → always add "Non-Profit Organization Management"

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PARAMETER 2 — KEYWORD TAGS (REQUIRED — do not leave blank)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Apollo tags each company with SHORT keyword phrases (1–4 words each).
You MUST return 2–5 tags. Empty keywords is not acceptable.

Below are examples that demonstrate the pattern of how to translate a niche into
short, specific Apollo keyword tags. Study the technique — short specific tags,
sub-niches, multiple tag angles — then apply that same technique to the user's
niche even if it doesn't match any example exactly. Generate tags that feel like
they were written in the same style and granularity as the examples, but are
specific to the user's actual niche:

  PACE / senior day:   "pace program", "adult day care", "adult day services",
                       "managed long-term care", "home health", "senior services"
  Senior living:       "senior living", "assisted living", "memory care",
                       "independent living", "continuing care"
  Home-based care:     "home health", "home care", "home-based care", "homemaker services"
  Skilled nursing:     "skilled nursing facility", "long-term care", "nursing home", "rehabilitation"
  Hospice / palliative:"hospice", "palliative care", "end-of-life care"
  Behavioral health:   "behavioral health", "mental health", "outpatient therapy",
                       "substance abuse", "addiction treatment"
  General healthcare:  "managed care", "medicaid", "medicare", "primary care",
                       "ambulatory care", "urgent care", "telehealth"
  CMMC / GovCon cyber: "cmmc", "c3pao", "registered provider organization",
                       "rpo", "cca", "ccp", "cmmc assessor", "cmmc certification",
                       "dfars compliance", "nist 800-171", "cui", "controlled unclassified",
                       "fedramp", "govcon cyber", "defense contractor"
  Cybersecurity (private operators):
                       "managed security", "mssp", "soc as a service", "vulnerability assessment",
                       "penetration testing", "compliance auditing", "security operations",
                       "incident response"
  Home / trade:        "hvac", "plumbing", "electrical contractor", "facilities management"
  Veterinary:          "veterinary", "animal hospital", "pet care"

RULES:
1. Tags MUST be 1–4 words. Longer phrases will match nothing in Apollo.
2. Choose tags that specifically describe "{niche_description}" operators.
3. Return 2–5 tags as a comma-separated string.
4. You MUST return keywords — do not return an empty string.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Return JSON only — no explanation:
{{"industries": ["Category A", "Category B", "Category C"],
  "keywords":   "short tag one, short tag two, short tag three"}}"""

    return _call_with_fallback(client, prompt, niche_description)


# ---------------------------------------------------------------------------
# TIER 2B — COMPLEX TAG GENERATION (research + generate, two-call)
# ---------------------------------------------------------------------------
def _complex_tag_generation(client, niche_description: str) -> dict:
    """Two-call approach: research the niche, then generate tags from that research."""

    # --- Call 1: Research ---
    research_prompt = f"""You are a lower middle market private equity analyst researching a niche
for a company sourcing campaign. Analyze this niche in 3–5 sentences:

  "{niche_description}"

Cover:
1. What kinds of operators actually do this work (company types, not just the label).
2. What specific sub-niches or specializations exist within it.
3. What Apollo.io industry categories these companies are most likely classified under
   (think about how small operators self-classify, not how the industry is labeled at scale).
4. Any specific small-operator types that match this niche but might be missed by
   obvious keyword searches.

Be concrete and specific. No boilerplate."""

    try:
        research_resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": research_prompt}],
            temperature=0.2,
            timeout=30,
        )
        research = research_resp.choices[0].message.content or ""
    except Exception:
        # If research call fails, fall back to simple
        return _simple_tag_generation(client, niche_description)

    # --- Call 2: Generate tags using research as context ---
    tag_prompt = f"""You are configuring a B2B company database search on Apollo.io.
The analyst is targeting companies in this niche:

  "{niche_description}"

A research analyst has provided the following context about this niche:

{research}

Use that research to make informed choices below.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PARAMETER 1 — INDUSTRY CATEGORIES (choose 3–5)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Pick from this exact list only. Choose the categories where companies in this niche
are ACTUALLY classified in Apollo — include non-obvious ones the research identified:

{json.dumps(APOLLO_INDUSTRIES)}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PARAMETER 2 — KEYWORD TAGS (REQUIRED — do not leave blank)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Apollo tags each company with SHORT keyword phrases (1–4 words each).
You MUST return 2–5 tags. Empty keywords is not acceptable.

Use the research above to choose tags that target the specific operator types,
sub-niches, and classification patterns identified. Tags should be 1–4 words,
specific to the actual niche, and likely to appear on real Apollo company profiles.

RULES:
1. Tags MUST be 1–4 words. Longer phrases will match nothing in Apollo.
2. Choose tags that specifically describe "{niche_description}" operators.
3. Return 2–5 tags as a comma-separated string.
4. You MUST return keywords — do not return an empty string.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Return JSON only — no explanation:
{{"industries": ["Category A", "Category B", "Category C"],
  "keywords":   "short tag one, short tag two, short tag three"}}"""

    return _call_with_fallback(client, tag_prompt, niche_description)


# ---------------------------------------------------------------------------
# PUBLIC API
# ---------------------------------------------------------------------------
def suggest_search_params(client, niche_description: str) -> dict:
    """Map a plain-English niche to Apollo industries + keyword tags.

    Uses adaptive routing: classifies niche complexity, then routes to
    a single-prompt strategy (simple/moderate) or a two-call research
    strategy (complex).

    Args:
        client: An initialized OpenAI client.
        niche_description: Plain-English description of the target niche.

    Returns:
        dict with keys "industries" (list of Apollo industry strings) and
        "keywords" (comma-separated string of short tags).
    """
    classification = _classify_complexity(client, niche_description)
    complexity = classification.get("complexity", "simple")
    print(f"[AI Params] Niche complexity: {complexity} — {classification.get('rationale', '')}")

    if complexity == "complex":
        try:
            return _complex_tag_generation(client, niche_description)
        except Exception:
            return _simple_tag_generation(client, niche_description)
    else:
        return _simple_tag_generation(client, niche_description)
