import json
import re
from config import client, OPENAI_MODEL, APOLLO_INDUSTRIES


# ---------------------------------------------------------------------------
# AI — NICHE → APOLLO PARAMETERS
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


def suggest_search_params(niche_description: str) -> dict:
    """
    Maps a plain-English niche to Apollo industries + keyword tags.
    The prompt gives GPT-4o concrete Apollo tag examples so it generates
    short real tags rather than long descriptive phrases that match nothing.
    A rule-based fallback fires if the AI returns empty keywords.
    """
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

Classification guidance:
• PACE / senior day programs → "Individual & Family Services", "Non-Profit Organization Management",
  "Hospital & Health Care" (NOT "Pharmaceuticals" — that is drug manufacturers)
• Skilled nursing / assisted living → "Hospital & Health Care", "Individual & Family Services"
• Behavioral / mental health → "Mental Health Care", "Hospital & Health Care", "Individual & Family Services"
• Small clinics / practices → "Medical Practice" is usually more accurate than "Hospital & Health Care"
• Home services / HVAC → "Construction", "Facilities Services"
• Non-profit care operators → always add "Non-Profit Organization Management"

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PARAMETER 2 — KEYWORD TAGS (REQUIRED — do not leave blank)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Apollo tags each company with SHORT keyword phrases (1–4 words each).
You MUST return 2–5 tags. Empty keywords is not acceptable.

Real Apollo tags by domain (use these as your vocabulary):
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

    def _parse_suggest(content):
        data     = json.loads(content)
        valid    = [i for i in (data.get("industries") or []) if i in APOLLO_INDUSTRIES]
        keywords = (data.get("keywords") or "").strip()
        if not keywords:
            keywords = _fallback_keywords(niche_description)
        return {
            "industries": valid or ["Hospital & Health Care"],
            "keywords":   keywords,
        }

    # Attempt 1 — structured JSON output
    try:
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0,
            timeout=25,
        )
        return _parse_suggest(resp.choices[0].message.content)
    except Exception as e:
        if "content" not in str(e).lower() and "filter" not in str(e).lower() and "400" not in str(e):
            return {
                "industries": ["Hospital & Health Care"],
                "keywords":   _fallback_keywords(niche_description),
            }

    # Attempt 2 — retry without response_format (avoids content-filter on structured output)
    try:
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            timeout=25,
        )
        raw   = resp.choices[0].message.content or ""
        match = re.search(r'\{.*\}', raw, re.DOTALL)
        if match:
            return _parse_suggest(match.group())
    except Exception:
        pass

    return {
        "industries": ["Hospital & Health Care"],
        "keywords":   _fallback_keywords(niche_description),
    }
