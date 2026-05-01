"""NCP active portfolio + conflict checking.

Used by the analysis pipeline to ensure new opportunities don't directly
compete with existing portfolio companies.
"""

import json
import re

from lib.constants import OPENAI_MODEL


# ---------------------------------------------------------------------------
# NCP ACTIVE PORTFOLIO
# Source: https://newcapitalpartners.com/companies/
# ---------------------------------------------------------------------------
NCP_ACTIVE_PORTFOLIO = [
    {
        "name": "ACES Quality Management",
        "sector": "Financial Services / RegTech",
        "description": (
            "Mortgage and consumer-lending quality control and compliance auditing "
            "software for banks, lenders, and credit unions."
        ),
    },
    {
        "name": "Ariel Re",
        "sector": "Specialty Reinsurance",
        "description": (
            "Specialty property and casualty reinsurance underwriter."
        ),
    },
    {
        "name": "Collect Rx",
        "sector": "Healthcare Revenue Cycle Management",
        "description": (
            "Out-of-network claims negotiation and revenue cycle management "
            "services for hospitals and ambulatory providers."
        ),
    },
    {
        "name": "ControlCase",
        "sector": "Cybersecurity Compliance / GRC",
        "description": (
            "Cybersecurity compliance, IT audit, and certification services "
            "covering PCI DSS, SOC 1/2, HITRUST, ISO 27001, and CMMC. "
            "C3PAO and Registered Provider Organization for the CMMC ecosystem."
        ),
    },
    {
        "name": "DotCom Therapy",
        "sector": "Virtual Behavioral Health",
        "description": (
            "Virtual behavioral health and teletherapy provider serving schools, "
            "families, and health plans."
        ),
    },
    {
        "name": "Precision",
        "sector": "Data Analytics — Professional Supply",
        "description": (
            "Data analytics platform serving the professional supply distribution market."
        ),
    },
    {
        "name": "Sprout",
        "sector": "IT Asset Disposition / E-Waste",
        "description": (
            "IT asset disposition, secure data destruction, and electronics recycling."
        ),
    },
]


def get_portfolio_summary():
    """Return a plain-text summary of the active portfolio (used in AI prompts)."""
    lines = []
    for c in NCP_ACTIVE_PORTFOLIO:
        lines.append(f"- {c['name']} ({c['sector']}): {c['description']}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CONFLICT CHECK
# ---------------------------------------------------------------------------
def check_portfolio_conflict(client, company_name, company_description):
    """Use GPT-4o to check whether a candidate conflicts with any NCP portfolio company.

    Returns dict: {"conflicts": bool, "with": "Portfolio Company Name or None",
                   "reason": "one-sentence explanation"}.
    Conservative on AI errors: returns "conflicts": False so candidates aren't lost.
    """
    portfolio_text = get_portfolio_summary()

    prompt = f"""You are screening a new investment opportunity for New Capital Partners (NCP),
a lower middle market private equity firm. NCP's current active portfolio includes:

{portfolio_text}

Candidate company being evaluated:
Name: "{company_name}"
Description: "{company_description}"

Determine whether the candidate company would CONFLICT with any existing portfolio company.
A conflict exists if the candidate is a direct competitor offering substantially the same
products or services to substantially the same customers as a portfolio company.

A conflict does NOT exist merely because of operating in an adjacent sector. For example:
- Two cybersecurity firms with different specialties and customers are usually NOT a conflict
- A behavioral health firm serving adults is NOT a conflict with one serving children/schools
- Different segments of a broad market (e.g., revenue cycle for hospitals vs physician practices)
  may or may not conflict — judge based on customer overlap

Be conservative: only flag a clear, direct competitive overlap.

Return JSON only:
{{"conflicts": true/false,
  "with": "Portfolio Company Name or null",
  "reason": "one-sentence explanation"}}"""

    def _parse(content):
        data = json.loads(content)
        return {
            "conflicts": bool(data.get("conflicts")),
            "with": data.get("with"),
            "reason": data.get("reason") or "",
        }

    # Attempt 1 — structured JSON output
    try:
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0,
            timeout=20,
        )
        return _parse(resp.choices[0].message.content)
    except Exception as e:
        msg = str(e).lower()
        if "content" not in msg and "filter" not in msg and "400" not in msg:
            return {"conflicts": False, "with": None, "reason": "AI error"}

    # Attempt 2 — retry without response_format
    try:
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            timeout=20,
        )
        raw = resp.choices[0].message.content or ""
        match = re.search(r"\{.*\}", raw, re.DOTALL)
        if match:
            return _parse(match.group())
    except Exception:
        pass

    return {"conflicts": False, "with": None, "reason": "AI error"}