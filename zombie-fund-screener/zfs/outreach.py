"""AI email drafting for GP outreach — the zombie-fund use case.

Uses the same email-prospecting playbook as the sourcing engine (three
gates: delivered / opened / converted; Hook → Relate → Bridge → Ask),
but tailored to a completely different audience: a private equity GP,
not a founder.

The cardinal rule here: the email must NEVER hint at the zombie thesis.
A GP who senses "they think my fund is a zombie" gets defensive and the
door closes. The email is a credible, specific, peer-to-peer buyer
inquiry about one portfolio company — nothing more.

Requires an OpenAI key in Streamlit secrets (OPENAI_API_KEY) or the
OPENAI_API_KEY environment variable.
"""

import json
import os
from datetime import date


def get_openai_client():
    """Build an OpenAI client from secrets/env. Returns None if no key."""
    key = os.environ.get("OPENAI_API_KEY")
    if not key:
        try:
            import streamlit as st
            key = st.secrets.get("OPENAI_API_KEY")
        except Exception:
            pass
    if not key:
        return None
    from openai import OpenAI
    return OpenAI(api_key=key)


def draft_gp_email(client, gp, contact=None, funds=None, companies=None,
                   focus_company=None, sender_name="Trey"):
    """Draft one outreach email to a GP about acquiring a portfolio company.

    Args:
        client: OpenAI client.
        gp: GP dict from the database.
        contact: contact dict (preferred contact) or None.
        funds: list of fund dicts (vintages give useful context).
        companies: list of portfolio company dicts.
        focus_company: the specific company to express interest in
                       (dict) — strongly recommended; picked in the UI.
    Returns {"subject": ..., "body": ...}
    """
    funds = funds or []
    companies = companies or []
    contact_name = (contact or {}).get("name", "")
    first_name = contact_name.split()[0] if contact_name else ""
    contact_title = (contact or {}).get("title") or (contact or {}).get("role_tag", "")

    fund_lines = "\n".join(
        f"  - {f['name']} ({str(f.get('filing_date') or '?')[:4]})"
        for f in funds) or "  (none on file)"

    def _hold_years(co):
        acq = str(co.get("acquisition_date") or "")[:4]
        try:
            return f"{date.today().year - int(acq)} yrs held"
        except ValueError:
            return "hold unknown"

    co_lines = "\n".join(
        f"  - {c['name']} ({c.get('vertical') or 'Other'}, "
        f"{c.get('hq_state') or '?'}, {_hold_years(c)})"
        for c in companies) or "  (none on file)"

    focus = focus_company or (companies[0] if companies else None)
    focus_line = (f"{focus['name']} ({focus.get('vertical') or 'their sector'}, "
                  f"{focus.get('hq_state') or ''}, {_hold_years(focus)})"
                  if focus else "(no specific company — keep it sector-level)")

    prompt = f"""You are {sender_name}, a principal at New Capital Partners (NCP), a
lower-middle-market private equity firm. Write ONE email to a fellow PE
professional — a GP — expressing acquisition interest in one of their
portfolio companies. Your goal: earn a reply and a first call.

THE RECIPIENT:
- Firm: {gp['name']}
- Contact: {contact_name or 'the managing partner'} ({contact_title})
- Their funds:
{fund_lines}
- Their portfolio companies:
{co_lines}
- The company you're interested in: {focus_line}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
UNDERSTAND THE GP'S MINDSET (this is NOT founder outreach):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
This is a sophisticated PE professional. They read buyer inquiries for a
living and can smell an angle instantly. They are professionally proud
and highly sensitive about their fund's age and pace of exits.

THE CARDINAL RULE: never hint that you think their fund is old, slow,
stressed, or under pressure to sell. If they sense that thesis, they get
defensive and the door closes forever. FORBIDDEN words and framings:
zombie, aging, stale, legacy fund, wind-down, end of life, distressed,
liquidity pressure, LP pressure, DPI, "still holding", "been a while",
or ANY reference to how long they've held the company or how old the
fund is. You know the hold period — you must act as if you don't.

What DOES work on a GP:
  1. A specific, credible reason you want THIS company (sector fit,
     geography, your mandate) — buyer specificity signals a real bid.
  2. Peer respect — one line crediting what the company or firm does
     well. Professional, not flattering.
  3. Low-risk framing — "if it's core, say so and I'll leave you be"
     gives them a graceful out and paradoxically keeps the door open.
  4. Speed and certainty — LMM GPs answer buyers who look easy to
     transact with. No process demands, no diligence asks, just a call.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
STRUCTURE — the 4-beat arc:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
1. HOOK — subject + first sentence earn the open. The first sentence
   shows in the inbox preview: name the company or sector interest
   immediately. Never open with "Hi/Hello/Dear" as the first line.
2. RELATE — one line of peer respect: what the company (or their work
   with it) does well. Specific beats effusive.
3. BRIDGE — why it fits YOUR mandate (sector, size, geography) — the
   credible reason you're writing them and not a hundred others.
4. ASK — one easy action: a short call. Either ultra-low-friction
   ("worth a short call?") or assumptive ("how about Thursday at 3?").
   Pair with the graceful out: "if it's a long-term hold for you, tell
   me and I won't bother you again."

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SUBJECT LINE:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
1-6 words, under 50 characters, lowercase. NEVER a question mark.
Statements only. Best forms: the portfolio company's name, or
"interest in [company]", or "[company] inquiry".
NEVER: "Partnership Opportunity", "Acquisition Interest — [Fund III]",
anything referencing their fund.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
WRITING RULES:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- 50-90 words in the body. 3-6 sentences. Professional but plain —
  written like one investor to another, not a broker blast.
- First word: the company name, "your", or their first name. Never "I".
- DELIVERABILITY: plain text only — no links, images, attachments,
  ALL CAPS, or spam-trigger words (free, guarantee, act now, exclusive
  opportunity). One person, one email.
- No valuation, multiples, price, or deal terms. No "proven track
  record" boilerplate about NCP. One question mark maximum.
- Sign off with just "{sender_name}, New Capital Partners".

Return JSON only:
{{"subject": "lowercase subject", "body": "complete email body including sign-off"}}"""

    resp = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}],
        response_format={"type": "json_object"},
        temperature=0.8,
        timeout=30,
    )
    result = json.loads(resp.choices[0].message.content)
    return {
        "subject": result.get("subject", f"interest in "
                   f"{(focus or {}).get('name', gp['name']).lower()}"),
        "body": result.get("body", ""),
    }
