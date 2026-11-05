"""Banking - Special Assets: who at lenders manages the distressed loans.

As sponsor-backed companies hit the maturity wall, lenders end up
controlling businesses they do not want to own. The people who decide
what happens to those credits (workout, restructuring, a sale of the
business) sit in banks' special assets groups and in mezzanine and
private credit funds' portfolio and workout teams. This module holds the
search vocabulary for finding them and the AI review that separates the
right contacts from the many look-alike titles (consumer collections,
mortgage and real estate workouts, loan operations).
"""

import json
import re

# Titles Apollo matches against (substring-style), bank side.
SPECIAL_ASSETS_TITLES = [
    "special assets", "special asset", "special assets officer",
    "special assets manager", "special assets group", "workout",
    "loan workout", "credit workout", "loan restructuring",
    "credit restructuring", "restructuring officer", "managed assets",
    "problem asset", "problem loan", "asset recovery", "loan recovery",
    "commercial recovery", "remedial", "classified assets",
    "criticized assets", "special situations", "distressed debt",
    "loan resolution", "asset resolution", "credit resolution",
]

# People at a bank who see troubled sponsor credits first or can route.
CREDIT_ROUTING_TITLES = [
    "chief credit officer", "senior credit officer", "credit officer",
    "sponsor finance", "leveraged finance", "middle market banking",
    "corporate banking", "portfolio manager",
]

# Mezzanine / private credit / BDC: the deal and portfolio people own
# underperforming credits at most funds.
FUND_TITLES = [
    "portfolio management", "portfolio manager", "workout", "restructuring",
    "special situations", "managing director", "partner", "principal",
    "head of portfolio", "chief credit officer", "managing partner",
]

FUND_KEYWORDS = [
    "mezzanine", "private credit", "direct lending", "subordinated debt",
    "business development company", "sbic", "junior capital",
    "special situations lending",
]

# Seniorities searched. Special assets officers are often titled
# "Officer" or "Manager", so manager and senior are included.
SENIORITIES = ["owner", "founder", "c_suite", "partner", "vp", "head",
               "director", "manager", "senior"]

# Obvious mismatches, dropped before the AI review to save calls.
_HARD_EXCLUDE = re.compile(
    r"\b(mortgage|residential|consumer|credit card|card services|auto loan|"
    r"indirect|collections? (agent|specialist|representative|rep)|"
    r"collector|teller|branch manager|personal banker|bsa|aml|fraud|"
    r"compliance|marketing|human resources|recruit|software|engineer|"
    r"intern|assistant|administrative|receptionist)\b", re.I)


def plausible(title):
    """Cheap pre-filter: keep anything that is not an obvious mismatch."""
    t = (title or "").strip()
    return bool(t) and not _HARD_EXCLUDE.search(t)


_REVIEW_SYSTEM = """You qualify outreach contacts for New Capital Partners (NCP), a lower middle market private equity firm. NCP buys operating businesses (not real estate). Its thesis: the coming debt maturity wall will push many sponsor-backed companies into their lenders' hands, and lenders do not want to own them. NCP wants relationships with the people at lenders who manage those distressed credits and decide how to exit them, so NCP is the first call when a business needs a buyer.

Grade each contact:
- "A": directly manages or leads workouts of troubled COMMERCIAL loans to operating companies (C&I, middle market, sponsor or leveraged loans) at a bank, or leads portfolio workouts or special situations at a mezzanine, private credit or BDC lender. Typical titles: Special Assets Officer, Manager, Director or Head; Workout Officer; Credit Restructuring; Managed Assets; Commercial Loan Recovery; Special Situations at a lender. These are the people who decide on a credit bid, a consensual sale of the business, or a 363 sale.
- "B": cannot run the workout but sees troubled credits first or can route NCP to the person who does: chief or senior credit officers, sponsor finance or leveraged finance heads, middle market or corporate banking leaders, portfolio managers at mezzanine or private credit funds, and partners or MDs at small mezzanine funds where the deal team also owns problem credits.
- "reject": consumer, retail, mortgage, residential, auto or credit card collections and recovery; special assets roles that are clearly commercial real estate only (property, REO, CRE workouts), since NCP does not buy real estate; loan operations, servicing processors, analysts and associates without decision authority, compliance, fraud, BSA/AML; anyone not employed by a lender (turnaround consultants, law firms, accounting firms, receivers are a different channel; say so); and anyone whose title does not plausibly involve commercial credit.

Use the company too: a "Special Assets" title at a non-lender, or a "Portfolio Manager" at an asset manager that does not lend, is a reject. Classify the lender type as "Bank", "Mezzanine / Private Credit", "BDC", "Other lender" or "Not a lender".

Geography: the search is for the given metro. A contact elsewhere in the same state at a bank or fund headquartered in that state is acceptable (special assets groups often sit in a regional hub); mark "local": false for them. A contact clearly outside the state is a reject unless the note says the institution is based in the metro.

For A and B contacts give a one-line outreach angle in plain language that Trey could use to open the conversation (no em dashes). When in doubt between A and B, choose B; when in doubt between B and reject, explain the doubt in the reason.

Return JSON only: {"results": [{"id": "...", "tier": "A|B|reject", "lender_type": "...", "local": true, "reason": "one short sentence", "angle": "one sentence, empty for rejects"}]} with one entry per contact, in any order."""


def _extract_json(text):
    t = (text or "").strip()
    t = re.sub(r"^```(?:json)?\s*|\s*```$", "", t)
    start, end = t.find("{"), t.rfind("}")
    if start == -1 or end <= start:
        raise ValueError("no JSON in the model's reply")
    return json.loads(t[start:end + 1])


def _claude_json(client, system, user):
    """One Claude call returning JSON. Claude Opus 5.5 thinks adaptively;
    low effort suits per-contact grading. Server-side fallback reroutes a
    policy decline to Anthropic's recommended model."""
    import anthropic
    kwargs = dict(
        model="claude-opus-5-5",
        max_tokens=16000,
        system=system,
        messages=[{"role": "user", "content": user}],
        output_config={"effort": "low"},
    )
    try:
        with client.beta.messages.stream(
                betas=["server-side-fallback-2026-07-01"],
                fallbacks="default", **kwargs) as s:
            resp = s.get_final_message()
    except anthropic.BadRequestError as e:
        if "fallback" not in str(e).lower():
            raise
        with client.messages.stream(**kwargs) as s:
            resp = s.get_final_message()
    if resp.stop_reason == "refusal":
        raise RuntimeError("the model declined the request")
    return _extract_json("".join(b.text for b in resp.content
                                 if getattr(b, "type", "") == "text"))


def _openai_json(client, system, user):
    resp = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "system", "content": system},
                  {"role": "user", "content": user}],
        response_format={"type": "json_object"},
        temperature=0,
        timeout=120,
    )
    return json.loads(resp.choices[0].message.content)


def review_batch(candidates, location_label, anthropic_client=None,
                 openai_client=None):
    """Grade one batch of candidate dicts (id, name, title, company,
    company_industry, city, state, note). Returns ({id: verdict}, engine)."""
    user = (f"Metro searched: {location_label}\n\nContacts:\n"
            + json.dumps(candidates, ensure_ascii=False, indent=1))
    errors = []
    if anthropic_client is not None:
        try:
            data = _claude_json(anthropic_client, _REVIEW_SYSTEM, user)
            engine = "Claude"
        except Exception as e:
            errors.append(f"Claude: {type(e).__name__}: {str(e)[:200]}")
            data = None
    else:
        data = None
    if data is None and openai_client is not None:
        try:
            data = _openai_json(openai_client, _REVIEW_SYSTEM, user)
            engine = "GPT-4o"
        except Exception as e:
            errors.append(f"GPT-4o: {type(e).__name__}: {str(e)[:200]}")
    if data is None:
        raise RuntimeError("; ".join(errors) or "no AI client configured")
    out = {}
    for r in data.get("results") or []:
        if isinstance(r, dict) and r.get("id") is not None:
            tier = str(r.get("tier") or "").strip()
            out[str(r["id"])] = {
                "tier": tier if tier in ("A", "B") else "reject",
                "lender_type": str(r.get("lender_type") or ""),
                "local": bool(r.get("local", True)),
                "reason": str(r.get("reason") or ""),
                "angle": str(r.get("angle") or ""),
            }
    return out, engine
