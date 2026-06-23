"""Company enrichment + scoring functions.

Description generation, differentiation/priority/growth/transaction-readiness/
conviction scoring, and revenue/EBITDA estimation. All API clients/keys passed in.
"""

import json
import os
import concurrent.futures

from lib.constants import OPENAI_MODEL


def _format_feedback_section(feedback_history, max_entries=10):
    """Build a calibration prompt section from recent user feedback."""
    if not feedback_history:
        return ""
    recent = feedback_history[-max_entries:]
    liked, rejected, caveats = [], [], []
    for fb in recent:
        verdict = (fb.get("verdict") or "").lower()
        company = fb.get("company", "?")
        text = fb.get("feedback", "")
        niche = fb.get("niche", "")
        entry = f"  - {company}"
        if niche:
            entry += f" ({niche})"
        if text:
            entry += f": {text}"
        if verdict == "liked":
            liked.append(entry)
        elif verdict == "rejected":
            rejected.append(entry)
        elif verdict == "caveats":
            caveats.append(entry)

    sections = []
    if liked:
        sections.append("Companies Trey LIKED (match these traits):\n" + "\n".join(liked))
    if rejected:
        sections.append("Companies Trey REJECTED (avoid these traits):\n" + "\n".join(rejected))
    if caveats:
        sections.append("Trey's SPECIFIC CRITIQUES:\n" + "\n".join(caveats))
    if not sections:
        return ""
    return ("\n\nCALIBRATION FROM PAST FEEDBACK:\n"
            + "\n".join(sections)
            + "\nWeight these patterns when scoring.")


def _load_thesis():
    thesis_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "ncp_thesis.json")
    try:
        with open(thesis_path) as f:
            return json.load(f)
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# SECTOR BENCHMARKS
# ---------------------------------------------------------------------------
EASTERN_US_STATES = {
    "al", "ar", "co", "ct", "dc", "de", "fl", "ga", "ia", "il", "in",
    "ks", "ky", "la", "ma", "md", "me", "mi", "mn", "mo", "ms", "nc",
    "nd", "ne", "nh", "nj", "ny", "oh", "ok", "pa", "ri", "sc", "sd",
    "tn", "tx", "va", "vt", "wi", "wv",
}

REV_PER_EMP = {
    "healthcare":            150_000,
    "financial_services":    200_000,
    "technology":            250_000,
    "professional_services": 175_000,
    "default":               175_000,
}

EBITDA_MARGINS = {
    "healthcare":            (0.10, 0.20),
    "financial_services":    (0.15, 0.25),
    "technology":            (0.15, 0.30),
    "professional_services": (0.12, 0.22),
    "default":               (0.10, 0.20),
}


# ---------------------------------------------------------------------------
# DESCRIPTION GENERATION
# ---------------------------------------------------------------------------
def generate_company_description(
    client,
    firecrawl_scrape_fn,
    company_name,
    domain,
    apollo_desc,
    apollo_keywords,
    niche=None,
):
    """Generate a 2-3 sentence factual description from website + Apollo + Google."""
    snippets = []
    if domain:
        urls = [f"https://{domain}", f"https://{domain}/about", f"https://{domain}/about-us"]
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as ex:
            futures = {ex.submit(firecrawl_scrape_fn, u): u for u in urls}
            for f in concurrent.futures.as_completed(futures):
                content = f.result()
                if content and len(content) >= 100:
                    snippets.append(content[:8000])

    from urllib.parse import quote as _url_quote
    _gq = f"{company_name} {niche}" if niche else company_name
    _google_url = f"https://www.google.com/search?q={_url_quote(_gq)}"
    try:
        _gc = firecrawl_scrape_fn(_google_url)
        if _gc and len(_gc) >= 100:
            snippets.append(f"GOOGLE SEARCH RESULTS:\n{_gc[:6000]}")
    except Exception:
        pass

    web_text = "\n---\n".join(snippets) if snippets else "(no website content available)"

    prompt = f"""Write a 2-3 sentence factual description of "{company_name}".

Apollo database info:
- Description: "{apollo_desc}"
- Keywords: {apollo_keywords}

Website content:
{web_text[:20000]}

Rules:
- Focus on what the company does, who they serve, and where they operate
- Be factual and concise — no marketing language or superlatives
- If limited information is available, write what you can confirm
- Return ONLY the description text, nothing else"""

    try:
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
            timeout=20,
        )
        result = (resp.choices[0].message.content or "").strip()
        if result:
            return result
    except Exception:
        pass

    return apollo_desc if apollo_desc else "No description available."


# ---------------------------------------------------------------------------
# DIFFERENTIATION
# ---------------------------------------------------------------------------
def assess_differentiation(client, company_name, description, niche, feedback_history=None):
    """Rate company differentiation within its niche (High/Medium/Low) + confidence."""
    _fb = _format_feedback_section(feedback_history)
    prompt = f"""You are evaluating whether a company is meaningfully differentiated within its niche market.

Search niche: "{niche}"
Company: "{company_name}"
Description: "{description}"

Your job is to determine whether this company has a UNIQUE VALUE PROPOSITION or competitive
differentiator that sets it apart from the typical operator in the "{niche}" space.

Scoring guidance:
- HIGH: The company has a clear, specific differentiator within the niche — e.g., proprietary
  technology or methodology, a highly specialized sub-niche focus, a unique service delivery model,
  a regulatory or IP-based moat, vertical integration others lack, or a demonstrably novel approach.
- MEDIUM: The company shows some differentiation but it's not clearly defensible or particularly rare.
- LOW: The company is a standard/commodity operator in this niche with no obvious unique value
  proposition — it does essentially what most competitors do.

IMPORTANT: Do NOT rate a company as "High" simply because it seems out of place or unrelated to
the niche. A company that slipped through filters and doesn't truly belong in this search should
be rated LOW, not HIGH.

Also rate your CONFIDENCE in this assessment:
- HIGH confidence: You have detailed info about the company's services, approach, and market position.
- MEDIUM confidence: You have a reasonable description but lack specifics about their competitive edge.
- LOW confidence: Very limited information — the description is vague or generic.

Return JSON only: {{"differentiation": "High", "reason": "one sentence", "confidence": "High"}}{_fb}"""

    try:
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            timeout=20,
        )
        data = json.loads(resp.choices[0].message.content)
        d = (data.get("differentiation") or "Medium").strip().capitalize()
        if d not in ("High", "Medium", "Low"):
            d = "Medium"
        conf = (data.get("confidence") or "Medium").strip().capitalize()
        if conf not in ("High", "Medium", "Low"):
            conf = "Medium"
        return d, data.get("reason", ""), conf
    except Exception:
        return "Medium", "Unable to assess", "Low"


# ---------------------------------------------------------------------------
# PRIORITY (NCP investment criteria fit)
# ---------------------------------------------------------------------------
def assess_priority(client, company_name, description, state, employees, keywords, niche,
                    ebitda_estimate=None, feedback_history=None):
    """Rate acquisition priority (High/Medium/Low) for Strategy A + confidence."""
    _fb = _format_feedback_section(feedback_history)
    ebitda_line = ""
    if ebitda_estimate and ebitda_estimate != "Unknown":
        ebitda_line = f"\nEstimated EBITDA (industry-adjusted): {ebitda_estimate}"

    prompt = f"""You are prioritizing acquisition targets for New Capital Partners (NCP).

NCP's investment criteria:
1. Founder-owned (not public, not PE/VC backed) — already pre-filtered, assume satisfied
2. US-based, preferably eastern US (Denver/Colorado and east)
3. $2M–$4M EBITDA target range
4. Niche, high-growth (>10%) markets with limited competitors
5. Focus sectors: tech-enabled healthcare, financial services, governance risk & compliance (GRC)

Company: "{company_name}"
Description: "{description}"
State: "{state}"
Estimated employees: {employees}{ebitda_line}
Keywords: {keywords}
Search niche: "{niche}"

Eastern US states (preferred): AL, AR, CO, CT, DC, DE, FL, GA, IA, IL, IN, KS, KY,
LA, MA, MD, ME, MI, MN, MO, MS, NC, ND, NE, NH, NJ, NY, OH, OK, PA, RI, SC, SD,
TN, TX, VA, VT, WI, WV

Scoring guidance:
- HIGH: Strong fit on geography (eastern US), EBITDA in or near $2M-$4M range, AND sector
  alignment with NCP's focus areas (tech-enabled healthcare, financial services, or GRC)
- MEDIUM: Fits most criteria but has one notable weakness
- LOW: Matches the search niche but weak fit on multiple NCP criteria

Also rate your CONFIDENCE in this assessment:
- HIGH confidence: Clear data on geography, size, and sector alignment.
- MEDIUM confidence: Some criteria are uncertain or data is incomplete.
- LOW confidence: Major gaps in data (no state, unclear sector, unknown size).

Return JSON only: {{"priority": "High", "reason": "one sentence", "confidence": "High"}}{_fb}"""

    try:
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            timeout=20,
        )
        data = json.loads(resp.choices[0].message.content)
        p = (data.get("priority") or "Medium").strip().capitalize()
        if p not in ("High", "Medium", "Low"):
            p = "Medium"
        conf = (data.get("confidence") or "Medium").strip().capitalize()
        if conf not in ("High", "Medium", "Low"):
            conf = "Medium"
        return p, data.get("reason", ""), conf
    except Exception:
        pass

    # Fallback rule-based scoring
    score = 0
    st_code = (state or "").strip().lower()
    if st_code in EASTERN_US_STATES:
        score += 2
    emp = employees or 0
    if 20 <= emp <= 80:
        score += 3
    elif 10 <= emp <= 150:
        score += 1
    if score >= 4:
        return "High", "Good geography and size fit", "Medium"
    elif score >= 2:
        return "Medium", "Partial criteria match", "Medium"
    return "Low", "Weak criteria match", "Medium"


# ---------------------------------------------------------------------------
# GROWTH SCORE
# ---------------------------------------------------------------------------
def assess_growth_score(client, firecrawl_scrape_fn, company_name, domain,
                        apollo_people, linkedin_url=None, feedback_history=None):
    """Rate growth trajectory (High/Medium/Low) from hiring, Google, LinkedIn + confidence."""
    signals = []

    if domain:
        job_urls = [
            f"https://{domain}{p}" for p in
            ["/careers", "/jobs", "/join-us", "/work-with-us",
             "/career-opportunities", "/open-positions"]
        ]
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as ex:
            futs = {ex.submit(firecrawl_scrape_fn, u): u for u in job_urls[:3]}
            for f in concurrent.futures.as_completed(futs):
                content = f.result()
                if content and len(content) >= 200:
                    signals.append(f"CAREERS PAGE ({futs[f]}):\n{content[:6000]}")
        if not signals:
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as ex:
                futs = {ex.submit(firecrawl_scrape_fn, u): u for u in job_urls[3:]}
                for f in concurrent.futures.as_completed(futs):
                    content = f.result()
                    if content and len(content) >= 200:
                        signals.append(f"CAREERS PAGE ({futs[f]}):\n{content[:6000]}")

    from urllib.parse import quote as _url_quote
    _growth_queries = [
        f"{company_name} hiring jobs",
        f"{company_name} expansion new location",
    ]
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
        _gfuts = {
            ex.submit(firecrawl_scrape_fn,
                      f"https://www.google.com/search?q={_url_quote(q)}"): q
            for q in _growth_queries
        }
        for f in concurrent.futures.as_completed(_gfuts):
            try:
                content = f.result()
                if content and len(content) >= 100:
                    signals.append(f"GOOGLE ({_gfuts[f]}):\n{content[:5000]}")
            except Exception:
                pass

    if linkedin_url:
        try:
            li_content = firecrawl_scrape_fn(linkedin_url)
            if li_content and len(li_content) >= 200:
                signals.append(f"LINKEDIN COMPANY PAGE:\n{li_content[:6000]}")
        except Exception:
            pass

    if apollo_people:
        team_lines = [
            f"- {p.get('first_name', '')} {p.get('last_name', '')} — {p.get('title', '')}"
            for p in apollo_people[:20]
        ]
        signals.append(f"CURRENT TEAM ({len(apollo_people)} found):\n" + "\n".join(team_lines))

    prompt = f"""Assess the growth trajectory of "{company_name}" based on these signals.

Indicators of growth:
- Open job postings (more = growing; especially operational/clinical roles)
- Team size increases or many recent hires (LinkedIn headcount trends)
- Expansion language (new locations, new services, new markets)
- Hiring for roles that indicate scaling (operations, sales, regional managers)
- Google search results showing recent hiring or expansion news
- LinkedIn page showing employee growth trends or recent headcount increases

Available data:
{chr(10).join(signals) if signals else "(no careers page or Google results found; limited data)"}

Rating guidance:
- HIGH: Clear evidence of active hiring (3+ open positions), explicit expansion plans,
  or LinkedIn/Google showing recent growth news
- MEDIUM: Some hiring activity or moderate growth signals (1-2 open roles, growing team,
  some expansion mentions in search results)
- LOW: No evidence of active hiring or growth; appears stable or contracting

Also rate your CONFIDENCE in this assessment:
- HIGH confidence: Rich data from careers pages, Google results, and/or LinkedIn showing
  clear hiring or expansion activity.
- MEDIUM confidence: Some data available but incomplete (e.g., only Google results, or only
  team roster without careers page).
- LOW confidence: Very limited data — no careers page found, minimal Google results.

Return JSON only: {{"growth_score": "High", "reason": "one sentence", "confidence": "High"}}{_format_feedback_section(feedback_history)}"""

    try:
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            timeout=20,
        )
        data = json.loads(resp.choices[0].message.content)
        g = (data.get("growth_score") or "Low").strip().capitalize()
        if g not in ("High", "Medium", "Low"):
            g = "Low"
        conf = (data.get("confidence") or "Low").strip().capitalize()
        if conf not in ("High", "Medium", "Low"):
            conf = "Low"
        return g, data.get("reason", ""), conf
    except Exception:
        pass
    return "Low", "Unable to assess", "Low"


# ---------------------------------------------------------------------------
# TRANSACTION READINESS
# ---------------------------------------------------------------------------
def assess_transaction_readiness(
    client,
    firecrawl_scrape_fn,
    company_name,
    domain,
    apollo_people,
    description,
    feedback_history=None,
):
    """Rate transaction readiness (High/Medium/Low) from founder/CFO/strategic signals."""
    signals = []

    owner_person = None
    for p in (apollo_people or []):
        title = (p.get("title") or "").lower()
        if any(t in title for t in [
            "owner", "founder", "co-founder", "ceo",
            "president", "managing partner", "managing member",
        ]):
            owner_person = p
            break

    if owner_person:
        li_url = owner_person.get("linkedin_url")
        if li_url:
            content = firecrawl_scrape_fn(li_url)
            if content and len(content) >= 200:
                signals.append(
                    f"FOUNDER LINKEDIN ({owner_person.get('first_name', '')} "
                    f"{owner_person.get('last_name', '')}):\n{content[:8000]}"
                )
        signals.append(
            f"FOUNDER: {owner_person.get('first_name', '')} "
            f"{owner_person.get('last_name', '')} — {owner_person.get('title', '')}"
        )

    for p in (apollo_people or []):
        title = (p.get("title") or "").lower()
        if any(t in title for t in [
            "cfo", "chief financial", "vp finance", "vp of finance",
            "vice president of finance", "vice president, finance",
        ]):
            signals.append(
                f"CFO/FINANCE LEADER PRESENT: {p.get('first_name', '')} "
                f"{p.get('last_name', '')} — {p.get('title', '')}"
            )

    if domain:
        content = firecrawl_scrape_fn(f"https://{domain}")
        if content and len(content) >= 200:
            signals.append(f"HOMEPAGE:\n{content[:5000]}")

    prompt = f"""Assess how likely "{company_name}" is to be open to a sale or investment
in the near term.

Key transaction-readiness signals (in order of importance):
1. Founder/owner appears to be 60+ years old (look at college graduation dates,
   career start dates, years of experience — a 1985 grad is ~62 in 2026)
2. Recently hired a CFO or VP of Finance for the first time (classic pre-sale move)
3. Language suggesting "exploring strategic options", "next chapter", succession planning
4. Long founder tenure (25+ years running the business suggests approaching retirement)
5. No clear next-generation successor visible in the leadership team

Company description: "{description}"

Available data:
{chr(10).join(signals) if signals else "(limited data available)"}

Rating guidance:
- HIGH: Strong evidence of 60+ founder age OR recent CFO/finance hire with no prior one,
  OR explicit succession/strategic language
- MEDIUM: Some signals (long-tenured founder, CFO present but unclear if recent,
  founder age plausibly 55-65 but uncertain)
- LOW: Young founder, no CFO hire signal, no transition indicators, or insufficient data

Also rate your CONFIDENCE in this assessment:
- HIGH confidence: LinkedIn profile with graduation dates, clear career timeline, or explicit
  succession language found.
- MEDIUM confidence: Some founder data available but age/tenure is uncertain.
- LOW confidence: No founder LinkedIn found, no career timeline, guessing from limited data.

Return JSON only: {{"readiness": "High", "reason": "one sentence", "confidence": "High"}}{_format_feedback_section(feedback_history)}"""

    try:
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            timeout=20,
        )
        data = json.loads(resp.choices[0].message.content)
        r = (data.get("readiness") or "Low").strip().capitalize()
        if r not in ("High", "Medium", "Low"):
            r = "Low"
        conf = (data.get("confidence") or "Low").strip().capitalize()
        if conf not in ("High", "Medium", "Low"):
            conf = "Low"
        return r, data.get("reason", ""), conf
    except Exception:
        pass
    return "Low", "Unable to assess", "Low"


# ---------------------------------------------------------------------------
# REVENUE / EBITDA ESTIMATION
# ---------------------------------------------------------------------------
def estimate_revenue_ebitda(employees, apollo_revenue, niche):
    """Estimate EBITDA range from employees + sector benchmarks."""
    hint = (niche or "").lower()
    if any(x in hint for x in [
        "health", "medical", "care", "clinical", "hospice",
        "nursing", "dental", "therapy", "behavioral", "pace",
    ]):
        sector = "healthcare"
    elif any(x in hint for x in [
        "financial", "banking", "insurance", "fintech",
        "wealth", "lending", "payment",
    ]):
        sector = "financial_services"
    elif any(x in hint for x in [
        "software", "saas", "tech", "digital", "platform",
        "cyber", "compliance tech",
    ]):
        sector = "technology"
    elif any(x in hint for x in ["consulting", "advisory", "legal", "accounting"]):
        sector = "professional_services"
    else:
        sector = "default"

    rev = None
    if apollo_revenue and apollo_revenue > 0:
        rev = apollo_revenue
    elif employees and employees > 0:
        rev = employees * REV_PER_EMP[sector]

    if not rev:
        return "Unknown"

    low_m, high_m = EBITDA_MARGINS[sector]
    ebitda_low = rev * low_m
    ebitda_high = rev * high_m

    def _fmt(n):
        if n >= 1_000_000:
            return f"${n / 1_000_000:.1f}M"
        return f"${n / 1_000:.0f}K"

    return f"{_fmt(ebitda_low)}–{_fmt(ebitda_high)}"


# ---------------------------------------------------------------------------
# CONVICTION / EXCITEMENT SCORING
# ---------------------------------------------------------------------------
def score_conviction(client, company_name, description, niche, scores, thesis=None, feedback_history=None):
    """Score conviction on a 1-10 scale using NCP's investment thesis.

    Returns:
        (score: int 1-10, pitch: str, reasoning: str)
    """
    if thesis is None:
        thesis = _load_thesis()

    feedback_section = ""
    if feedback_history:
        recent = feedback_history[-15:]

        liked, rejected, caveats = [], [], []
        for fb in recent:
            verdict = (fb.get("verdict") or "").lower()
            company = fb.get("company", "?")
            text = fb.get("feedback", "")
            if verdict == "liked":
                liked.append(f"  - {company}: {text}")
            elif verdict == "rejected":
                rejected.append(f"  - {company}: {text}")
            elif verdict == "caveats":
                caveats.append(f"  - {company}: {text}")

        sections = []
        if liked:
            sections.append("THUMBS-UP (find more like these — match their traits):\n" + "\n".join(liked))
        if rejected:
            sections.append("THUMBS-DOWN (avoid traits like these):\n" + "\n".join(rejected))
        if caveats:
            sections.append("SPECIFIC CRITIQUES (calibrate scoring against these):\n" + "\n".join(caveats))

        if sections:
            feedback_section = f"""

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CALIBRATION FROM PAST FEEDBACK (heavily weight these patterns):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
{chr(10) + chr(10).join(sections)}

Apply these lessons directly to THIS company:
- If this company shares traits with the THUMBS-DOWN list, score it lower.
- If this company shares traits with the THUMBS-UP list, score it higher.
- If a CRITIQUE applies (e.g., "too big," "no recurring revenue"), penalize for it.
- The user's history is more reliable than any abstract framework. Trust it."""

    prompt = f"""You are a senior associate at {thesis.get('firm', 'a PE firm')}.

Investment mandate: {thesis.get('mandate', '')}

What excites us about a deal:
{chr(10).join(f'- {s}' for s in thesis.get('excitement_signals', []))}

Deal breakers:
{chr(10).join(f'- {s}' for s in thesis.get('deal_breakers', []))}

Our conviction bar: {thesis.get('conviction_bar', '')}{feedback_section}

---

Evaluate this company:

Company: "{company_name}"
Description: "{description}"
Search niche: "{niche}"
Differentiation: {scores.get('Differentiated', 'Unknown')}
Priority: {scores.get('Priority', 'Unknown')}
Growth: {scores.get('Growth', 'Unknown')}
Txn Readiness: {scores.get('Txn Readiness', 'Unknown')}

Score your conviction from 1 to 10:
- 1-3: Not worth pursuing. Generic operator, no clear edge.
- 4-5: Interesting but nothing special. Wouldn't pitch to Trey.
- 6-7: Solid candidate with at least one standout trait.
- 8-9: Genuinely exciting. Clear right to win. Pitch with enthusiasm.
- 10: Exceptional. Rare find.

Write a 2-3 sentence "pitch" as if you're telling Trey why he should look at
this company. Be specific — what is SPECIAL about this one? If conviction is
below 6, be honest about what's missing.

Return JSON only:
{{"conviction": 8, "pitch": "two to three sentences", "reasoning": "one sentence on what drove the score"}}"""

    try:
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            timeout=25,
        )
        data = json.loads(resp.choices[0].message.content)
        score = int(data.get("conviction", 5))
        score = max(1, min(10, score))
        return score, data.get("pitch", ""), data.get("reasoning", "")
    except Exception:
        return 5, "", "Unable to assess conviction"