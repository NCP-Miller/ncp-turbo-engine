"""AI-powered outreach email drafting, mailto link, and follow-up calendar generation."""

import json
from datetime import datetime, timedelta, timezone
from urllib.parse import quote
from textwrap import dedent


def draft_cold_email(openai_client, row, thesis, sender_name="Trey"):
    """Draft a personalized cold outreach email using Jeb Blount's
    fanatical prospecting methodology for maximum open and reply rates.

    Returns dict with keys: subject, body
    """
    company = row.get("Company", "the company")
    contact = row.get("CEO/Owner Name", "")
    title = row.get("Title", "")
    description = row.get("Description", "")
    city = row.get("City", "")
    state = row.get("State", "")
    employees = row.get("Employees", "")
    niche = row.get("_niche", "")
    differentiated = row.get("Differentiated", "")
    website = row.get("Website", "")

    first_name = contact.split()[0] if contact and contact != "N/A" else ""

    # Build buyer-angle context from thesis
    excitement = thesis.get("excitement_signals", [])
    excitement_text = "\n".join(f"  - {s}" for s in excitement) if excitement else "  (none provided)"
    deal_breakers = thesis.get("deal_breakers", [])
    breakers_text = "\n".join(f"  - {s}" for s in deal_breakers) if deal_breakers else "  (none provided)"
    conviction_bar = thesis.get("conviction_bar", "")
    firm_mandate = thesis.get("mandate", "lower middle market PE — founder-owned service businesses")

    prompt = f"""You are {sender_name} at New Capital Partners (NCP). You invest in
founder-owned service businesses. Draft a cold email to get a REPLY from this
founder. That is your ONLY goal — not to sell, pitch, or make an offer.

COMPANY:
- Company: {company}
- Contact: {contact} ({title})
- Location: {city}, {state}
- Employees: {employees}
- Description: {description}
- Website: {website}
- Differentiation: {differentiated}

YOUR BUYER ANGLE (use this to craft WHY you're reaching out to THIS company):
- NCP Mandate: {firm_mandate}
- What excites NCP about a deal:
{excitement_text}
- What NCP avoids:
{breakers_text}
- Conviction bar: {conviction_bar}

Read the company info above and identify which of NCP's excitement signals this
company triggers. Use that SPECIFIC angle in the relevance sentence — don't be
generic. If they have a defensible moat, say that. If they're in a growing
market with tailwinds, reference the tailwind. If they have recurring revenue,
note it. The email should make it obvious you understand WHY their company is
interesting, not just THAT it is.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SUBJECT LINE (1-3 words — data says shorter = more opens):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- 1-3 words ONLY. All lowercase. No title case. No punctuation.
- Use ONLY their first name, company name, or a 2-word curiosity hook.
- Pattern interrupt — it should NOT look like a sales email.
- WINNING examples:
  "{first_name.lower() if first_name else 'quick question'}"
  "{company.lower() if company else 'quick question'}"
  "quick question"
  "curious"
  "{first_name.lower() + ', question' if first_name else 'hi'}"
- NEVER use: "Partnership Opportunity", "Introduction", "Reaching Out",
  "Business Inquiry", or anything that screams sales/PE email.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
EMAIL BODY — THE FRAMEWORK:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Write at a 5th grade reading level. Short words. Short sentences. 3-4 sentences
total. 25-50 words MAXIMUM (excluding sign-off). Every word must earn its place.

The email has exactly 3 parts (1 sentence each):

1. OBSERVATION — "Show me you know me" (Sam McKenna):
   - Start with "you" or their company name — NEVER start with "I".
   - Reference ONE specific thing about THEIR business that caught your eye.
   - Pull from their description, differentiation, location, or website.
   - This must be real and specific — no generic flattery.
   - Good: "{{company}} caught my eye — [specific detail from description]."
   - Bad: "I came across your impressive company." (generic = delete)

2. RELEVANCE — Make it about their world, not yours (Jeb Blount):
   - One sentence connecting who you are to why THEY should care.
   - Frame around their reality as a founder, not your credentials.
   - Use "we" sparingly. Center the sentence on "you" or "founders like you."
   - Good: "We back founders in [niche] who've built something hard to copy."
   - Bad: "We are a PE firm with a proven track record." (nobody cares)

3. MICRO-ASK — Permission-based close (Josh Braun):
   - Ask the SMALLEST possible question. One question only.
   - Frame it so saying "no" feels safe — this paradoxically increases replies.
   - Use "not sure if" / "would it be worth" / "is this even" phrasing.
   - Best closers:
     "Not sure if this is even on your radar — would it be worth a quick chat?"
     "Is this something you'd ever think about?"
     "Would a 10-minute call be worth it, or am I off base?"
     "Happy to share what we're seeing in [niche] — worth a conversation?"
   - NEVER: "Would you be open to a 30-minute call next Tuesday?"

Sign off: just "{sender_name}" — nothing else. No title, phone, or LinkedIn.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ABSOLUTE RULES:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- UNDER 50 WORDS in the body (excluding sign-off). Lavender data: emails under
  50 words get 2x the reply rate of longer emails. Count them.
- 5th grade reading level. No SAT words. Write like you talk.
- NEVER start any sentence with "I" — always lead with "you" or their name.
- No "I hope this finds you well" or any throat-clearing opener.
- No "My name is..." — your name is in the sign-off.
- No jargon: synergies, value creation, strategic partnership, unlock potential,
  deal flow, portfolio company, platform acquisition.
- No mention of deal terms, valuation, EBITDA, multiples, or acquisition price.
- No bullet points, bold, links, or formatting — plain text only.
- No exclamation marks. One question mark max (the ask).
- Do NOT sound like a PE firm. Sound like a real person writing a real email.
- The email should feel like it took 30 seconds to write, even though it didn't.

Return JSON only:
{{"subject": "1-3 word lowercase subject", "body": "the complete email body including sign-off"}}"""

    resp = openai_client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}],
        response_format={"type": "json_object"},
        temperature=0.8,
        timeout=30,
    )
    result = json.loads(resp.choices[0].message.content)
    return {
        "subject": result.get("subject", f"{first_name or company} — quick question"),
        "body": result.get("body", ""),
    }


def make_mailto_url(to_email, subject, body):
    """Generate a mailto: URL that opens Outlook (or default mail client)."""
    params = []
    if subject:
        params.append(f"subject={quote(subject)}")
    if body:
        params.append(f"body={quote(body)}")
    query = "&".join(params)
    addr = to_email or ""
    return f"mailto:{addr}?{query}" if query else f"mailto:{addr}"


def generate_followup_ics(company, contact_name, phone="", email="",
                          send_date=None):
    """Generate an .ics file with two follow-up reminders:
      1. Phone call — 1 day after send_date (9:00 AM, 15 min)
      2. Follow-up email — 3 days after send_date (9:00 AM, 15 min)

    Returns the .ics content as a string.
    """
    if send_date is None:
        send_date = datetime.now(timezone.utc)

    call_date = send_date + timedelta(days=1)
    email_date = send_date + timedelta(days=3)

    def _fmt(dt):
        return dt.strftime("%Y%m%dT%H%M%S")

    def _uid(suffix):
        ts = int(send_date.timestamp())
        safe = company.replace(" ", "").replace("'", "")[:20]
        return f"ncp-{safe}-{ts}-{suffix}@ncpengine"

    call_start = call_date.replace(hour=9, minute=0, second=0, microsecond=0)
    call_end = call_start + timedelta(minutes=15)
    email_start = email_date.replace(hour=9, minute=0, second=0, microsecond=0)
    email_end = email_start + timedelta(minutes=15)

    phone_note = f"\\nPhone: {phone}" if phone else ""
    email_note = f"\\nEmail: {email}" if email else ""

    ics = dedent(f"""\
        BEGIN:VCALENDAR
        VERSION:2.0
        PRODID:-//NCP//Sourcing Engine//EN
        METHOD:PUBLISH
        BEGIN:VEVENT
        UID:{_uid("call")}
        DTSTART:{_fmt(call_start)}
        DTEND:{_fmt(call_end)}
        SUMMARY:Follow-up call: {contact_name} at {company}{f" — {phone}" if phone else ""}
        DESCRIPTION:Call {contact_name} at {company} to follow up on outreach email sent {send_date.strftime('%b %d')}.{phone_note}{email_note}
        BEGIN:VALARM
        TRIGGER:-PT30M
        ACTION:DISPLAY
        DESCRIPTION:Follow-up call with {contact_name} in 30 minutes
        END:VALARM
        END:VEVENT
        BEGIN:VEVENT
        UID:{_uid("email2")}
        DTSTART:{_fmt(email_start)}
        DTEND:{_fmt(email_end)}
        SUMMARY:Follow-up email: {contact_name} at {company}
        DESCRIPTION:Send follow-up email to {contact_name} at {company}. Reference your initial outreach from {send_date.strftime('%b %d')}.{email_note}
        BEGIN:VALARM
        TRIGGER:-PT30M
        ACTION:DISPLAY
        DESCRIPTION:Follow-up email to {contact_name} in 30 minutes
        END:VALARM
        END:VEVENT
        END:VCALENDAR""")
    return ics
