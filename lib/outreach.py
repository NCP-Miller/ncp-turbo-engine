"""AI-powered outreach email drafting, mailto link, and follow-up calendar generation."""

import json
from datetime import datetime, timedelta, timezone
from urllib.parse import quote
from textwrap import dedent


def draft_cold_email(openai_client, row, thesis, sender_name="Trey"):
    """Draft a personalized cold outreach email that earns a reply from
    a founder or CEO.  Uses proven cold-email psychology (Blount, Braun,
    McKenna) but varies the angle so emails never feel templated.

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

    excitement = thesis.get("excitement_signals", [])
    excitement_text = "\n".join(f"  - {s}" for s in excitement) if excitement else "  (none provided)"
    deal_breakers = thesis.get("deal_breakers", [])
    breakers_text = "\n".join(f"  - {s}" for s in deal_breakers) if deal_breakers else "  (none provided)"
    conviction_bar = thesis.get("conviction_bar", "")
    firm_mandate = thesis.get("mandate", "lower middle market PE — founder-owned service businesses")

    prompt = f"""You are {sender_name}, a principal at New Capital Partners. You
personally invest alongside founders of service businesses. Your job: write ONE
cold email to {first_name or 'this founder'} that earns a reply.

ABOUT THE RECIPIENT:
- Company: {company}
- Contact: {contact} ({title})
- Location: {city}, {state}
- Employees: {employees}
- Description: {description}
- Website: {website}
- Differentiation: {differentiated}

YOUR INVESTMENT ANGLE:
- Mandate: {firm_mandate}
- What excites you about a deal:
{excitement_text}
- What you avoid:
{breakers_text}
- Conviction bar: {conviction_bar}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
UNDERSTAND THE FOUNDER'S MINDSET:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
This person built this company from nothing. They are proud, protective, and
skeptical of outside investors. They get generic PE emails regularly and delete
them. They will ONLY reply if:
  1. You clearly understand their specific business (not just their industry)
  2. You say something that makes them think "huh, this person actually gets it"
  3. The ask is so small it feels riskless to respond

Your email must pass the "would I reply to this?" test from a busy founder
who has heard every PE pitch in the book.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CHOOSE ONE ANGLE (pick the one that fits this company best):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

ANGLE A — "I noticed something specific"
Lead with a detail only someone who actually researched them would know.
Pull from their description, website, differentiation, or geography.
Then connect it to why you're reaching out in one sentence. Close casually.

ANGLE B — "Industry insight"
Open with something happening in their industry right now — a trend, a
shift, a regulatory change, a wave of consolidation — that affects them.
Position yourself as someone tracking this space, not selling. Ask if
they're seeing the same thing.

ANGLE C — "Founder-to-founder respect"
Acknowledge what they've built is rare or hard to replicate. Be specific
about WHAT is impressive (their moat, their longevity, their model). Then
ask a genuine question about their business or plans.

ANGLE D — "The mutual connection frame"
Reference that you've been studying companies in their niche in their
region. Name the niche specifically. Say you keep hearing good things or
their name keeps surfacing. Ask if they'd be open to connecting.

ANGLE E — "The disarming honesty"
Be upfront — you invest in companies like theirs, you're not sure if
the timing is right for them, but something about their business caught
your attention. Ask if it's even worth a conversation. The honesty
itself is the pattern interrupt.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SUBJECT LINE:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
1-3 words. All lowercase. No punctuation. Must NOT look like a sales email.
Best performers: just their first name, their company name, or a 2-word hook.
Examples: "{first_name.lower() if first_name else 'quick question'}", "{company.lower().split()[0] if company else 'hi'}", "curious about something", "quick question"
NEVER: "Partnership Opportunity", "Introduction", "Connecting", "Business Inquiry"

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
WRITING RULES:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- 3-5 sentences. 40-75 words in the body (excluding sign-off).
- 5th grade reading level. Short words. Short sentences.
- Write like a text message from a smart friend, not a business letter.
- First word of the email must be "you", their name, or their company name.
  NEVER open with "I".
- The specific detail about THEIR company is the most important sentence.
  If you can't point to something concrete from the company info above,
  the email will fail. Generic = delete.
- One question maximum. Make it easy to answer.
- No "I hope this finds you well" or any filler opener.
- No "My name is..." — your name is in the sign-off.
- No jargon: synergies, value creation, strategic partnership, unlock,
  deal flow, portfolio company, platform, scalable, leverage.
- No mention of deal terms, valuation, EBITDA, multiples, or price.
- No exclamation marks. Maximum one question mark.
- Plain text. No formatting, bullets, or links.
- Sign off with just "{sender_name}" — no title, phone, or tagline.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
EXAMPLES OF GREAT vs TERRIBLE EMAILS:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

GREAT (specific, human, earns curiosity):
"subject: {first_name.lower() if first_name else 'quick question'}

{first_name or company}, your team has built something unusual in [specific
detail from their business]. We've been tracking [their niche] in [their
region] and your name keeps coming up.

Not sure if you'd ever think about a growth partner, but would a quick
call be worth it?

{sender_name}"

GREAT (industry insight):
"subject: {company.lower().split()[0] if company else 'curious'}

{company} sits right in the middle of [specific industry trend]. We back
founders in this space and I'm curious how you're thinking about the next
few years.

Would it be worth 10 minutes to compare notes?

{sender_name}"

TERRIBLE (generic, self-centered, instant delete):
"subject: Partnership Opportunity

Dear {first_name or 'Sir/Madam'},

I came across your company and was impressed by your growth. We are a
private equity firm that partners with founder-owned businesses to unlock
their full potential. We have a proven track record of creating value for
our portfolio companies.

I would love to schedule a 30-minute call to discuss how we might work
together. Would next Tuesday work?

Best regards,
{sender_name}
Managing Director, New Capital Partners"

The terrible example does everything wrong: generic subject, "I" opener,
no specifics about THEIR company, jargon-filled, long, mentions PE, asks
for too much time, formal sign-off. Every founder has seen this exact
email 100 times. Do the OPPOSITE.

Return JSON only:
{{"subject": "1-3 word lowercase subject", "body": "the complete email body including sign-off", "angle": "A/B/C/D/E"}}"""

    resp = openai_client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}],
        response_format={"type": "json_object"},
        temperature=0.9,
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
