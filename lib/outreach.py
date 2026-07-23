"""AI-powered outreach email drafting, mailto link, and follow-up calendar generation."""

import json
from datetime import datetime, timedelta, timezone
from urllib.parse import quote
from textwrap import dedent


def draft_cold_email(openai_client, row, thesis, sender_name="Trey"):
    """Draft a personalized cold outreach email that earns a reply from
    a founder or CEO.  Follows the email-prospecting playbook (three
    gates: delivered / opened / converted; Hook-Relate-Bridge-Ask arc)
    and varies the angle so emails never feel templated.

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
STRUCTURE — every email follows this 4-beat arc (whatever the angle):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
1. HOOK — the subject line + first sentence earn the open and the read.
   The first sentence is the "glimpse factor": it shows in the inbox
   preview, so it must grab on its own. Never open with "Hi/Hello/Dear".
2. RELATE — one line that shows you get THEIR world and its weight.
   Founders are people, not targets: running the company is stressful,
   personal, and all-consuming. Empathy and authenticity, not a pitch.
3. BRIDGE — connect what you noticed to why it matters FOR THEM.
   Answer "what's in it for me?" — the value of replying must beat the
   cost of their time. Their reasons, not yours: 95% of the time they
   are thinking about themselves. The whole email is about them.
4. ASK — one crystal-clear, easy action. Two proven forms:
   a) Ultra-low-friction: "worth a quick call?"
   b) Assumptive: name a specific slot — "how about Thursday at 3?"
      It takes the decision burden off them.
   Pair either with disarming honesty ("no idea if we'd even be a fit")
   — admitting you're not sure PULLS founders in; hard pitching pushes
   them away. This works in ANY angle, not just Angle E.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SUBJECT LINE:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
1-6 words, under 50 characters — most founders read email on a phone,
where long subject lines die. All lowercase. Must NOT look like a sales email.
NEVER use a question mark in the subject — question subjects kill opens.
Use statements, their name/company, or a genuine specific compliment.
Best forms:
  - just their first name or company: "{first_name.lower() if first_name else 'quick question'}", "{company.lower().split()[0] if company else 'hi'}"
  - a directive statement about their world: "the hardest job in [their niche]"
  - a genuine compliment tied to something real: "what you built in {(city or 'your market').lower()}"
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
- No exclamation marks. Maximum one question mark (in the body only).
- DELIVERABILITY — this email must actually reach the inbox:
  - Plain text only. No links, no images, no attachments, no formatting
    — all three trip spam filters and read as spam behavior.
  - No ALL CAPS words anywhere.
  - No spam-trigger words: free, guarantee, act now, save, cash,
    limited time, opportunity of a lifetime.
  - This is one person, one email — it must read like it could only
    have been written to them, never like a blast.
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

GREAT (full 4-beat arc — hook, relate, bridge, assumptive ask):
"subject: the hardest seat at {company.lower().split()[0] if company else 'the company'}

{first_name or 'You'}, founders in [their niche] tell me the last few years
have made running a company like {company} heavier than ever — [specific
pressure from their world]. What you've built through that is exactly
what we look for.

No idea if we'd even be a fit, but worth 15 minutes to find out?
How about Thursday at 3?

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


RECURRENCE_OPTIONS = {
    "One-time": None,
    "Daily": "FREQ=DAILY",
    "Weekly": "FREQ=WEEKLY",
    "Every 2 weeks": "FREQ=WEEKLY;INTERVAL=2",
    "Monthly": "FREQ=MONTHLY",
}


def generate_custom_reminder_ics(company, action_type, start_dt,
                                 duration_minutes=15, contact_name="",
                                 phone="", email="", notes="",
                                 recurrence="One-time", occurrences=1):
    """Generate a single tailored Outlook reminder, optionally recurring.

    Args:
        company: Company name.
        action_type: "Call", "Email", "LinkedIn", or "Text".
        start_dt: datetime for the event start (naive = user's local time).
        duration_minutes: event length.
        contact_name/phone/email: contact details for the description.
        notes: free-text appended to the description.
        recurrence: key from RECURRENCE_OPTIONS.
        occurrences: how many times a recurring event fires (COUNT).

    Returns the .ics content as a string.
    """
    def _fmt(dt):
        return dt.strftime("%Y%m%dT%H%M%S")

    end_dt = start_dt + timedelta(minutes=duration_minutes)
    safe = company.replace(" ", "").replace("'", "")[:20]
    uid = f"ncp-{safe}-{_fmt(start_dt)}-{action_type.lower()}@ncpengine"

    contact_bits = []
    if contact_name:
        contact_bits.append(f"Contact: {contact_name}")
    if phone:
        contact_bits.append(f"Phone: {phone}")
    if email:
        contact_bits.append(f"Email: {email}")
    desc = f"{action_type} outreach for {company}."
    if contact_bits:
        desc += "\\n" + "\\n".join(contact_bits)
    if notes:
        desc += f"\\nNotes: {notes}"

    summary = f"{action_type}: {contact_name or 'contact'} at {company}"

    lines = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//NCP//Deal Tracker//EN",
        "METHOD:PUBLISH",
        "BEGIN:VEVENT",
        f"UID:{uid}",
        f"DTSTART:{_fmt(start_dt)}",
        f"DTEND:{_fmt(end_dt)}",
        f"SUMMARY:{summary}",
        f"DESCRIPTION:{desc}",
    ]
    rrule = RECURRENCE_OPTIONS.get(recurrence)
    if rrule and occurrences and occurrences > 1:
        lines.append(f"RRULE:{rrule};COUNT={int(occurrences)}")
    lines += [
        "BEGIN:VALARM",
        "TRIGGER:-PT15M",
        "ACTION:DISPLAY",
        f"DESCRIPTION:{summary} in 15 minutes",
        "END:VALARM",
        "END:VEVENT",
        "END:VCALENDAR",
    ]
    return "\r\n".join(lines)


def generate_followup_ics(company, contact_name, phone="", email="",
                          send_date=None):
    """Generate an .ics file with 6 follow-up reminders timed from click:
      1. +15 min  — Email follow-up
      2. +20 min  — LinkedIn invite & message
      3. +15 hrs  — Phone call
      4. +15h 15m — Email follow-up
      5. +36 hrs  — Email follow-up
      6. +48 hrs  — Phone call

    All times are relative to when the user clicks Add Reminders.
    Returns the .ics content as a string.
    """
    if send_date is None:
        send_date = datetime.now(timezone.utc)

    def _fmt(dt):
        return dt.strftime("%Y%m%dT%H%M%S")

    def _uid(suffix):
        ts = int(send_date.timestamp())
        safe = company.replace(" ", "").replace("'", "")[:20]
        return f"ncp-{safe}-{ts}-{suffix}@ncpengine"

    phone_note = f"\\nPhone: {phone}" if phone else ""
    email_note = f"\\nEmail: {email}" if email else ""
    sent_str = send_date.strftime('%b %d')

    events = [
        {
            "uid": _uid("email15m"),
            "offset": timedelta(minutes=15),
            "summary": f"Email follow-up: {contact_name} at {company}",
            "desc": f"Send follow-up email to {contact_name} at {company}. Reference your initial outreach from {sent_str}.{email_note}",
            "alarm_desc": f"Email follow-up with {contact_name} in 5 minutes",
        },
        {
            "uid": _uid("linkedin20m"),
            "offset": timedelta(minutes=20),
            "summary": f"LinkedIn invite & message: {contact_name} at {company}",
            "desc": f"Send LinkedIn connection request and message to {contact_name} at {company}. Reference your outreach from {sent_str}.{email_note}",
            "alarm_desc": f"LinkedIn outreach to {contact_name} in 5 minutes",
        },
        {
            "uid": _uid("call15h"),
            "offset": timedelta(hours=15),
            "summary": f"Follow-up call: {contact_name} at {company}{f' — {phone}' if phone else ''}",
            "desc": f"Call {contact_name} at {company} to follow up on outreach sent {sent_str}.{phone_note}{email_note}",
            "alarm_desc": f"Follow-up call with {contact_name} in 5 minutes",
        },
        {
            "uid": _uid("email15h15m"),
            "offset": timedelta(hours=15, minutes=15),
            "summary": f"Email follow-up #2: {contact_name} at {company}",
            "desc": f"Send follow-up email to {contact_name} at {company} after your call. Reference outreach from {sent_str}.{email_note}",
            "alarm_desc": f"Email follow-up with {contact_name} in 5 minutes",
        },
        {
            "uid": _uid("email36h"),
            "offset": timedelta(hours=36),
            "summary": f"Email follow-up #3: {contact_name} at {company}",
            "desc": f"Send another follow-up email to {contact_name} at {company}. Try a different angle. Original outreach sent {sent_str}.{email_note}",
            "alarm_desc": f"Email follow-up with {contact_name} in 5 minutes",
        },
        {
            "uid": _uid("call48h"),
            "offset": timedelta(hours=48),
            "summary": f"Follow-up call #2: {contact_name} at {company}{f' — {phone}' if phone else ''}",
            "desc": f"Second follow-up call to {contact_name} at {company}. Original outreach sent {sent_str}.{phone_note}{email_note}",
            "alarm_desc": f"Follow-up call with {contact_name} in 5 minutes",
        },
    ]

    lines = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//NCP//Sourcing Engine//EN",
        "METHOD:PUBLISH",
    ]

    for ev in events:
        start = send_date + ev["offset"]
        end = start + timedelta(minutes=15)
        lines += [
            "BEGIN:VEVENT",
            f"UID:{ev['uid']}",
            f"DTSTART:{_fmt(start)}",
            f"DTEND:{_fmt(end)}",
            f"SUMMARY:{ev['summary']}",
            f"DESCRIPTION:{ev['desc']}",
            "BEGIN:VALARM",
            "TRIGGER:-PT5M",
            "ACTION:DISPLAY",
            f"DESCRIPTION:{ev['alarm_desc']}",
            "END:VALARM",
            "END:VEVENT",
        ]

    lines.append("END:VCALENDAR")
    return "\r\n".join(lines)
