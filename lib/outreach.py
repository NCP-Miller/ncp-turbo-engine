"""AI-powered outreach email drafting and mailto link generation."""

import json
from urllib.parse import quote


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

    prompt = f"""You are {sender_name} at New Capital Partners (NCP), a lower middle market
private equity firm that partners with founder-owned service businesses.

Draft a cold outreach email to the owner/CEO of this company. Your ONLY goal is to
get a reply — not to sell, not to pitch, not to make an offer.

COMPANY INFO:
- Company: {company}
- Contact: {contact} ({title})
- Location: {city}, {state}
- Employees: {employees}
- Description: {description}
- Website: {website}
- Differentiation: {differentiated}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SUBJECT LINE RULES (Jeb Blount method):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- 3-5 words MAX. Shorter = higher open rate
- Pattern interrupt — NOT "Introduction" or "Partnership Opportunity"
- Use their first name or company name to feel personal
- Trigger curiosity without being clickbait
- Examples of great subject lines:
  "{first_name or 'Quick'} — quick question"
  "Noticed {company}"
  "{company} — impressed"
  "Question about {company}"
  "For {first_name}" (ultra-simple, high open rate)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
EMAIL BODY RULES (Jeb Blount / Sales Gravy):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Structure — exactly 3 parts, each 1-2 sentences:

1. HOOK (pattern interrupt + personalized observation):
   - Reference something SPECIFIC you noticed about their company
   - NOT "I hope this finds you well" — that's an instant delete
   - Show you did your homework in ONE sentence
   - Example: "I came across {company} while researching [niche] in [state] —
     [specific thing from description that impressed you]."

2. BRIDGE (why you're reaching out — make it about THEM, not you):
   - Briefly say who you are in a way that's relevant to them
   - Frame it around THEIR world, not yours
   - Do NOT list your credentials, portfolio, or deal history
   - Example: "We partner with founders in [niche] who've built something
     special — and from what I can tell, that's exactly what you've done."

3. ASK (micro-commitment, not a meeting request):
   - Ask for something SMALL — a reply, a thought, a question
   - "Would you be open to a 15-minute call?" is too big
   - Better: "Would it be worth a quick conversation?"
   - Best: "Is this something you'd ever think about?"
   - Or: "Happy to share what we're seeing in [niche] — worth a quick chat?"
   - The smaller the ask, the higher the reply rate

ABSOLUTE DON'TS:
- No "I hope this email finds you well"
- No "My name is Trey and I work at..."
- No jargon: synergies, value creation, strategic partnership, unlock potential
- No mention of deal terms, valuation, EBITDA, or acquisition price
- No long paragraphs — 5 sentences TOTAL for the entire email
- No "We have a proven track record" — nobody cares
- No bullet points or formatting — plain text only
- No exclamation marks
- Sign off with just "{sender_name}" — no title, no phone number, no LinkedIn

THE EMAIL MUST BE UNDER 75 WORDS (excluding signature). Shorter emails get
more replies. Every word must earn its place.

Return JSON only:
{{"subject": "3-5 word subject line", "body": "the complete email body including signature"}}"""

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
