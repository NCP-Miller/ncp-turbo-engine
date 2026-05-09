"""AI-powered outreach email drafting and mailto link generation."""

import json
from urllib.parse import quote


def draft_cold_email(openai_client, row, thesis, sender_name="Trey"):
    """Draft a personalized cold outreach email for a sourcing target.

    Args:
        openai_client: OpenAI client instance.
        row: Dict with company data from the sourcing results table.
        thesis: Dict loaded from ncp_thesis.json.
        sender_name: Name to sign the email with.

    Returns:
        Dict with keys: subject, body
    """
    company = row.get("Company", "the company")
    contact = row.get("CEO/Owner Name", "")
    title = row.get("Title", "")
    description = row.get("Description", "")
    city = row.get("City", "")
    state = row.get("State", "")
    employees = row.get("Employees", "")
    niche = row.get("_niche", "")

    first_name = contact.split()[0] if contact and contact != "N/A" else ""

    prompt = f"""You are drafting a cold outreach email from {sender_name} at New Capital Partners (NCP),
a lower middle market private equity firm. NCP acquires founder-owned service businesses.

Write a short, warm, professional email to the owner/CEO of this company.
The goal is to introduce NCP and express interest in learning more about their business — NOT to make an offer.
Keep it conversational and genuine. 2-3 short paragraphs max.

COMPANY INFO:
- Company: {company}
- Contact: {contact} ({title})
- Location: {city}, {state}
- Employees: {employees}
- Description: {description}

TONE GUIDELINES:
- Founder-friendly, respectful of what they've built
- Brief — busy operators won't read a wall of text
- No jargon like "synergies" or "value creation"
- Don't mention specific deal terms, valuation, or EBITDA
- Express genuine curiosity about their business
- Mention something specific about their company (from the description) so it doesn't feel templated
- End with a soft ask: "Would you be open to a brief call?"

Return JSON only:
{{"subject": "short email subject line", "body": "the full email body"}}"""

    resp = openai_client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}],
        response_format={"type": "json_object"},
        temperature=0.7,
        timeout=30,
    )
    result = json.loads(resp.choices[0].message.content)
    return {
        "subject": result.get("subject", f"Introduction — New Capital Partners"),
        "body": result.get("body", ""),
    }


def make_mailto_url(to_email, subject, body):
    """Generate a mailto: URL that opens Outlook (or default mail client).

    Args:
        to_email: Recipient email address.
        subject: Email subject line.
        body: Email body text.

    Returns:
        mailto: URL string.
    """
    params = []
    if subject:
        params.append(f"subject={quote(subject)}")
    if body:
        params.append(f"body={quote(body)}")
    query = "&".join(params)
    addr = to_email or ""
    return f"mailto:{addr}?{query}" if query else f"mailto:{addr}"
