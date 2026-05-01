"""Email guessing from sample emails found on company website.

Detects the naming pattern used at a company and applies it to a target person.
"""

import json
import re
import concurrent.futures

from lib.constants import OPENAI_MODEL


GENERIC_EMAIL_PREFIXES = {
    "info", "contact", "admin", "support", "hello", "sales",
    "office", "help", "hr", "jobs", "careers", "billing",
    "noreply", "no-reply", "webmaster", "marketing",
}


def guess_email(client, firecrawl_scrape_fn, first_name, last_name, domain, company_name):
    """Detect the company's email naming pattern and apply it to a target person.

    Args:
        client: OpenAI client.
        firecrawl_scrape_fn: Callable(url) -> str (markdown content).
        first_name, last_name, domain: target person + company domain.
        company_name: used for context only.

    Returns:
        (guessed_email, pattern_source) or (None, None) if nothing usable found.
    """
    if not first_name or not last_name or not domain:
        return None, None

    first = first_name.strip().split()[0].lower()
    last = last_name.strip().split()[-1].lower()

    # Collect sample emails from common pages
    sample_emails = set()
    scrape_urls = [
        f"https://{domain}/contact",
        f"https://{domain}/contact-us",
        f"https://{domain}/about",
        f"https://{domain}/about-us",
        f"https://{domain}/team",
        f"https://{domain}/leadership",
    ]
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as ex:
        futs = {ex.submit(firecrawl_scrape_fn, u): u for u in scrape_urls[:3]}
        for f in concurrent.futures.as_completed(futs):
            content = f.result()
            if content:
                found = re.findall(r"[a-zA-Z0-9._%+-]+@" + re.escape(domain), content)
                sample_emails.update(e.lower() for e in found)
    if len(sample_emails) < 2:
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as ex:
            futs = {ex.submit(firecrawl_scrape_fn, u): u for u in scrape_urls[3:]}
            for f in concurrent.futures.as_completed(futs):
                content = f.result()
                if content:
                    found = re.findall(r"[a-zA-Z0-9._%+-]+@" + re.escape(domain), content)
                    sample_emails.update(e.lower() for e in found)

    # Try a Google search if nothing on the site
    if len(sample_emails) < 2:
        search_content = firecrawl_scrape_fn(
            f"https://www.google.com/search?q=%22%40{domain}%22+email&num=10"
        )
        if search_content:
            found = re.findall(r"[a-zA-Z0-9._%+-]+@" + re.escape(domain), search_content)
            sample_emails.update(e.lower() for e in found)

    personal_emails = [
        e for e in sample_emails if e.split("@")[0] not in GENERIC_EMAIL_PREFIXES
    ]

    if not personal_emails and not sample_emails:
        return None, None

    prompt = f"""You found these email addresses from {domain}:
{json.dumps(list(sample_emails))}

Of these, the personal (non-generic) ones are:
{json.dumps(personal_emails) if personal_emails else "(none identified)"}

Detect the email naming pattern used at this company (e.g., first.last@, flast@,
firstl@, first@, first_last@, etc.).

Then apply that SAME pattern to generate an email for:
  First name: "{first_name}"
  Last name: "{last_name}"

Rules:
- Use lowercase only
- If multiple patterns exist, use the most common one
- If you can identify a clear pattern from even ONE personal email, use it
- If only generic emails exist (info@, contact@), try the most common convention: first.last@{domain}

Return JSON only:
{{"pattern": "description of pattern detected",
  "sample_used": "the example email you based it on",
  "guessed_email": "result@{domain}"}}"""

    try:
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            timeout=20,
        )
        data = json.loads(resp.choices[0].message.content)
        guessed = (data.get("guessed_email") or "").strip().lower()
        pattern = data.get("pattern") or ""
        sample = data.get("sample_used") or ""

        if guessed and f"@{domain}" in guessed and re.match(r"^[a-z0-9._%+-]+@", guessed):
            source = f"Pattern: {pattern}"
            if sample:
                source += f" (from {sample})"
            return guessed, source
    except Exception:
        pass

    # Fallback: if we found at least one personal email, use first.last
    if personal_emails:
        sample = personal_emails[0]
        guess = f"{first}.{last}@{domain}"
        return guess, f"Fallback: first.last (sample found: {sample})"

    return None, None