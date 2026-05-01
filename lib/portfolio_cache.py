"""PE portfolio cache — maps PE firms to their portfolio companies.

Enables fast, cheap PE-backed detection without web scraping every candidate.
"""

import json
import os
from datetime import datetime, timezone

CACHE_PATH = "pipeline_data/pe_portfolio_cache.json"
PE_FIRMS_PATH = "lib/pe_firms.txt"
CACHE_STALE_DAYS = 30


def load_pe_firms():
    """Read lib/pe_firms.txt, return a list of firm names with whitespace stripped."""
    try:
        with open(PE_FIRMS_PATH, "r", encoding="utf-8") as f:
            return [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        return []


def load_portfolio_cache():
    """Read the portfolio cache JSON. Returns dict with last_updated, firms, all_companies_lower."""
    try:
        with open(CACHE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            raise ValueError("Invalid cache format")
        data.setdefault("last_updated", None)
        data.setdefault("firms", {})
        data.setdefault("all_companies_lower", [])
        return data
    except (FileNotFoundError, json.JSONDecodeError, ValueError):
        return {"last_updated": None, "firms": {}, "all_companies_lower": []}


def cache_age_days():
    """Return number of days since last_updated, or 999 if cache is missing."""
    cache = load_portfolio_cache()
    if not cache["last_updated"]:
        return 999
    try:
        last = datetime.fromisoformat(cache["last_updated"])
        if last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        return (now - last).days
    except (ValueError, TypeError):
        return 999


def is_cache_stale():
    """Return True if cache_age_days() >= CACHE_STALE_DAYS."""
    return cache_age_days() >= CACHE_STALE_DAYS


def refresh_portfolio_cache(openai_client, firecrawl_scrape_fn, log_fn=print):
    """Scrape PE firm portfolio pages and build the company cache.

    Args:
        openai_client: OpenAI client instance.
        firecrawl_scrape_fn: Callable(url) -> str.
        log_fn: Logging function, default print.

    Returns:
        The cache dict after refresh.
    """
    firms = load_pe_firms()
    cache_firms = {}

    for firm in firms:
        try:
            # Ask GPT for the portfolio page URL
            resp = openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": (
                    f"What is the most likely URL of the active portfolio companies page "
                    f"on the website for the PE firm '{firm}'? Respond with ONLY the URL, "
                    f"no explanation. If you don't know, respond with NONE."
                )}],
                timeout=15,
            )
            url = (resp.choices[0].message.content or "").strip()

            if not url or url.upper() == "NONE" or not url.startswith("http"):
                log_fn(f"[Portfolio Cache] {firm}: no URL found")
                continue

            # Scrape the portfolio page
            content = firecrawl_scrape_fn(url)
            if not content or len(content) < 100:
                log_fn(f"[Portfolio Cache] {firm}: scrape returned insufficient content")
                continue

            # Extract company names via GPT
            extract_resp = openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": (
                    f"The following is the text of a private equity firm's portfolio companies page. "
                    f"Extract the names of all current portfolio companies. Return JSON: "
                    f"{{'companies': ['Company A', 'Company B', ...]}}. Return only the JSON, no explanation.\n\n"
                    f"{content[:15000]}"
                )}],
                response_format={"type": "json_object"},
                timeout=20,
            )
            raw = extract_resp.choices[0].message.content or ""
            data = json.loads(raw)
            companies = data.get("companies", [])

            if companies:
                cache_firms[firm] = companies
                log_fn(f"[Portfolio Cache] {firm}: extracted {len(companies)} companies")
            else:
                log_fn(f"[Portfolio Cache] {firm}: no companies extracted")

        except Exception as e:
            log_fn(f"[Portfolio Cache] Could not scrape {firm}: {e}")

    # Build all_companies_lower
    all_lower = list(set(
        c.lower() for companies in cache_firms.values() for c in companies
    ))

    cache = {
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "firms": cache_firms,
        "all_companies_lower": sorted(all_lower),
    }

    # Save
    os.makedirs("pipeline_data", exist_ok=True)
    try:
        with open(CACHE_PATH, "w", encoding="utf-8") as f:
            json.dump(cache, f, indent=2)
    except Exception as e:
        log_fn(f"[Portfolio Cache] Failed to save cache: {e}")

    return cache


def is_pe_backed_via_cache(openai_client, candidate_name):
    """Check if a candidate company is PE-backed using the portfolio cache.

    Returns:
        dict with keys: is_pe_backed, matched_company (optional), matched_firm (optional), method (optional).
    """
    cache = load_portfolio_cache()
    candidate_lower = candidate_name.lower().strip()

    # Fast exact match
    if candidate_lower in cache["all_companies_lower"]:
        # Find which firm owns it
        matched_firm = None
        for firm, companies in cache["firms"].items():
            if candidate_lower in [c.lower() for c in companies]:
                matched_firm = firm
                break
        return {
            "is_pe_backed": True,
            "matched_company": candidate_name,
            "matched_firm": matched_firm,
            "method": "exact",
        }

    # Fuzzy match — find top 50 most similar by substring containment
    all_companies = []
    for firm, companies in cache["firms"].items():
        for c in companies:
            all_companies.append((c, firm))

    # Score by substring overlap
    scored = []
    for company, firm in all_companies:
        c_lower = company.lower()
        # Check substring containment in either direction
        if candidate_lower in c_lower or c_lower in candidate_lower:
            scored.append((company, firm, 100))
        else:
            # Simple character overlap ratio
            common = sum(1 for ch in candidate_lower if ch in c_lower)
            ratio = common / max(len(candidate_lower), len(c_lower), 1)
            scored.append((company, firm, int(ratio * 100)))

    scored.sort(key=lambda x: x[2], reverse=True)
    top_50 = scored[:50]

    if not top_50 or top_50[0][2] < 30:
        return {"is_pe_backed": False}

    candidate_list = [item[0] for item in top_50]

    try:
        resp = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": (
                f"I'm checking if '{candidate_name}' is the same company as any of the following "
                f"PE-backed companies (allowing for renames, abbreviations, and rebrandings): "
                f"{candidate_list}. Return JSON: {{'match': true/false, 'matched_company': 'name or null', "
                f"'confidence': 0-100}}. Only call it a match if confidence >= 70."
            )}],
            response_format={"type": "json_object"},
            timeout=15,
        )
        data = json.loads(resp.choices[0].message.content or "{}")
        if data.get("match") and data.get("confidence", 0) >= 70:
            matched_company = data.get("matched_company")
            # Find the firm
            matched_firm = None
            for company, firm in top_50:
                if company == matched_company:
                    matched_firm = firm
                    break
            return {
                "is_pe_backed": True,
                "matched_company": matched_company,
                "matched_firm": matched_firm,
                "method": "fuzzy",
            }
    except Exception:
        pass

    return {"is_pe_backed": False}
