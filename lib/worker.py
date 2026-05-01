"""Per-company processing pipeline.

Filters one Apollo organization, finds leadership contact, generates a
description, scores it across multiple dimensions, and returns an
enriched row dict ready for display/export.

All API clients/keys are passed in. No Streamlit, no globals.
"""

import concurrent.futures

from lib.filters import (
    is_buyable_structure,
    is_obvious_mismatch,
    check_relevance_gpt4o,
    check_pe_vc_web,
    check_news_for_pe_vc,
)
from lib.contacts import (
    clean_domain,
    firecrawl_scrape,
    get_people_apollo_robust,
    spider_for_contact,
    select_best_apollo_contact,
    repair_single_name,
    bulk_enrich_names,
)
from lib.news import get_latest_news_link
from lib.enrichment import (
    generate_company_description,
    assess_differentiation,
    assess_priority,
    assess_growth_score,
    assess_transaction_readiness,
    estimate_revenue_ebitda,
)
from lib.email_guess import guess_email
from lib.history import company_in_history


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------
def _email_matches_domain(email, company_domain):
    """Return True if the email's domain plausibly belongs to the company domain."""
    if not email or not company_domain:
        return True
    try:
        e_dom = email.split("@")[-1].lower().lstrip("www.")
        c_dom = company_domain.lower().lstrip("www.")
        return (
            e_dom == c_dom
            or e_dom.endswith("." + c_dom)
            or c_dom.endswith("." + e_dom)
        )
    except Exception:
        return True


# ---------------------------------------------------------------------------
# WORKER
# ---------------------------------------------------------------------------
def process_single_company(
    org,
    specific_niche,
    strat_code,
    *,
    openai_client,
    apollo_api_key,
    firecrawl_api_key,
    history_keys=None,
    user_agent=None,
):
    """Filter, enrich, score, and contact-find for a single Apollo organization.

    Args:
        org: Apollo organization dict.
        specific_niche: Target niche string (used for relevance + scoring).
        strat_code: "A" (acquisition), "B" (sales prospects), or "C" (add-on).
        openai_client: OpenAI client (keyword-only).
        apollo_api_key: Apollo API key (keyword-only).
        firecrawl_api_key: Firecrawl API key (keyword-only).
        history_keys: Optional set of {name, domain} keys for cross-run dedup.
        user_agent: Optional User-Agent string.

    Returns:
        Enriched row dict, or None if filtered out.
    """
    comp_name = org.get("name")

    # Curried Firecrawl scraper so downstream functions don't need the key
    def _scrape(url):
        return firecrawl_scrape(firecrawl_api_key, url)

    # Stage 1: structural & obvious-mismatch filters
    if not is_buyable_structure(org, strat_code)[0]:
        return None
    if is_obvious_mismatch(org, specific_niche, strat_code)[0]:
        return None

    # Stage 2: AI relevance check
    desc = org.get("short_description") or org.get("headline") or ""
    tags = org.get("keywords") or []
    if not check_relevance_gpt4o(
        openai_client, comp_name, desc, tags, specific_niche, strat_code
    )[0]:
        return None

    domain = clean_domain(org.get("website_url"))
    org_id = org.get("id")

    # Stage 3: Strategy A web-based PE/VC ownership check
    if strat_code == "A":
        is_pe_vc, _ = check_pe_vc_web(openai_client, _scrape, comp_name, domain)
        if is_pe_vc:
            return None

    # Cross-run dedup flag (informational only — doesn't filter)
    previously_sourced = bool(history_keys and company_in_history(org, history_keys))

    row = {
        "Company":            comp_name,
        "Description":        "",
        "Website":            org.get("website_url"),
        "City":               org.get("city"),
        "State":              org.get("state"),
        "LinkedIn":           org.get("linkedin_url"),
        "Employees":          org.get("estimated_num_employees"),
        "Est. EBITDA":        estimate_revenue_ebitda(
                                  org.get("estimated_num_employees"),
                                  org.get("estimated_annual_revenue"),
                                  specific_niche,
                              ),
        "CEO/Owner Name":     "N/A",
        "Title":              "N/A",
        "Email":              "N/A",
        "Email Estimate":     "",
        "Phone":              "N/A",
        "Source":             "Not Found",
        "Notes":              "",
        "Confidence":         "Low",
        "Latest News":        "N/A",
        "Previously Sourced": "Yes" if previously_sourced else "No",
        "Differentiated":     "Medium",
        "_niche":             specific_niche,  # internal — strip before display
    }
    if strat_code == "A":
        row["Priority"] = "Medium"
        row["Growth"] = "Low"
        row["Txn Readiness"] = "Low"

    # Stage 4: parallel Apollo people search + web spider + description generation
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as inner:
        apollo_future = inner.submit(
            get_people_apollo_robust, apollo_api_key, comp_name, domain, org_id
        )
        web_future = inner.submit(
            spider_for_contact, openai_client, firecrawl_api_key,
            comp_name, domain, user_agent,
        )
        desc_future = inner.submit(
            generate_company_description,
            openai_client, _scrape, comp_name, domain, desc, tags,
        )
        apollo_people = apollo_future.result()
        web_person, web_source, web_email, web_phone = web_future.result()
        row["Description"] = desc_future.result()

    found_person = None

    # Prefer web-discovered person; verify via Apollo enrichment
    if web_person:
        found_person = web_person
        row["Source"] = web_source or "Web Spider"
        row["Confidence"] = "Medium"
        full = (
            f"{found_person.get('first_name', '')} "
            f"{found_person.get('last_name', '')}"
        ).strip()
        if " " not in full:
            rep = repair_single_name(full, apollo_people)
            if rep:
                found_person = rep
                row["Source"] = "Web → Apollo Repaired"
        if domain:
            matches = bulk_enrich_names(apollo_api_key, [found_person], domain)
            if matches and matches[0]:
                enriched = matches[0]
                enr_email = enriched.get("email")
                if not enr_email or _email_matches_domain(enr_email, domain):
                    found_person = enriched
                    row["Source"] += " → Verified"
                    row["Confidence"] = "High"

    # Fallback: best Apollo contact
    if not found_person and apollo_people:
        best, method = select_best_apollo_contact(apollo_people)
        if best:
            found_person = best
            row["Source"] = method
            row["Confidence"] = "Medium"

    if found_person:
        row["CEO/Owner Name"] = (
            f"{found_person.get('first_name', '')} "
            f"{found_person.get('last_name', '')}"
        ).strip()
        row["Title"] = found_person.get("title") or "N/A"
        a_email = found_person.get("email")
        if a_email and domain and not _email_matches_domain(a_email, domain):
            a_email = None
        row["Email"] = a_email if a_email else (web_email or "N/A")
        pnums = found_person.get("phone_numbers") or []
        a_phone = pnums[0].get("sanitized_number") if pnums else None
        row["Phone"] = a_phone if a_phone else (web_phone or "N/A")
        if found_person.get("notes"):
            row["Notes"] = found_person["notes"]
    else:
        if web_email:
            row["Email"] = web_email
        if web_phone:
            row["Phone"] = web_phone
        if web_email or web_phone:
            row["Confidence"] = "Medium"
            row["Source"] = "Web (contact only)"

    # Stage 5: email guess (only when we have a name + domain but no verified email)
    if row["Email"] == "N/A" and found_person and domain:
        fn = found_person.get("first_name", "").strip()
        ln = found_person.get("last_name", "").strip()
        if fn and ln:
            guessed, _ = guess_email(openai_client, _scrape, fn, ln, domain, comp_name)
            if guessed:
                row["Email Estimate"] = guessed

    # Stage 6: latest news + news-based PE/VC final gate (Strategy A)
    t, u = get_latest_news_link(comp_name, org.get("city"), user_agent=user_agent)
    if u:
        row["Latest News"] = f"{t} | {u}" if t else u
    if strat_code == "A" and check_news_for_pe_vc(t):
        return None

    # Stage 7: scoring
    if strat_code == "A":
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as score_ex:
            pri_fut = score_ex.submit(
                assess_priority, openai_client, comp_name, row["Description"],
                org.get("state"), org.get("estimated_num_employees"),
                tags, specific_niche,
            )
            growth_fut = score_ex.submit(
                assess_growth_score, openai_client, _scrape, comp_name, domain, apollo_people,
            )
            txn_fut = score_ex.submit(
                assess_transaction_readiness, openai_client, _scrape, comp_name,
                domain, apollo_people, row["Description"],
            )
            diff_fut = score_ex.submit(
                assess_differentiation, openai_client, comp_name,
                row["Description"], specific_niche,
            )
            row["Priority"], _ = pri_fut.result()
            row["Growth"], _ = growth_fut.result()
            row["Txn Readiness"], _ = txn_fut.result()
            row["Differentiated"], _ = diff_fut.result()
    else:
        row["Differentiated"], _ = assess_differentiation(
            openai_client, comp_name, row["Description"], specific_niche,
        )

    return row