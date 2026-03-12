import concurrent.futures
from filters import is_buyable_structure, is_obvious_mismatch, check_relevance_gpt4o
from contacts import (clean_domain, get_people_apollo_robust, spider_for_contact,
                      select_best_apollo_contact, repair_single_name, bulk_enrich_names)
from news import get_latest_news_link


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------
def _email_matches_domain(email: str, company_domain: str) -> bool:
    """Return True if the email's domain plausibly belongs to the company domain.
    Used to reject Apollo bulk_enrich returning a same-name person at a different company."""
    if not email or not company_domain:
        return True
    try:
        e_dom = email.split("@")[-1].lower().lstrip("www.")
        c_dom = company_domain.lower().lstrip("www.")
        return (e_dom == c_dom
                or e_dom.endswith("." + c_dom)
                or c_dom.endswith("." + e_dom))
    except Exception:
        return True


# ---------------------------------------------------------------------------
# WORKER
# ---------------------------------------------------------------------------
def process_single_company(org, specific_niche, strat_code):
    comp_name = org.get("name")

    if not is_buyable_structure(org, strat_code)[0]:            return None
    if is_obvious_mismatch(org, specific_niche, strat_code)[0]: return None

    desc = org.get("short_description") or org.get("headline") or ""
    tags = org.get("keywords") or []
    if not check_relevance_gpt4o(comp_name, desc, tags, specific_niche, strat_code)[0]:
        return None

    domain = clean_domain(org.get("website_url"))
    org_id = org.get("id")

    row = {
        "Company":        comp_name,
        "Website":        org.get("website_url"),
        "City":           org.get("city"),
        "State":          org.get("state"),
        "LinkedIn":       org.get("linkedin_url"),
        "Employees":      org.get("estimated_num_employees"),
        "CEO/Owner Name": "N/A",
        "Title":          "N/A",
        "Email":          "N/A",
        "Phone":          "N/A",
        "Source":         "Not Found",
        "Notes":          "",
        "Confidence":     "Low",
        "Latest News":    "N/A",
    }

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as inner:
        apollo_future = inner.submit(get_people_apollo_robust, comp_name, domain, org_id)
        web_future    = inner.submit(spider_for_contact, comp_name, domain)
        apollo_people                                 = apollo_future.result()
        web_person, web_source, web_email, web_phone = web_future.result()

    found_person = None

    if web_person:
        found_person      = web_person
        row["Source"]     = web_source or "Web Spider"
        row["Confidence"] = "Medium"   # named person found — upgrade to High if verified
        full = f"{found_person.get('first_name','')} {found_person.get('last_name','')}".strip()
        if " " not in full:
            rep = repair_single_name(full, apollo_people)
            if rep: found_person = rep; row["Source"] = "Web → Apollo Repaired"
        if domain:
            matches = bulk_enrich_names([found_person], domain)
            if matches and matches[0]:
                enriched       = matches[0]
                enr_email      = enriched.get("email")
                # Reject if Apollo matched a same-name person at a different company
                if not enr_email or _email_matches_domain(enr_email, domain):
                    found_person      = enriched
                    row["Source"]    += " → Verified"
                    row["Confidence"] = "High"

    if not found_person and apollo_people:
        best, method = select_best_apollo_contact(apollo_people)
        if best:
            found_person      = best
            row["Source"]     = method
            row["Confidence"] = "Medium"

    if found_person:
        row["CEO/Owner Name"] = (
            f"{found_person.get('first_name','')} "
            f"{found_person.get('last_name','')}").strip()
        row["Title"] = found_person.get("title") or "N/A"
        a_email = found_person.get("email")
        # Discard email if it clearly belongs to a different company's domain
        if a_email and domain and not _email_matches_domain(a_email, domain):
            a_email = None
        row["Email"] = a_email if a_email else (web_email or "N/A")
        pnums   = found_person.get("phone_numbers") or []
        a_phone = pnums[0].get("sanitized_number") if pnums else None
        row["Phone"] = a_phone if a_phone else (web_phone or "N/A")
        if found_person.get("notes"): row["Notes"] = found_person["notes"]
    else:
        if web_email: row["Email"] = web_email
        if web_phone: row["Phone"] = web_phone
        if web_email or web_phone:
            row["Confidence"] = "Medium"
            row["Source"]     = "Web (contact only)"

    t, u = get_latest_news_link(comp_name, org.get("city"))
    if u: row["Latest News"] = f"{t} | {u}" if t else u

    return row
