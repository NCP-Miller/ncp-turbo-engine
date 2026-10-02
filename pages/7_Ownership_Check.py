"""Ownership Check — paste a company website, get an ownership verdict.

Runs the same multi-source research the sourcing pipeline uses:
Apollo funding data, the company's own about/investor pages, Crunchbase/
Tracxn, and Google searches for investors and parent companies — then
GPT renders a verdict: founder-owned, or owned/backed by PE, VC, or a
public parent, with the evidence shown.
"""

import streamlit as st

# Page config is owned by the app.py navigation router.


def _check_password():
    try:
        app_password = st.secrets["APP_PASSWORD"]
    except (FileNotFoundError, KeyError):
        st.error("APP_PASSWORD is not configured. Add it to secrets.toml.")
        return False

    def _entered():
        if st.session_state.get("password") == app_password:
            st.session_state["password_correct"] = True
        else:
            st.session_state["password_correct"] = False
            st.session_state["password_attempted"] = True

    if st.session_state.get("password_correct"):
        return True
    st.text_input("Enter Password", type="password",
                  on_change=_entered, key="password")
    if st.session_state.get("password_attempted"):
        st.error("Password incorrect")
    return False


if not _check_password():
    st.stop()

st.title("🕵️ Ownership Check")
st.caption(
    "Is this company founder-owned — or backed by private equity, venture "
    "capital, or a public parent? Paste the website and find out."
)

c1, c2 = st.columns([3, 2])
_url = c1.text_input("Company website",
                     placeholder="e.g. colonialtrust.com or https://...")
_name_in = c2.text_input("Company name (optional — improves the search)")

if st.button("Run ownership research", type="primary",
             disabled=not _url.strip()):
    from lib.api_clients import load_api_keys, make_openai_client, get_secret
    from lib.contacts import clean_domain, firecrawl_scrape
    from lib.apollo_search import enrich_organization
    from lib.filters import check_pe_vc_web

    domain = clean_domain(_url.strip())
    if not domain:
        st.error("That doesn't look like a valid website. "
                 "Try e.g. `company.com`.")
        st.stop()

    keys = load_api_keys()
    client = make_openai_client(api_key=keys["OPENAI_API_KEY"])
    firecrawl_key = keys.get("FIRECRAWL_API_KEY") or get_secret("FIRECRAWL_API_KEY", "")
    apollo_key = keys.get("APOLLO_API_KEY") or get_secret("APOLLO_API_KEY", "")

    def _scrape(u):
        return firecrawl_scrape(firecrawl_key, u)

    findings = []

    # ── Apollo funding record ─────────────────────────────────────
    org = None
    company_name = _name_in.strip()
    with st.spinner(f"Checking Apollo's record for {domain}..."):
        try:
            org = enrich_organization(apollo_key, domain)
        except Exception:
            org = None
    if org:
        company_name = company_name or org.get("name") or domain
        funding = org.get("total_funding")
        stage = str(org.get("latest_funding_stage") or "").replace("_", " ")
        rounds = org.get("number_of_funding_rounds")
        ticker = org.get("publicly_traded_symbol") or org.get("ticker")
        status = org.get("ownership_status")
        if ticker:
            findings.append(("🚨", f"Publicly traded — ticker {ticker}"))
        if status:
            findings.append(("ℹ️", f"Apollo ownership status: {status}"))
        if funding:
            try:
                findings.append(
                    ("⚠️" if float(funding) > 5_000_000 else "ℹ️",
                     f"Total funding on record: ${float(funding):,.0f}"))
            except (ValueError, TypeError):
                pass
        if stage:
            findings.append(("⚠️" if any(s in stage.lower() for s in
                             ["series", "private equity", "seed"]) else "ℹ️",
                             f"Latest funding stage: {stage}"))
        if rounds:
            findings.append(("ℹ️", f"Funding rounds on record: {rounds}"))
    else:
        company_name = company_name or domain
        findings.append(("ℹ️", "No Apollo record for this domain — relying "
                               "on web research."))

    # ── Web research (about pages, Crunchbase, Google, portfolios) ──
    with st.spinner(
            f"Researching {company_name} across the web (about/investor "
            f"pages, Crunchbase, Google) — takes ~30-60 seconds..."):
        try:
            is_backed, reason = check_pe_vc_web(
                client, _scrape, company_name, domain)
        except Exception as e:
            is_backed, reason = False, f"Web research failed: {e}"

    # ── Verdict ───────────────────────────────────────────────────
    st.markdown("---")
    _hard_flags = [f for icon, f in findings if icon == "🚨"]
    if is_backed or _hard_flags:
        st.error(
            f"## 🚨 Not founder-owned (or at serious risk)\n"
            f"**{company_name}** shows institutional-ownership signals."
        )
    else:
        _soft = [f for icon, f in findings if icon == "⚠️"]
        if _soft:
            st.warning(
                f"## ⚠️ Probably founder-owned — with caveats\n"
                f"**{company_name}**: no definitive PE/VC/public ownership "
                f"found, but review the flags below."
            )
        else:
            st.success(
                f"## ✅ Likely founder-owned\n"
                f"**{company_name}**: no PE, VC, or public-parent signals "
                f"found in Apollo data or web research."
            )

    st.markdown(f"**Web research conclusion:** {reason}")
    if findings:
        st.markdown("**Evidence collected:**")
        for icon, f in findings:
            st.markdown(f"- {icon} {f}")

    st.caption(
        "Checks: Apollo funding/ticker records · the company's own "
        "about/investor pages · Crunchbase & Tracxn · Google searches for "
        "investors, funding, and parent companies · known PE/VC portfolio "
        "pages. A clean result is strong but not absolute — quiet minority "
        "stakes don't always surface. Cost: roughly $0.05 per check."
    )
