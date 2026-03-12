import streamlit as st
import pandas as pd
import concurrent.futures
from config import APOLLO_INDUSTRIES, check_password
from ai_params import suggest_search_params
from apollo_search import search_organizations, web_discovery_pass
from contacts import clean_domain
from worker import process_single_company

st.set_page_config(page_title="NCP Sourcing Engine", layout="wide")

st.title("🚀 NCP Sourcing Engine")
st.caption("Describe your target → AI suggests search fields → source at scale.")

# ── Step 1: Niche input + Suggest Fields ────────────────────────────────────
st.markdown("### Step 1 — What are you looking for?")
n1, n2 = st.columns([5, 1])
niche_raw = n1.text_input(
    "Niche",
    placeholder="e.g.  PACE programs for elderly  |  commercial HVAC contractors  |  veterinary practices",
    label_visibility="collapsed",
    key="niche_raw_input",
)
suggest_clicked = n2.button("🔍 Suggest Fields", use_container_width=True)

if suggest_clicked:
    if not (niche_raw or "").strip():
        st.warning("Please describe your target niche first.")
    else:
        with st.spinner(f"Analysing '{niche_raw}' and mapping to Apollo parameters…"):
            s = suggest_search_params(niche_raw)
            st.session_state["s_industries"] = s["industries"]
            st.session_state["s_keywords"]   = s["keywords"]
            st.session_state["s_niche"]      = niche_raw
        st.rerun()

# Banner — show what was suggested (only after Suggest Fields has run)
if "s_industries" in st.session_state:
    ind_str = ", ".join(st.session_state["s_industries"])
    kw_str  = st.session_state.get("s_keywords") or "(none — broaden if needed)"
    st.success(
        f"**Industries:** {ind_str}\n\n"
        f"**Keywords:** {kw_str}\n\n"
        f"Review and adjust below, then click **Start Sourcing**."
    )

st.divider()

# ── Step 2: Review / adjust fields ──────────────────────────────────────────
st.markdown("### Step 2 — Review, adjust, and run")

# Initialise session state defaults so widgets don't crash on first load
for k, v in [("s_industries", ["Hospital & Health Care"]),
             ("s_keywords",   ""),
             ("s_niche",      "")]:
    if k not in st.session_state:
        st.session_state[k] = v

r1a, r1b = st.columns(2)
industries = r1a.multiselect(
    "Apollo Industry Categories",
    options=APOLLO_INDUSTRIES,
    key="s_industries",
)
specific_niche = r1b.text_input(
    "Specific Niche (AI Filter)",
    key="s_niche",
    help="Plain-English description used by the AI relevance filter — be specific.",
)

r2a, r2b, r2c = st.columns(3)
target_geo = r2a.text_input("Geography", value="North Carolina, United States")
mode = r2b.selectbox("Strategy", [
    "A - Acquire  (Strict: small private operators only)",
    "B - Prospect (Broad: competitors & referral/sales targets, all sizes)",
])
apollo_keywords_raw = r2c.text_input(
    "Apollo Keyword Tags",
    key="s_keywords",
    help=(
        "Short 1–4 word tags only — longer phrases match nothing. "
        "Leave blank to search by industry alone (broadest results). "
        "Examples: pace program, adult day care, home health"
    ),
)

st.caption(
    "**Search strategy:** Each industry is swept up to 1,000 results (no keyword filter) so "
    "broadly-classified companies aren't missed. Keywords run a *separate* sweep across ALL "
    "industries to catch companies Apollo has placed in unexpected categories. "
    "A Google discovery pass then scrapes page 1 of Google to catch companies that "
    "Apollo doesn't have at all. The AI filter screens every candidate for true niche relevance."
)

if st.button("🚀 Start Sourcing", type="primary"):
    if not industries:
        st.error("Please select at least one industry, or click **Suggest Fields** first.")
        st.stop()

    strat_code   = "A" if "A -" in mode else "B"
    keyword_tags = [k.strip() for k in apollo_keywords_raw.split(",") if k.strip()] or None

    kw_display = f" + keyword sweep ({', '.join(keyword_tags)})" if keyword_tags else ""
    st.info(
        f"🔎 Searching **{len(industries)} industries**{kw_display} "
        f"in **{target_geo}** (up to 1,000 results per industry)…"
    )

    try:
        orgs = search_organizations(industries, target_geo, keyword_tags=keyword_tags)
    except Exception as e:
        st.error(f"Apollo API error: {e}")
        st.stop()

    # Build dedup sets for Google discovery pass
    seen_domains = set()
    seen_names   = set()
    for o in orgs:
        d = clean_domain(o.get("website_url"))
        if d: seen_domains.add(d)
        n = (o.get("name") or "").strip().lower()
        if n: seen_names.add(n)

    # Pass 3: Web scrape to catch companies Apollo missed
    google_orgs = []
    with st.spinner("Checking Google/DuckDuckGo/Bing for companies Apollo may have missed…"):
        try:
            google_orgs = web_discovery_pass(specific_niche, target_geo,
                                             seen_domains, seen_names)
        except Exception as e:
            st.warning(f"Web discovery pass failed (continuing with Apollo results): {e}")
    if google_orgs:
        orgs.extend(google_orgs)
        st.info(f"🔍 Web discovery added **{len(google_orgs)}** companies not in Apollo.")

    if not orgs:
        st.error(
            "No companies found in Apollo or Google. "
            "Try broadening the industry selection or clearing the Keywords field."
        )
        st.stop()

    st.success(
        f"Found **{len(orgs)}** unique candidates ({len(orgs) - len(google_orgs) if google_orgs else len(orgs)} "
        f"Apollo + {len(google_orgs) if google_orgs else 0} Google) — "
        f"running AI filter with 5 parallel workers…"
    )
    progress_bar = st.progress(0)
    status_text  = st.empty()
    final_data   = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
        futures = {
            ex.submit(process_single_company, org, specific_niche, strat_code): org
            for org in orgs
        }
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            try:
                result = future.result()
                if result: final_data.append(result)
            except Exception:
                pass  # skip companies that error out
            progress_bar.progress((i + 1) / len(orgs))
            status_text.caption(
                f"Processed {i+1}/{len(orgs)} | {len(final_data)} passed so far…"
            )

    status_text.write("✅ Sourcing complete!")

    if final_data:
        df  = pd.DataFrame(final_data)
        st.dataframe(df)
        csv   = df.to_csv(index=False).encode("utf-8")
        fname = (
            f"NCP_{'_'.join(industries[:2])}_{target_geo}.csv"
            .replace(" ", "_").replace(",", "")
        )
        st.download_button(
            "Download CSV", data=csv, file_name=fname, mime="text/csv", type="primary"
        )
    else:
        st.warning(
            "No targets passed the filters.\n\n"
            "**Tips:**\n"
            "- Clear the Keywords field and retry (keywords narrow Apollo results)\n"
            "- Click **Suggest Fields** for AI-recommended parameters\n"
            "- Switch to **Mode B** for a broader sweep\n"
            "- Add more industry categories"
        )
