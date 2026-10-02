"""NCP Turbo Engine — entrypoint and navigation router.

The legacy single-page engine that used to live here has been retired;
everything runs through the dedicated pages below. st.navigation gives
us full control of the sidebar: no stray "app" entry, NCP branding,
and human page names.
"""

import streamlit as st

st.set_page_config(
    page_title="NCP Turbo Engine",
    page_icon="assets/ncp_logo.svg",
    layout="wide",
)

# New Capital Partners logo at the top of the sidebar
try:
    st.logo("assets/ncp_logo.svg", size="large",
            link="https://newcapitalpartners.com")
except Exception:
    pass  # older Streamlit without st.logo — theme still applies

pages = [
    st.Page("pages/2_Sourcing_Pipeline.py",
            title="Sourcing Pipeline", icon="🤖", default=True),
    st.Page("pages/5_Deal_Tracker.py",
            title="Deal Tracker", icon="📇"),
    st.Page("pages/1_Intermediary_Outreach.py",
            title="Intermediary Outreach", icon="🔍"),
    st.Page("pages/6_NDA_Review.py",
            title="NDA Review", icon="📄"),
    st.Page("pages/7_Ownership_Check.py",
            title="Ownership Check", icon="🕵️"),
    st.Page("pages/4_Zombie_Fund_Screener.py",
            title="Zombie Fund Screener", icon="🧟"),
]

st.navigation(pages).run()
