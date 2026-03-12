import streamlit as st
from openai import OpenAI

# ---------------------------------------------------------------------------
# AUTH
# ---------------------------------------------------------------------------
def check_password():
    def password_entered():
        if st.session_state["password"] == st.secrets["APP_PASSWORD"]:
            st.session_state["password_correct"] = True
            del st.session_state["password"]
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        st.text_input("Enter Password", type="password", on_change=password_entered, key="password")
        return False
    elif not st.session_state["password_correct"]:
        st.text_input("Enter Password", type="password", on_change=password_entered, key="password")
        st.error("\u0050assword incorrect")
        return False
    return True

if not check_password():
    st.stop()

# ---------------------------------------------------------------------------
# SECRETS
# ---------------------------------------------------------------------------
try:
    APOLLO_API_KEY    = st.secrets["APOLLO_API_KEY"]
    OPENAI_API_KEY    = st.secrets["OPENAI_API_KEY"]
    FIRECRAWL_API_KEY = st.secrets["FIRECRAWL_API_KEY"]
    HTTP_USER_AGENT   = st.secrets["HTTP_USER_AGENT"]
except (FileNotFoundError, KeyError):
    st.error("Secrets missing (APP_PASSWORD, API keys). Set them in `.streamlit/secrets.toml`.")
    st.stop()

client = OpenAI(api_key=OPENAI_API_KEY, timeout=30.0)

# ---------------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------------
OPENAI_MODEL = "gpt-4o"

_TITLE_SCORES = {
    "owner": 100, "founder": 95, "co-founder": 90,
    "chief executive": 95, " ceo": 95,
    "president": 90, "managing partner": 88, "managing member": 88,
    "managing director": 85, "principal": 85,
    "executive director": 82, "medical director": 80,
    "chief operating": 75, " coo": 75,
    "chief financial": 70, " cfo": 70,
    "chief medical": 80, "chief clinical": 78, "chief nursing": 75,
    "administrator": 70, "director of": 60,
    "vice president": 50, " vp ": 50,
    "manager": 30,
}

_CONTACT_PATHS = [
    "/about", "/about-us", "/team", "/our-team", "/leadership",
    "/staff", "/management", "/who-we-are", "/meet-the-team",
    "/people", "/executives", "/administration", "/about/team",
    "/about/leadership", "/contact", "/contact-us",
    "/our-staff", "/board", "/board-of-directors", "/leadership-team",
    "/meet-our-team", "/staff-directory", "/our-leadership", "/directors",
    "/about/staff", "/about/leadership-team", "/about/administration",
]
