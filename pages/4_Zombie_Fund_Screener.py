"""Zombie Fund Screener — embedded as a page in the NCP suite.

The screener is a self-contained multi-page app living in the
zombie-fund-screener/ folder. This host page runs its pages inside the
suite so it shows up as a single sidebar entry with its own internal
navigation, sharing the suite's password gate.

It keeps its own SQLite database (zombie_screener.db) and its own GitHub
backup file, so nothing here touches the sourcing/CRM data.
"""

import os
import sys
import runpy

import streamlit as st

st.set_page_config(page_title="Zombie Fund Screener", page_icon="🧟",
                   layout="wide")


# Same password gate as the rest of the suite (shares 'password_correct',
# so unlocking any page unlocks this one too).
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

# Make the screener's package importable and flag embedded mode so its
# own page_setup() skips set_page_config and the (already-passed) gate.
_ZFS_ROOT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                         "zombie-fund-screener")
if _ZFS_ROOT not in sys.path:
    sys.path.insert(0, _ZFS_ROOT)
os.environ["ZFS_EMBEDDED"] = "1"

st.title("🧟 Zombie Fund Screener")

# Internal navigation across the screener's pages.
VIEWS = {
    "🗓️ Today": "Today.py",
    "📊 Dashboard": "pages/1_Dashboard.py",
    "🔍 GP Detail": "pages/2_GP_Detail.py",
    "🗄️ Data Manager": "pages/3_Data_Manager.py",
    "⚙️ Signal Settings": "pages/4_Signal_Settings.py",
    "🪦 Graveyard": "pages/5_Graveyard.py",
    "📝 Templates": "pages/6_Templates.py",
    "📤 Export / Sync": "pages/7_Export_Sync.py",
}

choice = st.radio("Screener section", list(VIEWS),
                  horizontal=True, label_visibility="collapsed",
                  key="_zfs_view")
st.divider()

# Run the selected screener page in place. Each page's own title renders
# below; runpy re-executes it every rerun exactly like a native page.
try:
    runpy.run_path(os.path.join(_ZFS_ROOT, VIEWS[choice]),
                   run_name="__zfs_embedded__")
except SystemExit:
    # zfs pages call st.stop() in some empty states; that raises inside
    # runpy — swallow it so the host page finishes cleanly.
    pass
