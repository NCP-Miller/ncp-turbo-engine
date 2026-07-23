"""Shared page bootstrap: password gate, DB init, one-time restore.

Every page calls page_setup() first. The password gate uses the same
APP_PASSWORD secret pattern as the other NCP apps; when no secret is
configured (e.g., running locally), the gate is skipped so a beginner
can just double-click run_app.bat and go.
"""

import os

import streamlit as st

from zfs.db import init_db
from zfs import backup


def _embedded():
    """True when running as a page inside another app (the NCP suite),
    where set_page_config and the password gate are handled by the host."""
    return os.environ.get("ZFS_EMBEDDED") == "1"


def _check_password():
    try:
        app_password = st.secrets["APP_PASSWORD"]
    except (FileNotFoundError, KeyError):
        return True  # no secret configured -> local mode, no gate

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


def page_setup(title, icon="🧟"):
    # When embedded in the host app, the host already set the page config
    # and gated the password (sharing the same session flag), so skip both.
    if not _embedded():
        st.set_page_config(page_title=title, page_icon=icon, layout="wide")
        if not _check_password():
            st.stop()
    init_db()
    # Merge the GitHub backup once per browser session (survives redeploys)
    if not st.session_state.get("_zfs_restored"):
        try:
            backup.restore_merge()
        except Exception:
            pass
        st.session_state["_zfs_restored"] = True


def save_backup():
    """Best-effort backup after a mutation — never blocks the UI on error."""
    try:
        backup.backup()
    except Exception:
        pass
