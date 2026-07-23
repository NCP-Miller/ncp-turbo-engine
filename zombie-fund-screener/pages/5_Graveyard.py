"""Graveyard — killed GPs with reasons; resurrect if you change your mind."""

import streamlit as st

from zfs.ui import page_setup, save_backup
from zfs import lifecycle
from zfs.db import connect, load_config

page_setup("Graveyard — Zombie Fund Screener")
st.title("🪦 Graveyard")

config = load_config()

conn = connect()
try:
    dead = [dict(r) for r in conn.execute(
        "SELECT * FROM gps WHERE killed = 1 ORDER BY killed_at DESC"
    ).fetchall()]
finally:
    conn.close()

if not dead:
    st.info("Nothing here yet. Killed GPs land in the Graveyard with your "
            "reason, stay out of the dashboard forever, and can be "
            "resurrected if you change your mind.")

for g in dead:
    c1, c2, c3, c4 = st.columns([3, 2, 3, 1])
    c1.markdown(f"**{g['name']}**")
    c2.caption(f"Killed {(g.get('killed_at') or '?')[:10]}")
    reason = g.get("kill_category") or "—"
    if g.get("kill_reason"):
        reason += f": {g['kill_reason']}"
    c3.caption(reason)
    if c4.button("Resurrect", key=f"res_{g['id']}"):
        lifecycle.resurrect_gp(g["id"], user=config["users"][0])
        save_backup()
        st.rerun()
