"""Dashboard — ranked live candidates with quick actions."""

import streamlit as st

from zfs.ui import page_setup, save_backup
from zfs import crm, lifecycle, scoring
from zfs.db import (load_config, PIPELINE_STATUSES, KILL_REASONS, VERTICALS,
                    EAST_OF_DENVER, connect)
from zfs.settings import get_signal_settings, get_cadence

page_setup("Dashboard — Zombie Fund Screener")
st.title("📊 Dashboard")

config = load_config()
settings = get_signal_settings()
cadence = get_cadence()

pool, all_rows = scoring.score_all(settings, cadence.get("min_signals", 1))
lifecycle.mark_surfaced([r["gp"]["id"] for r in pool])

# ── Pipeline summary stats ───────────────────────────────────────────
conn = connect()
try:
    status_counts = {s: 0 for s in PIPELINE_STATUSES}
    for r in conn.execute(
            "SELECT status, COUNT(*) AS n FROM gps WHERE killed = 0 "
            "GROUP BY status").fetchall():
        status_counts[r["status"]] = r["n"]
    week_new = conn.execute(
        "SELECT COUNT(*) AS n FROM gps WHERE killed = 0 AND "
        "first_surfaced_at >= date('now', '-7 day')").fetchone()["n"]
    week_killed = conn.execute(
        "SELECT COUNT(*) AS n FROM gps WHERE killed = 1 AND "
        "killed_at >= date('now', '-7 day')").fetchone()["n"]
    week_touches = conn.execute(
        "SELECT COUNT(*) AS n FROM events WHERE kind = 'activity' AND "
        "timestamp >= date('now', '-7 day')").fetchone()["n"]
finally:
    conn.close()

m = st.columns(6)
m[0].metric("Candidates", len(pool))
m[1].metric("Outreach Sent", status_counts.get("Outreach Sent", 0))
m[2].metric("In Dialogue", status_counts.get("In Dialogue", 0))
m[3].metric("New this week", week_new)
m[4].metric("Killed this week", week_killed)
m[5].metric("Touches this week", week_touches)

# ── Filters ──────────────────────────────────────────────────────────
f1, f2, f3, f4 = st.columns(4)
f_status = f1.multiselect("Status", PIPELINE_STATUSES, default=[])
f_vertical = f2.multiselect("Portfolio verticals", VERTICALS, default=[])
f_geo = f3.selectbox("Geography", ["All", "East of Denver only"])
f_minscore = f4.slider("Minimum score", 0, 100, 0)

rows = pool
if f_status:
    rows = [r for r in rows if r["gp"]["status"] in f_status]
if f_minscore:
    rows = [r for r in rows if r["score"] >= f_minscore]
if f_vertical:
    conn = connect()
    try:
        keep = set()
        for r in rows:
            hit = conn.execute(
                f"SELECT 1 FROM portfolio_companies WHERE gp_id = ? AND "
                f"vertical IN ({','.join('?' * len(f_vertical))}) LIMIT 1",
                (r["gp"]["id"], *f_vertical)).fetchone()
            if hit:
                keep.add(r["gp"]["id"])
        rows = [r for r in rows if r["gp"]["id"] in keep]
    finally:
        conn.close()
if f_geo == "East of Denver only":
    rows = [r for r in rows
            if (r["gp"].get("state") or "").upper() in EAST_OF_DENVER]

st.caption(f"{len(rows)} candidates shown, ranked by score. "
           "Open a GP in **GP Detail** to see evidence and work the deal.")

# ── Candidate rows ───────────────────────────────────────────────────
for r in rows:
    gp = lifecycle.get_gp(r["gp"]["id"])  # fresh copy (seen/killed state)
    if not gp or gp["killed"]:
        continue
    new_badge = "🆕 " if lifecycle.is_new(gp) else ""
    ver_badge = " ✅" if r["verified"] else ""
    sig_badges = " ".join(s.upper() for s in r["fired"])
    last_act = crm.last_activity_date(gp["id"])
    next_task = crm.next_task_date(gp["id"])

    c = st.columns([3, 1, 2, 2, 1, 1])
    c[0].markdown(f"{new_badge}**{gp['name']}**{ver_badge}  \n"
                  f":gray[{sig_badges or 'no signals'}]")
    c[1].markdown(f"**{r['score']}**")
    new_status = c[2].selectbox(
        "Status", PIPELINE_STATUSES,
        index=PIPELINE_STATUSES.index(gp["status"])
        if gp["status"] in PIPELINE_STATUSES else 0,
        key=f"st_{gp['id']}", label_visibility="collapsed")
    if new_status != gp["status"]:
        lifecycle.set_status(gp["id"], new_status, user=config["users"][0])
        save_backup()
        st.rerun()
    c[3].caption(f"Last: {(last_act or 'never')[:10]}  \n"
                 f"Next: {next_task or '—'}")
    with c[4].popover("Log"):
        with st.form(key=f"dlog_{gp['id']}"):
            a_type = st.selectbox("Type", ["Call", "Email",
                                           "LinkedIn message"],
                                  key=f"dt_{gp['id']}")
            a_out = st.selectbox("Outcome", ["Connected", "Left voicemail",
                                             "No answer", "Replied",
                                             "Meeting set"],
                                 key=f"do_{gp['id']}")
            a_sum = st.text_input("Summary", key=f"ds_{gp['id']}")
            if st.form_submit_button("Log"):
                res = crm.log_activity(gp["id"], a_type,
                                       a_sum or a_type,
                                       direction="outbound", outcome=a_out,
                                       user=config["users"][0])
                if res.get("auto_task"):
                    st.toast(f"Cadence: {res['auto_task']}")
                save_backup()
                st.rerun()
    with c[5].popover("Kill"):
        with st.form(key=f"dkill_{gp['id']}"):
            k_cat = st.selectbox("Reason", KILL_REASONS, key=f"kc_{gp['id']}")
            k_txt = st.text_input("One line why", key=f"kt_{gp['id']}")
            if st.form_submit_button("Kill GP"):
                lifecycle.kill_gp(gp["id"], k_cat, k_txt,
                                  user=config["users"][0])
                save_backup()
                st.rerun()

if not rows:
    st.info("No candidates match. Add GPs in **Data Manager**, enter fund "
            "dates or manual signal data in **GP Detail**, or lower "
            "thresholds in **Signal Settings**.")
