"""Today — the landing page. Overdue tasks, due today, due this week,
stale relationships, and new candidates: your whole morning on one screen."""

import streamlit as st

from zfs.ui import page_setup, save_backup
from zfs import crm, lifecycle, scoring
from zfs.db import load_config
from zfs.settings import get_signal_settings, get_cadence

page_setup("Today — Zombie Fund Screener")

st.title("🧟 Zombie Fund Screener — Today")

config = load_config()
cadence = get_cadence()

# ── Start Here: a plain-English map of the whole workflow ────────────
from zfs import lifecycle as _lc
_gp_total = len(_lc.list_gps(include_killed=True))
with st.expander("🏁 Start Here — how this app works",
                 expanded=_gp_total == 0):
    st.markdown("""
The whole app is one loop: **find → score → work → kill or close.**

1. **Find GPs** *(Data Manager)* — add firms you know, or use
   **Discover** to search old SEC Form D filings by keyword and surface
   aging sponsors you've never heard of.
2. **Pull evidence** *(Data Manager)* — hit **Refresh EDGAR** (fund
   vintages, automatic), **Run Wayback checks** (stale websites,
   automatic), and drop in ADV / pension files when you have them.
   Everything else (hold periods, team decay, exits, UCC) you enter on
   the GP Detail page as you research.
3. **Work the list** *(Dashboard)* — GPs that fire signals appear
   ranked 0–100 with 🆕 badges. Open one, add a contact, log a call —
   the app schedules your follow-ups automatically and they show up
   right here on Today.
4. **Kill the misfits** — one click + a reason. Gone forever (but
   recoverable in the Graveyard). The list stays clean and every
   refresh only ever ADDS candidates, never resurrects your kills.

**Your first 10 minutes:** Data Manager → add 5 firm names → Refresh
EDGAR → watch the Dashboard rank them.
""")

# ── Task buckets ─────────────────────────────────────────────────────
overdue, due_today, due_week = crm.today_buckets()


def _task_row(t, flavor):
    c1, c2, c3, c4 = st.columns([4, 2, 1, 1])
    icon = {"overdue": "🔴", "today": "🟡", "week": "⚪"}[flavor]
    c1.markdown(f"{icon} **{t['description']}** — {t['gp_name']}")
    c2.caption(f"Due {t['due_date']} · {t['priority']} · {t['assigned_to']}"
               + (" · auto" if t.get("auto_generated") else ""))
    if c3.button("Done", key=f"done_{t['id']}"):
        crm.complete_task(t["id"])
        save_backup()
        st.rerun()
    with c4.popover("Log"):
        with st.form(key=f"qlog_{t['id']}"):
            a_type = st.selectbox("Type", ["Call", "Email", "LinkedIn message"],
                                  key=f"qt_{t['id']}")
            a_out = st.selectbox("Outcome",
                                 ["Connected", "Left voicemail", "No answer",
                                  "Replied", "Meeting set"],
                                 key=f"qo_{t['id']}")
            a_sum = st.text_input("Summary", key=f"qs_{t['id']}")
            if st.form_submit_button("Log activity"):
                res = crm.log_activity(
                    t["gp_id"], a_type, a_sum or f"{a_type} (from task)",
                    direction="outbound", outcome=a_out,
                    user=config["users"][0])
                crm.complete_task(t["id"])
                if res.get("auto_task"):
                    st.toast(f"Cadence: {res['auto_task']}")
                if res.get("suggestion"):
                    st.session_state["_suggest"] = (t["gp_id"],
                                                    res["suggestion"])
                save_backup()
                st.rerun()


# Status suggestion confirmation (one click, never automatic)
if st.session_state.get("_suggest"):
    sgp, sstatus = st.session_state["_suggest"]
    sg = lifecycle.get_gp(sgp)
    if sg:
        sc1, sc2, sc3 = st.columns([3, 1, 1])
        sc1.info(f"Suggestion: move **{sg['name']}** to **{sstatus}**?")
        if sc2.button("Yes, move it"):
            lifecycle.set_status(sgp, sstatus, user=config["users"][0])
            st.session_state.pop("_suggest")
            save_backup()
            st.rerun()
        if sc3.button("Dismiss"):
            st.session_state.pop("_suggest")
            st.rerun()

st.subheader(f"🔴 Overdue ({len(overdue)})")
if overdue:
    for t in overdue:
        _task_row(t, "overdue")
else:
    st.caption("Nothing overdue. 👏")

st.subheader(f"🟡 Due today ({len(due_today)})")
if due_today:
    for t in due_today:
        _task_row(t, "today")
else:
    st.caption("Nothing due today.")

st.subheader(f"⚪ Due this week ({len(due_week)})")
if due_week:
    for t in due_week:
        _task_row(t, "week")
else:
    st.caption("Nothing due this week.")

# ── Stale relationships ──────────────────────────────────────────────
st.subheader(f"🕸️ Stale relationships (no touch in {cadence['stale_days']}+ days)")
stale = crm.stale_relationships(cadence["stale_days"])
if stale:
    for g in stale:
        c1, c2 = st.columns([4, 2])
        c1.markdown(f"**{g['name']}** — {g['status']}")
        c2.caption(f"Last activity: {(g.get('last_act') or 'never')[:10]}")
else:
    st.caption("No stale relationships.")

# ── New candidates ───────────────────────────────────────────────────
st.subheader("🆕 New candidates")
settings = get_signal_settings()
pool, _ = scoring.score_all(settings, cadence.get("min_signals", 1))
lifecycle.mark_surfaced([r["gp"]["id"] for r in pool])
new_ones = [r for r in pool if lifecycle.is_new(lifecycle.get_gp(r["gp"]["id"]))]
if new_ones:
    for r in new_ones:
        st.markdown(
            f"- 🆕 **{r['gp']['name']}** — score {r['score']}, "
            f"{len(r['fired'])} signal(s) fired"
            + (" · ✅ pension-verified" if r["verified"] else ""))
    st.caption("Open a GP's detail page to clear its NEW badge.")
else:
    st.caption("No new candidates since your last review.")
