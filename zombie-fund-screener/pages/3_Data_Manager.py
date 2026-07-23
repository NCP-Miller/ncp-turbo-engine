"""Data Manager — seed GPs, run refreshes, see source freshness.

Phase 1 ships manual seeding. The three automated importers (EDGAR
Form D, ADV bulk data, pension files) are the next build phase — their
buttons are visible but clearly marked so nothing feels broken.
"""

import streamlit as st

from zfs.ui import page_setup, save_backup
from zfs import lifecycle
from zfs.db import connect, load_config

page_setup("Data Manager — Zombie Fund Screener")
st.title("🗄️ Data Manager")

config = load_config()

# ── Seed GPs manually ────────────────────────────────────────────────
st.markdown("#### Add GPs")
c1, c2 = st.columns(2)
with c1:
    with st.form("gp_add", clear_on_submit=True):
        st.markdown("**One at a time**")
        gname = st.text_input("GP / firm name")
        gweb = st.text_input("Website (optional)")
        g1, g2 = st.columns(2)
        gcity = g1.text_input("City (optional)")
        gstate = g2.text_input("State (optional)")
        if st.form_submit_button("Add GP") and gname.strip():
            _, created = lifecycle.add_gp(gname.strip(), website=gweb or None,
                                          city=gcity or None,
                                          state=(gstate or "").upper() or None)
            save_backup()
            st.success(f"Added {gname.strip()}." if created
                       else f"{gname.strip()} already exists.")
with c2:
    with st.form("gp_bulk", clear_on_submit=True):
        st.markdown("**Bulk paste** (one firm name per line)")
        bulk = st.text_area("Names", height=150,
                            placeholder="Alpha Capital Partners\n"
                                        "Beta Growth Equity\n...")
        if st.form_submit_button("Add all"):
            added = 0
            for line in bulk.splitlines():
                if line.strip():
                    _, created = lifecycle.add_gp(line.strip())
                    added += int(created)
            save_backup()
            st.success(f"Added {added} new GPs.")

st.markdown("---")

# ── Refresh sources ──────────────────────────────────────────────────
st.markdown("#### Data sources")
conn = connect()
try:
    log = {r["source"]: dict(r) for r in
           conn.execute("SELECT * FROM refresh_log").fetchall()}
finally:
    conn.close()

src_rows = [
    ("EDGAR Form D", "edgar",
     "Pulls each sponsor's Form D history and flags aged funds with no "
     "successor. Uses your contact email in the User-Agent "
     f"({config.get('sec_contact_email') or 'set it in config.json'})."),
    ("Form ADV bulk data", "adv",
     "Imports the SEC's monthly adviser dataset: RAUM, headcount, and "
     "Schedule D private funds, kept as snapshots for trend detection."),
    ("Wayback website checks", "wayback",
     "Checks GP and portfolio-company sites for staleness via the "
     "Wayback Machine CDX API."),
    ("Pension files (CalPERS + uploads)", "pension",
     "Parses pension PE reports for vintage / DPI / NAV and fuzzy-matches "
     "fund names for your confirmation."),
]
for label, key, desc in src_rows:
    r1, r2, r3 = st.columns([2, 4, 2])
    r1.markdown(f"**{label}**")
    r2.caption(desc)
    last = log.get(key, {}).get("last_run")
    r3.button(
        f"Refresh — coming in Phase 3",
        key=f"refresh_{key}", disabled=True,
        help="The automated importers are the next build phase. "
             "All scoring already reads from these tables, so data "
             "lights up the moment the importer lands.",
    )
    r3.caption(f"Last run: {last[:16] if last else 'never'}")

st.info(
    "**Phase 3 (next build):** the EDGAR, ADV, Wayback, and pension "
    "importers plug into the evidence tables that already exist — no "
    "schema changes, and none of your manual work is ever touched. "
    "Until then, fund vintages entered on the GP Detail page drive "
    "Signal 1 the same way."
)
