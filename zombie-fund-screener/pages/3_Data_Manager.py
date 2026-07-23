"""Data Manager — seed GPs, run every refresh, confirm fuzzy matches.

The daily rhythm is simple:
  1. Add GPs (by hand, or discover them from old EDGAR filings)
  2. Hit the refresh buttons — evidence flows into the signal tables
  3. Confirm any fuzzy matches the importers weren't sure about
Scores update instantly; your manual work is never touched.
"""

from datetime import date

import streamlit as st

from zfs.ui import page_setup, save_backup
from zfs import lifecycle
from zfs.db import connect, load_config

page_setup("Data Manager — Zombie Fund Screener")
st.title("🗄️ Data Manager")

config = load_config()

conn = connect()
try:
    log = {r["source"]: dict(r) for r in
           conn.execute("SELECT * FROM refresh_log").fetchall()}
    gp_count = conn.execute(
        "SELECT COUNT(*) AS n FROM gps WHERE killed = 0").fetchone()["n"]
finally:
    conn.close()


def _last(source):
    r = log.get(source, {}).get("last_run")
    d = log.get(source, {}).get("detail")
    return (f"Last run {r[:16].replace('T', ' ')} — {d}"
            if r else "Never run")


tab_add, tab_edgar, tab_adv, tab_way, tab_pen, tab_conf = st.tabs([
    "➕ Add GPs", "🔎 EDGAR (Signal 1)", "📉 ADV (Signals 2 & 9)",
    "🕸️ Wayback (Signals 3 & 8)", "🏛️ Pension (Signal 6)",
    "✅ Confirm matches"])

# ═══════════════════════ ADD GPS ═════════════════════════════════════
with tab_add:
    c1, c2 = st.columns(2)
    with c1:
        with st.form("gp_add", clear_on_submit=True):
            st.markdown("**One at a time**")
            gname = st.text_input("GP / firm name")
            gweb = st.text_input("Website (optional — powers Signal 3)")
            g1, g2 = st.columns(2)
            gcity = g1.text_input("City (optional)")
            gstate = g2.text_input("State (optional)")
            gcrd = st.text_input("CRD number (optional — makes ADV "
                                 "matching exact)")
            if st.form_submit_button("Add GP") and gname.strip():
                _, created = lifecycle.add_gp(
                    gname.strip(), website=gweb or None,
                    city=gcity or None,
                    state=(gstate or "").upper() or None,
                    crd_number=gcrd or None)
                save_backup()
                st.success(f"Added {gname.strip()}." if created
                           else f"{gname.strip()} already exists.")
    with c2:
        with st.form("gp_bulk", clear_on_submit=True):
            st.markdown("**Bulk paste** (one firm name per line)")
            bulk = st.text_area("Names", height=170,
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

# ═══════════════════════ EDGAR ═══════════════════════════════════════
with tab_edgar:
    st.caption(_last("edgar"))
    st.markdown(
        "**Refresh known GPs** — searches SEC EDGAR for each GP's Form D "
        "filings, stores them as evidence, and adds every distinct fund "
        "with its first-filing date. Signal 1 fires when the newest fund "
        "is past your age threshold with no successor. Throttled to ~2 "
        "requests/second (SEC allows 10); GPs refreshed in the last 20 "
        "hours are skipped."
    )
    e1, e2 = st.columns([1, 1])
    force = e2.checkbox("Force re-fetch (ignore 20-hour cache)")
    if e1.button(f"🔄 Refresh EDGAR for all {gp_count} GPs",
                 use_container_width=True, disabled=gp_count == 0):
        from zfs.importers import edgar
        gps = lifecycle.list_gps()
        bar = st.progress(0.0, text="Starting...")

        def _cb(i, total, name):
            bar.progress((i + 1) / max(total, 1),
                         text=f"Searching EDGAR: {name} ({i + 1}/{total})")
        try:
            result = edgar.refresh_all(gps, progress_cb=_cb, force=force)
            bar.empty()
            save_backup()
            msg = (f"Done — {result['filings']} new filings, "
                   f"{result['funds']} funds added, "
                   f"{result['skipped']} skipped (cached)")
            if result["errors"]:
                msg += (f", {result['errors']} GPs failed (SEC may be "
                        f"busy — run again later)")
            st.success(msg)
        except Exception as e:
            bar.empty()
            st.error(f"EDGAR is unreachable right now ({e}). "
                     "Nothing was lost — try again in a few minutes.")

    st.markdown("---")
    st.markdown(
        "**Discover unknown sponsors** — searches OLD Form D filings by "
        "keyword (sector, strategy, state...) so aging funds you've never "
        "heard of surface. Add the interesting ones, then run the refresh "
        "above to pull their full filing history."
    )
    with st.form("edgar_discover"):
        d1, d2, d3 = st.columns([3, 1, 1])
        q = d1.text_input("Search term",
                          placeholder='e.g., "healthcare private equity"')
        y1 = d2.number_input("From year", 2005, 2020, 2010)
        y2 = d3.number_input("To year", 2006, 2021, 2016)
        go = st.form_submit_button("Search old filings")
    if go and q.strip():
        from zfs.importers import edgar
        try:
            with st.spinner("Searching EDGAR..."):
                found = edgar.discover(q.strip(), int(y1), int(y2))
            st.session_state["_edgar_found"] = found
        except Exception as e:
            st.error(f"EDGAR is unreachable right now ({e}).")
    for i, f in enumerate(st.session_state.get("_edgar_found", [])):
        r1, r2, r3 = st.columns([4, 2, 1])
        r1.markdown(f"**{f['entity']}**")
        r2.caption(f"Form D filed {f['file_date']}"
                   + (f" · [filing]({f['url']})" if f['url'] else ""))
        if r3.button("Add as GP", key=f"disc_{i}"):
            lifecycle.add_gp(f["entity"])
            save_backup()
            st.toast(f"Added {f['entity']}")

# ═══════════════════════ ADV ═════════════════════════════════════════
with tab_adv:
    st.caption(_last("adv"))
    st.markdown(
        "**Import a monthly ADV bulk file** — download the latest "
        "\"Form ADV data\" CSV/ZIP from "
        "[sec.gov/foia/docs/form-adv-data](https://www.sec.gov/foia/docs/form-adv-data), "
        "then drop it here. The importer streams it in chunks, keeps only "
        "RAUM / headcount / fund fields, and stores one snapshot per GP "
        "per month — two snapshots and the decline trends (Signal 2) "
        "light up. If the ZIP includes Schedule D 7.B.1, service-provider "
        "changes (Signal 9) are diffed automatically."
    )
    up = st.file_uploader("ADV bulk file (.csv or .zip)",
                          type=["csv", "zip"], key="adv_up")
    snap_date = st.date_input("Snapshot date (the file's month)",
                              value=date.today().replace(day=1))
    if up is not None and st.button("Import ADV file",
                                    use_container_width=True):
        from zfs.importers import adv
        try:
            with st.spinner("Parsing (large files take a minute)..."):
                result = adv.import_adv(up.getvalue(), up.name,
                                        snap_date.isoformat())
            save_backup()
            if not result["main_file_found"]:
                st.warning("Couldn't find the adviser table (CRD + name + "
                           "regulatory assets columns) in that file. "
                           "Make sure it's the ADV base data file.")
            else:
                st.success(f"Imported {result['saved']} snapshots. "
                           f"{result['queued']} fuzzy matches queued for "
                           f"your confirmation, {result['dupes']} already "
                           f"had this month, {result['provider_changes']} "
                           f"provider changes detected.")
        except Exception as e:
            st.error(f"Could not parse that file: {e}")

# ═══════════════════════ WAYBACK ═════════════════════════════════════
with tab_way:
    st.caption(_last("wayback"))
    st.markdown(
        "**Check website staleness** — asks the Wayback Machine when each "
        "GP site (Signal 3) and portfolio-company site (Signal 8) last "
        "meaningfully changed. Only GPs/companies with a website filled "
        "in are checked. Free API, politely throttled."
    )
    if st.button("🕸️ Run Wayback checks", use_container_width=True):
        from zfs.importers import wayback
        bar = st.progress(0.0, text="Starting...")

        def _wcb(i, total, url):
            bar.progress((i + 1) / max(total, 1),
                         text=f"Checking {url} ({i + 1}/{total})")
        try:
            result = wayback.run_all(progress_cb=_wcb)
            bar.empty()
            save_backup()
            st.success(f"Checked {result['checked']} sites. "
                       f"{result['no_archive']} had no archive history, "
                       f"{result['errors']} errors.")
        except Exception as e:
            bar.empty()
            st.error(f"The Wayback Machine is unreachable right now ({e}).")

# ═══════════════════════ PENSION ═════════════════════════════════════
with tab_pen:
    st.caption(_last("pension"))
    st.markdown(
        "**Import a pension PE performance file** — CalPERS publishes its "
        "PEP performance as CSV/Excel; Washington SIB, Texas TRS, Oregon "
        "PERF, and Florida SBA publish similar files. Download one, drop "
        "it here, check the column mapping, import. Confirmed rows give "
        "GPs the ✅ pension-verified badge — the highest-quality signal."
    )
    pup = st.file_uploader("Pension file (.csv or .xlsx)",
                           type=["csv", "xlsx", "xls"], key="pen_up")
    src_label = st.text_input("Source label", value="CalPERS",
                              help="Shows in evidence, e.g. CalPERS 2026Q2")
    if pup is not None:
        from zfs.importers import pension
        try:
            df = pension.read_file(pup.getvalue(), pup.name)
            st.caption(f"{len(df)} rows. Map the columns (guessed for you):")
            guess = pension.guess_columns(df)
            opts = ["(none)"] + list(df.columns)

            def _ix(g):
                return opts.index(g) if g in opts else 0
            m1, m2, m3 = st.columns(3)
            col_fund = m1.selectbox("Fund name", opts, index=_ix(guess["fund_name"]))
            col_vint = m2.selectbox("Vintage year", opts, index=_ix(guess["vintage"]))
            col_comm = m3.selectbox("Committed", opts, index=_ix(guess["committed"]))
            m4, m5, m6 = st.columns(3)
            col_nav = m4.selectbox("NAV / remaining value", opts, index=_ix(guess["nav"]))
            col_dpi = m5.selectbox("DPI", opts, index=_ix(guess["dpi"]))
            col_irr = m6.selectbox("IRR", opts, index=_ix(guess["irr"]))
            if st.button("Import pension file", use_container_width=True,
                         disabled=col_fund == "(none)"):
                mapping = {"fund_name": col_fund,
                           "vintage": None if col_vint == "(none)" else col_vint,
                           "committed": None if col_comm == "(none)" else col_comm,
                           "nav": None if col_nav == "(none)" else col_nav,
                           "dpi": None if col_dpi == "(none)" else col_dpi,
                           "irr": None if col_irr == "(none)" else col_irr}
                result = pension.import_pension(df, mapping,
                                                src_label.strip() or "upload")
                save_backup()
                st.success(f"{result['auto']} rows auto-linked to your GPs, "
                           f"{result['queued']} queued for confirmation, "
                           f"{result['unmatched']} stored unmatched, "
                           f"{result['dupes']} duplicates skipped.")
        except Exception as e:
            st.error(f"Could not read that file: {e}")

# ═══════════════════════ CONFIRM MATCHES ═════════════════════════════
with tab_conf:
    from zfs.importers.adv import pending_matches, confirm_match, reject_match
    pend = pending_matches()
    st.markdown(
        f"**{len(pend)} fuzzy matches waiting** — the importers only "
        "auto-merge high-confidence matches; everything else waits for "
        "your yes/no here. Nothing is ever merged without you."
    )
    for m in pend:
        c1, c2, c3, c4 = st.columns([4, 2, 1, 1])
        c1.markdown(f"`{m['kind'].upper()}` **{m['matched_name']}**  \n"
                    f"→ your GP: **{m['gp_name']}**")
        c2.caption(f"Similarity: {m['score']:.0f}%")
        if c3.button("Confirm", key=f"cm_{m['id']}"):
            confirm_match(m["id"])
            save_backup()
            st.rerun()
        if c4.button("Reject", key=f"rm_{m['id']}"):
            reject_match(m["id"])
            st.rerun()
