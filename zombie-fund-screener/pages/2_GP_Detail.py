"""GP Detail — every signal, evidence, contacts, timeline, tasks,
and the template helper for one GP. Opening this page clears the
GP's NEW badge."""

from datetime import date
from urllib.parse import quote

import streamlit as st

from zfs.ui import page_setup, save_backup
from zfs import crm, lifecycle, scoring
from zfs.db import (connect, load_config, now, PIPELINE_STATUSES,
                    KILL_REASONS, VERTICALS, ACTIVITY_TYPES,
                    ACTIVITY_OUTCOMES, UCC_SEARCH_URLS)
from zfs.settings import get_signal_settings
from zfs import templates_lib

page_setup("GP Detail — Zombie Fund Screener")
st.title("🔍 GP Detail")

config = load_config()
settings = get_signal_settings()

gps = lifecycle.list_gps(include_killed=False)
if not gps:
    st.info("No GPs yet — add your first ones in **Data Manager**.")
    st.stop()

names = {g["name"]: g["id"] for g in gps}
default_ix = 0
if st.session_state.get("_detail_gp") in names:
    default_ix = list(names).index(st.session_state["_detail_gp"])
sel = st.selectbox("Select GP", list(names), index=default_ix)
st.session_state["_detail_gp"] = sel
gp_id = names[sel]
lifecycle.mark_seen(gp_id)          # clears the NEW badge
gp = lifecycle.get_gp(gp_id)

# ── Header: score, status, kill, links ───────────────────────────────
conn = connect()
try:
    bundle = scoring._gp_bundle(conn, gp_id)
finally:
    conn.close()
results = scoring.evaluate_gp(gp, bundle, settings)
score = scoring.composite_score(results, settings)

h1, h2, h3 = st.columns([2, 2, 2])
with h1:
    st.metric("Zombie score", f"{score}/100")
    st.caption(f"{sum(1 for r in results.values() if r['fired'])} of "
               f"{len(results)} enabled signals fired"
               + (" · ✅ pension-verified"
                  if any(r.get("verified") for r in results.values()) else ""))
with h2:
    new_status = st.selectbox(
        "Pipeline status", PIPELINE_STATUSES,
        index=PIPELINE_STATUSES.index(gp["status"])
        if gp["status"] in PIPELINE_STATUSES else 0)
    if new_status != gp["status"]:
        lifecycle.set_status(gp_id, new_status, user=config["users"][0])
        save_backup()
        st.rerun()
with h3:
    with st.popover("💀 Kill this GP"):
        with st.form("kill_form"):
            k_cat = st.selectbox("Reason", KILL_REASONS)
            k_txt = st.text_input("One line why")
            if st.form_submit_button("Kill"):
                lifecycle.kill_gp(gp_id, k_cat, k_txt, user=config["users"][0])
                save_backup()
                st.rerun()

# Research links (open in new tabs; LinkedIn is link-only, never scraped)
links = []
links.append(("EDGAR full-text search",
              f"https://efts.sec.gov/LATEST/search-index?q="
              f"{quote(gp['name'])}&forms=D"))
links.append(("Adviser search (Form ADV)",
              f"https://adviserinfo.sec.gov/search/genericsearch/firmgrid"
              f"?searchstring={quote(gp['name'])}"))
if gp.get("website"):
    links.append(("Website", gp["website"]))
    links.append(("Wayback history",
                  f"https://web.archive.org/web/2024*/{gp['website']}"))
if gp.get("linkedin_url"):
    links.append(("LinkedIn (check by eye)", gp["linkedin_url"]))
st.markdown(" · ".join(f"[{n}]({u})" for n, u in links))

with st.expander("✏️ Edit GP basics (website, LinkedIn, CRD, location, notes)"):
    with st.form("gp_edit"):
        e1, e2 = st.columns(2)
        w = e1.text_input("Website", gp.get("website") or "")
        li = e2.text_input("LinkedIn company URL", gp.get("linkedin_url") or "")
        e3, e4, e5 = st.columns(3)
        crd = e3.text_input("CRD number", gp.get("crd_number") or "")
        city = e4.text_input("City", gp.get("city") or "")
        state = e5.text_input("State (2 letters)", gp.get("state") or "")
        gnotes = st.text_area("Notes", gp.get("notes") or "", height=80)
        if st.form_submit_button("Save"):
            lifecycle.update_gp(gp_id, website=w, linkedin_url=li,
                                crd_number=crd, city=city,
                                state=state.upper(), notes=gnotes)
            save_backup()
            st.rerun()

tab_sig, tab_port, tab_crm, tab_time = st.tabs(
    ["🚨 Signals & Evidence", "🏢 Portfolio Companies",
     "👥 Contacts & Templates", "🕓 Timeline & Tasks"])

# ═════════════════════════ SIGNALS TAB ═══════════════════════════════
with tab_sig:
    st.markdown("#### Score breakdown")
    for sid, res in results.items():
        icon = "🔥" if res["fired"] else "▫️"
        line = (f"{icon} **{sid.upper()} — {settings[sid]['name']}** "
                f"(weight {settings[sid]['weight']}): {res['evidence']}")
        if res.get("link"):
            line += f" [source]({res['link']})"
        st.markdown(line)

    st.markdown("---")
    st.markdown("#### Funds (Signal 1 & 9 inputs)")
    st.caption("Seed fund vintages by hand now; the EDGAR refresh will add "
               "and link filings automatically in the next phase.")
    for f in bundle["funds"]:
        fc1, fc2, fc3 = st.columns([3, 2, 2])
        fc1.markdown(f"**{f['name']}** ({f.get('source', 'manual')})")
        fc2.caption(f"Filed/vintage: {f.get('filing_date') or '?'}")
        with fc3.popover("Extension / edit"):
            with st.form(f"fund_ext_{f['id']}"):
                ext = st.text_input("Term extension note",
                                    f.get("term_extension_note") or "")
                src = st.text_input("Source link",
                                    f.get("term_extension_source") or "")
                st.link_button(
                    "Search board minutes",
                    f"https://www.google.com/search?q="
                    f"{quote(f['name'] + ' extension board minutes')}")
                if st.form_submit_button("Save"):
                    conn = connect()
                    try:
                        conn.execute(
                            "UPDATE funds SET term_extension_note = ?, "
                            "term_extension_source = ? WHERE id = ?",
                            (ext, src, f["id"]))
                        conn.commit()
                    finally:
                        conn.close()
                    save_backup()
                    st.rerun()
    with st.form("fund_add", clear_on_submit=True):
        fa1, fa2 = st.columns([3, 2])
        fname = fa1.text_input("Fund name")
        fdate = fa2.text_input("Filing date / vintage (YYYY or YYYY-MM-DD)")
        if st.form_submit_button("Add fund") and fname.strip():
            conn = connect()
            try:
                conn.execute(
                    "INSERT INTO funds (gp_id, name, filing_date, source, "
                    "created_at) VALUES (?, ?, ?, 'manual', ?)",
                    (gp_id, fname.strip(), fdate.strip(), now()))
                conn.commit()
            finally:
                conn.close()
            save_backup()
            st.rerun()

    st.markdown("---")
    st.markdown("#### Signal 5 — Team decay checklist (LinkedIn, by eye)")
    with st.form("s5_form"):
        s51, s52, s53 = st.columns(3)
        cur = s51.number_input("Current headcount", 0, 10000,
                               int(gp.get("li_current_headcount") or 0))
        peak = s52.number_input("Peak headcount (est.)", 0, 10000,
                                int(gp.get("li_peak_headcount") or 0))
        junior = s53.selectbox(
            "Junior hire in window?", ["Unknown", "Yes", "No"],
            index={None: 0, 1: 1, 0: 2}.get(gp.get("li_junior_hire_recent"), 0))
        s5notes = st.text_input("Notes", gp.get("li_notes") or "")
        if st.form_submit_button("Save checklist"):
            lifecycle.update_gp(
                gp_id,
                li_current_headcount=cur or None,
                li_peak_headcount=peak or None,
                li_junior_hire_recent={"Unknown": None, "Yes": 1,
                                       "No": 0}[junior],
                li_notes=s5notes)
            save_backup()
            st.rerun()

    st.markdown("---")
    st.markdown("#### Signal 7 — Last confirmed exit")
    with st.form("s7_form"):
        s71, s72 = st.columns(2)
        exit_d = s71.text_input("Last exit date (YYYY-MM-DD, blank = none)",
                                gp.get("last_exit_date") or "")
        st.link_button(
            "Check exits (web search)",
            f"https://www.google.com/search?q="
            f"{quote(gp['name'] + chr(34) + 'completes sale of' + chr(34) + ' OR ' + chr(34) + 'exits its investment' + chr(34))}")
        if st.form_submit_button("Save exit date"):
            lifecycle.update_gp(gp_id,
                                last_exit_date=exit_d.strip() or None,
                                exit_last_checked=now())
            save_backup()
            st.rerun()
    if gp.get("exit_last_checked"):
        st.caption(f"Exit status last checked: {gp['exit_last_checked'][:10]}")

# ═════════════════════ PORTFOLIO COMPANIES TAB ═══════════════════════
with tab_port:
    st.caption("Signals 4 (hold period), 8 (decay), and 10 (UCC liens) "
               "live per company here.")
    for co in bundle["companies"]:
        with st.expander(f"{co['name']} — acquired {co.get('acquisition_date') or '?'}"):
            with st.form(f"co_{co['id']}"):
                p1, p2, p3 = st.columns(3)
                acq = p1.text_input("Acquisition date (YYYY-MM-DD)",
                                    co.get("acquisition_date") or "")
                vert = p2.selectbox(
                    "Vertical", VERTICALS,
                    index=VERTICALS.index(co["vertical"])
                    if co.get("vertical") in VERTICALS else 3)
                ebitda = p3.text_input("Est. EBITDA ($M)",
                                       co.get("ebitda_estimate") or "")
                p4, p5, p6 = st.columns(3)
                web = p4.text_input("Website", co.get("website") or "")
                hq = p5.text_input("HQ state", co.get("hq_state") or "")
                # Signal 8 manual decay boxes
                exec_dep = p6.checkbox(
                    "Exec departures w/o replacement",
                    value=bool(co.get("decay_exec_departures")))
                jobs = st.selectbox(
                    "Active job postings?", ["Unknown", "Yes", "No"],
                    index={None: 0, 1: 1, 0: 2}.get(co.get("decay_job_postings"), 0),
                    key=f"jobs_{co['id']}")
                dnotes = st.text_input("Decay notes",
                                       co.get("decay_notes") or "",
                                       key=f"dn_{co['id']}")
                # Signal 10 UCC findings
                st.markdown("**UCC findings** (use the state search link below)")
                u1, u2, u3 = st.columns(3)
                liens = u1.number_input("Active liens", 0, 99,
                                        int(co.get("ucc_active_liens") or 0))
                lender_chg = u2.checkbox(
                    "Lender changed",
                    value=bool(co.get("ucc_lender_changed")))
                amend = u3.number_input(
                    "Amendments/continuations", 0, 99,
                    int(co.get("ucc_amendment_count") or 0))
                u4, u5 = st.columns(2)
                secured = u4.text_input("Secured parties",
                                        co.get("ucc_secured_parties") or "")
                lastf = u5.text_input("Last filing date (YYYY-MM-DD)",
                                      co.get("ucc_last_filing_date") or "")
                unotes = st.text_input("UCC notes", co.get("ucc_notes") or "",
                                       key=f"un_{co['id']}")
                if st.form_submit_button("Save company"):
                    conn = connect()
                    try:
                        conn.execute(
                            """UPDATE portfolio_companies SET
                               acquisition_date=?, vertical=?, ebitda_estimate=?,
                               website=?, hq_state=?, decay_exec_departures=?,
                               decay_job_postings=?, decay_notes=?,
                               ucc_active_liens=?, ucc_lender_changed=?,
                               ucc_amendment_count=?, ucc_secured_parties=?,
                               ucc_last_filing_date=?, ucc_notes=?
                               WHERE id=?""",
                            (acq or None, vert, ebitda, web,
                             hq.upper(), int(exec_dep),
                             {"Unknown": None, "Yes": 1, "No": 0}[jobs],
                             dnotes, liens or None, int(lender_chg),
                             amend or None, secured, lastf or None,
                             unotes, co["id"]))
                        conn.commit()
                    finally:
                        conn.close()
                    save_backup()
                    st.rerun()
            # helper links (outside the form so they render as links)
            lk = []
            lk.append(("Find acquisition date",
                       f"https://www.google.com/search?q="
                       f"{quote(co['name'] + ' acquired by ' + gp['name'])}"))
            lk.append(("Job search",
                       f"https://www.google.com/search?q="
                       f"{quote(co['name'] + ' jobs hiring')}"))
            ucc_url = UCC_SEARCH_URLS.get((co.get("hq_state") or "").upper())
            if ucc_url:
                lk.append((f"UCC search ({co['hq_state'].upper()} SoS)", ucc_url))
            st.markdown(" · ".join(f"[{n}]({u})" for n, u in lk))

    with st.form("co_add", clear_on_submit=True):
        st.markdown("**Add portfolio company**")
        a1, a2, a3 = st.columns(3)
        cname = a1.text_input("Company name")
        cacq = a2.text_input("Acquisition date (YYYY-MM-DD)")
        cstate = a3.text_input("HQ state (2 letters)")
        if st.form_submit_button("Add company") and cname.strip():
            conn = connect()
            try:
                conn.execute(
                    "INSERT INTO portfolio_companies (gp_id, name, "
                    "acquisition_date, hq_state, created_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (gp_id, cname.strip(), cacq.strip() or None,
                     cstate.strip().upper() or None, now()))
                conn.commit()
            finally:
                conn.close()
            save_backup()
            st.rerun()

# ═════════════════════ CONTACTS & TEMPLATES TAB ══════════════════════
with tab_crm:
    st.markdown("#### Contacts")
    for ct in crm.list_contacts(gp_id=gp_id):
        cc1, cc2, cc3 = st.columns([3, 3, 1])
        star = "⭐ " if ct.get("preferred") else ""
        cc1.markdown(f"{star}**{ct['name']}** — {ct.get('title') or ''} "
                     f"({ct.get('role_tag')})")
        bits = [b for b in [ct.get("email"), ct.get("phone")] if b]
        cc2.caption(" · ".join(bits) or "no contact info")
        if ct.get("linkedin_url"):
            cc2.markdown(f"[LinkedIn]({ct['linkedin_url']})")
        if cc3.button("Delete", key=f"delc_{ct['id']}"):
            crm.delete_contact(ct["id"])
            save_backup()
            st.rerun()
    with st.form("contact_add", clear_on_submit=True):
        st.markdown("**Add contact**")
        n1, n2, n3 = st.columns(3)
        cn = n1.text_input("Name")
        cti = n2.text_input("Title")
        crole = n3.selectbox("Role", ["Managing Partner", "Partner", "CFO",
                                      "IR", "Other"])
        n4, n5, n6 = st.columns(3)
        cem = n4.text_input("Email")
        cph = n5.text_input("Phone")
        cli = n6.text_input("LinkedIn URL")
        cpref = st.checkbox("Preferred contact")
        if st.form_submit_button("Add contact") and cn.strip():
            crm.add_contact(gp_id=gp_id, name=cn.strip(), title=cti,
                            email=cem, phone=cph, linkedin_url=cli,
                            role_tag=crole, preferred=int(cpref))
            save_backup()
            st.rerun()

    st.markdown("---")
    st.markdown("#### ✉️ AI email draft (playbook-tuned for GP outreach)")
    st.caption(
        "Drafts a peer-to-peer buyer inquiry about one portfolio company. "
        "The prompt is hard-wired to NEVER hint at the zombie thesis — "
        "no references to fund age, hold periods, or exit pace."
    )
    from zfs.outreach import get_openai_client, draft_gp_email
    _contacts_ai = crm.list_contacts(gp_id=gp_id)
    _cos_ai = bundle["companies"]
    ai1, ai2 = st.columns(2)
    _ai_contact = ai1.selectbox(
        "To", ["(managing partner)"] + [c["name"] for c in _contacts_ai])
    _ai_focus = ai2.selectbox(
        "Company of interest",
        [c["name"] for c in _cos_ai] or ["(none on file — sector-level)"])
    if st.button("Draft email", use_container_width=True):
        _oc = get_openai_client()
        if _oc is None:
            st.error("Add OPENAI_API_KEY to this app's secrets (or env) "
                     "to enable AI drafting.")
        else:
            try:
                with st.spinner("Drafting..."):
                    _ct = next((c for c in _contacts_ai
                                if c["name"] == _ai_contact), None)
                    _fc = next((c for c in _cos_ai
                                if c["name"] == _ai_focus), None)
                    draft = draft_gp_email(
                        _oc, gp, contact=_ct, funds=bundle["funds"],
                        companies=_cos_ai, focus_company=_fc,
                        sender_name=config["users"][0])
                st.session_state[f"_zdraft_{gp_id}"] = draft
                crm.log_activity(
                    gp_id, "Email",
                    f"Drafted outreach email: {draft['subject']}",
                    direction="outbound", outcome="—",
                    contact_id=_ct["id"] if _ct else None,
                    user=config["users"][0])
                save_backup()
                st.rerun()
            except Exception as e:
                st.error(f"Drafting error: {e}")
    if st.session_state.get(f"_zdraft_{gp_id}"):
        _d = st.session_state[f"_zdraft_{gp_id}"]
        st.text(f"Subject: {_d['subject']}")
        st.code(_d["body"], language=None)
        st.caption("Copy with the icon in the corner, then send from "
                   "Outlook. Log the send in Timeline & Tasks to start "
                   "the follow-up cadence.")

    st.markdown("---")
    st.markdown("#### Template helper")
    tpls = templates_lib.list_templates()
    if tpls:
        tsel = st.selectbox("Template", [t["name"] for t in tpls])
        tpl = next(t for t in tpls if t["name"] == tsel)
        fields = templates_lib.merge_fields_for_gp(gp_id)
        st.text(f"Subject: {templates_lib.render(tpl.get('subject') or '', fields)}")
        st.code(templates_lib.render(tpl["body"], fields), language=None)
        st.caption("Use the copy icon in the corner of the block, then "
                   "send from Outlook.")
    else:
        st.caption("No templates yet — create them on the **Templates** page.")

# ═════════════════════ TIMELINE & TASKS TAB ══════════════════════════
with tab_time:
    lcol, rcol = st.columns(2)
    with lcol:
        st.markdown("#### Log an activity")
        with st.form("act_form", clear_on_submit=True):
            contacts = crm.list_contacts(gp_id=gp_id)
            v1, v2 = st.columns(2)
            a_type = v1.selectbox("Type", ACTIVITY_TYPES)
            a_dir = v2.selectbox("Direction", ["outbound", "inbound"])
            v3, v4 = st.columns(2)
            a_out = v3.selectbox("Outcome", ACTIVITY_OUTCOMES)
            a_who = v4.selectbox(
                "Contact", ["—"] + [c["name"] for c in contacts])
            a_user = st.selectbox("Logged by", config["users"])
            a_sum = st.text_input("Summary")
            if st.form_submit_button("Log activity") and a_sum.strip():
                cid = next((c["id"] for c in contacts if c["name"] == a_who),
                           None)
                res = crm.log_activity(gp_id, a_type, a_sum.strip(),
                                       direction=a_dir, outcome=a_out,
                                       contact_id=cid, user=a_user)
                if res.get("auto_task"):
                    st.toast(f"Cadence created: {res['auto_task']}")
                if res.get("suggestion"):
                    st.session_state["_detail_suggest"] = res["suggestion"]
                save_backup()
                st.rerun()
        if st.session_state.get("_detail_suggest"):
            sug = st.session_state["_detail_suggest"]
            b1, b2 = st.columns(2)
            b1.info(f"Suggestion: move to **{sug}**?")
            if b2.button(f"Move to {sug}"):
                lifecycle.set_status(gp_id, sug, user=config["users"][0])
                st.session_state.pop("_detail_suggest")
                save_backup()
                st.rerun()

        st.markdown("#### Open tasks")
        for t in crm.open_tasks(gp_id=gp_id):
            t1, t2, t3 = st.columns([4, 1, 1])
            t1.markdown(f"**{t['description']}**  \n:gray[due {t['due_date']} "
                        f"· {t['priority']} · {t['assigned_to']}"
                        + (" · auto]" if t["auto_generated"] else "]"))
            if t2.button("Done", key=f"td_{t['id']}"):
                crm.complete_task(t["id"])
                save_backup()
                st.rerun()
            if t["auto_generated"] and t3.button("Dismiss", key=f"tx_{t['id']}"):
                crm.dismiss_task(t["id"])
                save_backup()
                st.rerun()
        with st.form("task_add", clear_on_submit=True):
            ta1, ta2, ta3 = st.columns(3)
            tdesc = ta1.text_input("New task")
            tdue = ta2.date_input("Due", value=date.today())
            tass = ta3.selectbox("Assign to", config["users"])
            tpri = st.selectbox("Priority", ["High", "Medium", "Low"], index=1)
            if st.form_submit_button("Add task") and tdesc.strip():
                crm.add_task(gp_id, tdesc.strip(), tdue.isoformat(),
                             priority=tpri, assigned_to=tass)
                save_backup()
                st.rerun()

    with rcol:
        st.markdown("#### Timeline")
        for e in crm.timeline(gp_id):
            icon = {"activity": "📞", "status": "🔀", "kill": "💀",
                    "resurrect": "🪦"}.get(e["kind"], "•")
            who = f" → {e['contact_name']}" if e.get("contact_name") else ""
            meta = " · ".join(x for x in [
                (e.get("type") or ""), (e.get("direction") or ""),
                (e.get("outcome") if e.get("outcome") not in (None, "—")
                 else "")] if x)
            st.markdown(f"{icon} `{e['timestamp'][:10]}` {e['summary']}{who}"
                        + (f"  \n:gray[{meta}]" if meta else ""))
