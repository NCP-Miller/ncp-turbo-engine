import json
import streamlit as st
from datetime import datetime, date, time as dtime, timedelta, timezone

from lib import crm
from lib.crm import STATUSES, TERMINAL_STATUSES, ACTIVITY_TYPES
from lib.outreach import (
    generate_custom_reminder_ics, generate_followup_ics,
    make_mailto_url, RECURRENCE_OPTIONS,
)

def _check_password():
    try:
        app_password = st.secrets["APP_PASSWORD"]
    except (FileNotFoundError, KeyError):
        st.error("APP_PASSWORD is not configured. Add it to .streamlit/secrets.toml.")
        return False

    def _password_entered():
        if st.session_state.get("password") == app_password:
            st.session_state["password_correct"] = True
        else:
            st.session_state["password_correct"] = False
            st.session_state["password_attempted"] = True

    if st.session_state.get("password_correct"):
        return True

    st.text_input(
        "Enter Password", type="password",
        on_change=_password_entered, key="password",
    )
    if st.session_state.get("password_attempted"):
        st.error("Password incorrect")
    return False


if not _check_password():
    st.stop()


STATUS_ICONS = {
    "New": "🆕",
    "Outreach Active": "📤",
    "In Dialogue": "💬",
    "Meeting Scheduled": "📅",
    "Opportunity": "⭐",
    "Revisit Later": "⏰",
    "Closed – No Response": "🔇",
    "Contacted – No Opportunity": "🚫",
    "Not a Fit": "❌",
}

ACTIVE_STATUSES = [s for s in STATUSES if s not in TERMINAL_STATUSES]


def _fmt_ts(iso_str):
    if not iso_str:
        return "never"
    try:
        dt = datetime.fromisoformat(iso_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        days = (datetime.now(timezone.utc) - dt).days
        stamp = dt.strftime("%b %d, %Y")
        if days == 0:
            return f"{stamp} (today)"
        if days == 1:
            return f"{stamp} (yesterday)"
        return f"{stamp} ({days}d ago)"
    except (ValueError, TypeError):
        return iso_str


def _sf_login():
    from lib.salesforce import sf_login
    from lib.api_clients import get_secret
    return sf_login(
        get_secret("SF_USERNAME"),
        get_secret("SF_PASSWORD"),
        get_secret("SF_CONSUMER_KEY"),
        get_secret("SF_CONSUMER_SECRET"),
        get_secret("SF_SECURITY_TOKEN", ""),
    )


def _render_deal_card(deal):
    """Full editable deal card inside an expander."""
    _id = deal["id"]
    header = (
        f"{deal['company']} · "
        f"last activity: {_fmt_ts(deal.get('last_activity') or deal.get('created_at'))}"
    )
    with st.expander(header):
        top_l, top_r = st.columns([3, 2])

        with top_l:
            contact_bits = [
                b for b in [
                    deal.get("contact_name"),
                    deal.get("title"),
                    deal.get("email"),
                    deal.get("phone"),
                ] if b
            ]
            if contact_bits:
                st.markdown("**Contact:** " + " · ".join(contact_bits))
            meta_bits = [
                b for b in [
                    deal.get("website"),
                    f"{deal.get('city')}, {deal.get('state')}"
                    if deal.get("city") else deal.get("state"),
                    deal.get("niche"),
                ] if b
            ]
            if meta_bits:
                st.caption(" · ".join(meta_bits))
            st.caption(f"Sourced {(deal.get('created_at') or '?')[:10]}")
            if deal.get("sf_account_id"):
                st.caption(f"Salesforce Account: `{deal['sf_account_id']}`")

        with top_r:
            current = deal["status"] if deal["status"] in STATUSES else "New"
            new_status = st.selectbox(
                "Status",
                STATUSES,
                index=STATUSES.index(current),
                key=f"status_{_id}",
                format_func=lambda s: f"{STATUS_ICONS.get(s, '')} {s}",
                help="Changing the status moves this deal to that folder.",
            )
            if new_status != deal["status"]:
                crm.set_status(_id, new_status, deal["status"])
                crm.auto_sync_deal(_id)
                crm.backup_to_github()
                st.rerun()

        # ── Notes + follow-up date ────────────────────────────────
        n_col, f_col = st.columns([3, 2])
        with n_col:
            notes_val = st.text_area(
                "Notes",
                value=deal.get("notes") or "",
                key=f"notes_{_id}",
                height=90,
            )
            if st.button("Save Notes", key=f"savenotes_{_id}"):
                crm.update_deal(_id, notes=notes_val)
                _synced = crm.auto_sync_deal(_id)
                crm.backup_to_github()
                st.success("Notes saved." + (" Synced to Salesforce." if _synced else ""))
        with f_col:
            existing_fu = None
            if deal.get("next_followup"):
                try:
                    existing_fu = datetime.fromisoformat(
                        deal["next_followup"]
                    ).date()
                except (ValueError, TypeError):
                    existing_fu = None
            fu_date = st.date_input(
                "Next follow-up",
                value=existing_fu or (date.today() + timedelta(days=7)),
                key=f"fu_{_id}",
            )
            fu_set, fu_clear = st.columns(2)
            if fu_set.button("Set", key=f"fuset_{_id}"):
                crm.update_deal(_id, next_followup=fu_date.isoformat())
                crm.log_activity(_id, "Note", f"Follow-up set for {fu_date.isoformat()}")
                crm.auto_sync_deal(_id)
                crm.backup_to_github()
                st.rerun()
            if deal.get("next_followup") and fu_clear.button("Clear", key=f"fuclear_{_id}"):
                crm.update_deal(_id, next_followup=None)
                crm.auto_sync_deal(_id)
                crm.backup_to_github()
                st.rerun()
            if deal.get("next_followup"):
                st.caption(f"Currently: {deal['next_followup'][:10]}")
                try:
                    _fu_dt = datetime.combine(
                        datetime.fromisoformat(deal["next_followup"]).date(),
                        dtime(9, 0),
                    )
                    _fu_ics = generate_custom_reminder_ics(
                        deal["company"], "Follow-up", _fu_dt,
                        duration_minutes=15,
                        contact_name=deal.get("contact_name") or "",
                        phone=deal.get("phone") or "",
                        email=deal.get("email") or "",
                        notes=deal.get("notes") or "",
                    )
                    st.download_button(
                        "📅 Add to Outlook (9:00 AM)",
                        data=_fu_ics,
                        file_name=f"followup_{deal['company'].replace(' ', '_')}.ics",
                        mime="text/calendar",
                        key=f"fuics_{_id}",
                        use_container_width=True,
                    )
                except (ValueError, TypeError):
                    pass

        # ── Outreach tools: memo, email draft, reminder sequence ─
        st.markdown("**Outreach tools**")
        _row_data = {}
        if deal.get("row_json"):
            try:
                _row_data = json.loads(deal["row_json"])
            except (ValueError, TypeError):
                _row_data = {}

        if deal.get("memo"):
            if st.toggle("📄 Show investment memo", key=f"memo_{_id}"):
                st.markdown(deal["memo"])

        ot1, ot2, ot3 = st.columns(3)
        with ot1:
            if st.button("✉️ Draft Outreach Email", key=f"draft_{_id}",
                         use_container_width=True):
                try:
                    from lib.outreach import draft_cold_email
                    from lib.api_clients import load_api_keys, make_openai_client
                    _keys = load_api_keys()
                    _oc = make_openai_client(api_key=_keys["OPENAI_API_KEY"])
                    _thesis = {}
                    try:
                        with open("ncp_thesis.json") as f:
                            _thesis = json.load(f)
                    except Exception:
                        pass
                    enriched = dict(_row_data)
                    enriched["Company"] = deal["company"]
                    enriched["_niche"] = deal.get("niche") or enriched.get("_niche", "")
                    if deal.get("contact_name"):
                        enriched["CEO/Owner Name"] = deal["contact_name"]
                    if deal.get("email"):
                        enriched["Email"] = deal["email"]
                    if deal.get("phone"):
                        enriched["Phone"] = deal["phone"]
                    if deal.get("title"):
                        enriched["Title"] = deal["title"]
                    draft = draft_cold_email(_oc, enriched, _thesis)
                    st.session_state[f"_crm_draft_subj_{_id}"] = draft["subject"]
                    st.session_state[f"_crm_draft_body_{_id}"] = draft["body"]
                    crm.log_activity(
                        _id, "Email",
                        f"Drafted outreach email: {draft['subject']}",
                    )
                    st.rerun()
                except Exception as e:
                    st.error(f"Email drafting error: {e}")
        with ot2:
            _seq_ics = generate_followup_ics(
                deal["company"],
                deal.get("contact_name") or "Contact",
                phone=deal.get("phone") or "",
                email=deal.get("email") or "",
            )
            st.download_button(
                "📅 Follow-up Sequence (.ics)",
                data=_seq_ics,
                file_name=f"followup_{deal['company'].replace(' ', '_')}.ics",
                mime="text/calendar",
                key=f"seqics_{_id}",
                use_container_width=True,
                help="The standard 6-touch reminder sequence (email, LinkedIn, "
                     "calls) timed from the moment you download.",
                on_click=crm.log_activity,
                args=(_id, "Note", "Downloaded follow-up reminder sequence"),
            )
        with ot3:
            st.page_link(
                "pages/2_Sourcing_Pipeline.py",
                label="🔍 Open Sourcing App",
                use_container_width=True,
            )
            if deal.get("project"):
                st.caption(f"From project: {deal['project']}")

        _draft_body = st.session_state.get(f"_crm_draft_body_{_id}")
        if _draft_body:
            st.markdown(
                f"**Subject:** "
                f"{st.session_state.get(f'_crm_draft_subj_{_id}', '')}"
            )
            _edited_body = st.text_area(
                "Email draft (edit before sending)",
                value=_draft_body,
                height=200,
                key=f"drafted_{_id}",
            )
            _mailto = make_mailto_url(
                deal.get("email") or "",
                st.session_state.get(f"_crm_draft_subj_{_id}", ""),
                _edited_body,
            )
            st.link_button("Open in Outlook", url=_mailto)

        st.markdown("---")

        # ── Log activity ─────────────────────────────────────────
        st.markdown("**Log activity**")
        with st.form(key=f"act_form_{_id}", clear_on_submit=True):
            a1, a2 = st.columns([1, 3])
            a_type = a1.selectbox("Type", ACTIVITY_TYPES, key=f"atype_{_id}")
            a_summary = a2.text_input(
                "What happened?",
                placeholder="e.g., Left voicemail, sent intro email...",
                key=f"asum_{_id}",
            )
            if st.form_submit_button("Log") and a_summary.strip():
                crm.log_activity(_id, a_type, a_summary.strip())
                crm.auto_sync_deal(_id)
                crm.backup_to_github()
                st.rerun()

        activities = crm.list_activities(_id, limit=25)
        if activities:
            for act in activities:
                sync_badge = " ✓SF" if act.get("synced_to_sf") else ""
                st.markdown(
                    f"- `{(act['timestamp'] or '')[:10]}` **{act['type']}** — "
                    f"{act['summary']}{sync_badge}"
                )

        st.markdown("---")

        # ── Tailored Outlook reminder ────────────────────────────
        st.markdown("**Create Outlook reminder**")
        with st.form(key=f"rem_form_{_id}"):
            r1, r2, r3 = st.columns(3)
            r_date = r1.date_input(
                "Date", value=date.today() + timedelta(days=1), key=f"rdate_{_id}"
            )
            r_time = r2.time_input("Time", value=dtime(9, 0), key=f"rtime_{_id}")
            r_dur = r3.selectbox(
                "Duration", [15, 30, 45, 60],
                format_func=lambda m: f"{m} min", key=f"rdur_{_id}",
            )
            r4, r5, r6 = st.columns(3)
            r_action = r4.selectbox(
                "Action", ["Call", "Email", "LinkedIn", "Text"], key=f"ract_{_id}"
            )
            r_recur = r5.selectbox(
                "Repeats", list(RECURRENCE_OPTIONS.keys()), key=f"rrec_{_id}"
            )
            r_count = r6.number_input(
                "Occurrences", min_value=1, max_value=52, value=4,
                key=f"rcount_{_id}",
                help="How many times a repeating reminder fires. Ignored for one-time.",
            )
            r_notes = st.text_input("Reminder notes (optional)", key=f"rnotes_{_id}")
            if st.form_submit_button("Create Reminder"):
                start_dt = datetime.combine(r_date, r_time)
                ics = generate_custom_reminder_ics(
                    deal["company"], r_action, start_dt,
                    duration_minutes=r_dur,
                    contact_name=deal.get("contact_name") or "",
                    phone=deal.get("phone") or "",
                    email=deal.get("email") or "",
                    notes=r_notes,
                    recurrence=r_recur,
                    occurrences=int(r_count),
                )
                st.session_state[f"_rem_ics_{_id}"] = ics
                recur_label = (
                    f", {r_recur.lower()} ×{int(r_count)}"
                    if RECURRENCE_OPTIONS.get(r_recur) else ""
                )
                crm.log_activity(
                    _id, "Note",
                    f"Created {r_action} reminder for "
                    f"{r_date.isoformat()} {r_time.strftime('%H:%M')}{recur_label}",
                )
                crm.auto_sync_deal(_id)
                crm.backup_to_github()

        if st.session_state.get(f"_rem_ics_{_id}"):
            st.download_button(
                "⬇️ Download reminder (.ics)",
                data=st.session_state[f"_rem_ics_{_id}"],
                file_name=f"reminder_{deal['company'].replace(' ', '_')}.ics",
                mime="text/calendar",
                key=f"remdl_{_id}",
            )

        st.markdown("---")

        # ── Salesforce sync ──────────────────────────────────────
        sf_col, del_col = st.columns([1, 3])
        with sf_col:
            unsynced = crm.unsynced_activities(_id)
            _linked = bool(deal.get("sf_account_id"))
            _sync_label = (
                f"Sync to Salesforce ({len(unsynced)} pending)" if _linked
                else f"Link to Salesforce & sync ({len(unsynced)} pending)"
            )
            if st.button(
                _sync_label,
                key=f"sfsync_{_id}",
                use_container_width=True,
            ):
                try:
                    from lib.salesforce import sync_deal_to_salesforce
                    sf = _sf_login()
                    acct, cont, synced_ids = sync_deal_to_salesforce(
                        sf, deal, unsynced,
                    )
                    crm.mark_activities_synced(synced_ids)
                    crm.update_deal(_id, sf_account_id=acct, sf_contact_id=cont)
                    crm.backup_to_github()
                    st.success(
                        f"Synced {len(synced_ids)} activities + status — "
                        f"Account `{acct}`"
                    )
                    st.rerun()
                except Exception as e:
                    st.error(f"Salesforce sync error: {e}")
        with del_col:
            if _linked:
                st.caption(
                    "✅ Linked — every status change, note, follow-up, and "
                    "activity auto-syncs to Salesforce. Activities become "
                    "completed Tasks; status + notes update one summary Task; "
                    "the follow-up date maintains an open Task with that due date."
                )
            else:
                st.caption(
                    "Not linked yet — click once to create/find the Salesforce "
                    "Account and push everything. After linking, all changes "
                    "auto-sync from then on."
                )


st.title("📇 Deal Tracker")
st.caption(
    "Every target prospect in one place — statuses, notes, activity log, "
    "tailored Outlook reminders, and Salesforce sync."
)

crm.init_db()
crm.restore_from_github_if_empty()

# ---------------------------------------------------------------------------
# Needs Attention
# ---------------------------------------------------------------------------
attention = crm.deals_needing_attention()
if attention:
    with st.container(border=True):
        st.markdown(f"### 🔔 Needs Attention ({len(attention)})")
        for d in attention[:15]:
            icon = STATUS_ICONS.get(d["status"], "•")
            st.markdown(
                f"- {icon} **{d['company']}** — {d['attention_reason']}"
                f" · last touch: {_fmt_ts(d.get('last_activity') or d.get('created_at'))}"
                f" · in folder: {d['status']}"
            )
        if len(attention) > 15:
            st.caption(f"...and {len(attention) - 15} more in the folders below.")

# ---------------------------------------------------------------------------
# Import + manual add
# ---------------------------------------------------------------------------
with st.expander("⚙️ Import & add deals"):
    imp_col, add_col = st.columns(2)
    with imp_col:
        st.markdown("**Import from past searches**")
        st.caption(
            "Pulls in every investment memo and every company you marked "
            "👍 Interested — from local project files, the GitHub backup, "
            "and the feedback log. Skips companies you rejected."
        )
        sources = crm.backfill_sources()
        st.caption(
            f"Sources found: {len(sources['local_dbs'])} local project file(s), "
            f"{len(sources['github_projects'])} GitHub-backed project(s), "
            f"{sources['feedback_entries']} feedback entries."
        )
        if st.button("Import past deals", use_container_width=True):
            with st.spinner("Scanning local projects, GitHub backups, and feedback..."):
                result = crm.backfill_from_history()
                crm.backup_to_github()
            st.success(
                f"Imported {result['created']} deals "
                f"(scanned {result['local_dbs_scanned']} local + "
                f"{result['github_projects_scanned']} GitHub projects, "
                f"{result['feedback_entries']} feedback entries). "
                f"Skipped {result['skipped_rejected']} rejected, "
                f"{result['already_tracked']} already tracked."
            )
            if result["created"]:
                st.rerun()
    with add_col:
        st.markdown("**Add a deal manually**")
        with st.form("manual_add", clear_on_submit=True):
            m_company = st.text_input("Company name")
            m_contact = st.text_input("Contact name")
            m_email = st.text_input("Email")
            m_phone = st.text_input("Phone")
            m_niche = st.text_input("Niche / sector")
            if st.form_submit_button("Add Deal") and m_company.strip():
                deal_id = crm.upsert_deal(
                    m_company.strip(),
                    row={"CEO/Owner Name": m_contact, "Email": m_email,
                         "Phone": m_phone},
                    niche=m_niche or None, source="manual",
                )
                crm.log_activity(deal_id, "Note", "Added manually")
                crm.backup_to_github()
                st.rerun()

    st.markdown("---")
    st.markdown("**Salesforce catch-up**")
    st.caption(
        "Push everything logged so far — statuses, notes, follow-up dates, "
        "and activities — to Salesforce in one pass. Linked deals always "
        "sync; check the box to also link deals you've worked on that "
        "aren't in Salesforce yet (creates their Accounts). Untouched "
        "imports are never pushed."
    )
    _inc_unlinked = st.checkbox(
        "Also link deals not yet in Salesforce (creates Accounts)",
        value=True,
        key="syncall_unlinked",
    )
    if st.button("🔄 Sync All to Salesforce", use_container_width=True):
        with st.spinner("Syncing all deals to Salesforce..."):
            _res = crm.sync_all_to_salesforce(include_unlinked=_inc_unlinked)
            crm.backup_to_github()
        if _res.get("error"):
            st.error(_res["error"])
        else:
            st.success(
                f"Synced {_res['deals_synced']} deals "
                f"({_res['activities_synced']} activities). "
                f"Newly linked: {_res['newly_linked']}. "
                f"Skipped unlinked: {_res['skipped_unlinked']}."
            )
            for _err in _res.get("errors", [])[:5]:
                st.warning(_err)

# ---------------------------------------------------------------------------
# Metrics + search
# ---------------------------------------------------------------------------
all_deals = crm.list_deals()
active_deals = [d for d in all_deals if d["status"] not in TERMINAL_STATUSES]
opps = [d for d in all_deals if d["status"] == "Opportunity"]

m1, m2, m3, m4 = st.columns(4)
m1.metric("Total Deals", len(all_deals))
m2.metric("Active", len(active_deals))
m3.metric("Opportunities", len(opps))
m4.metric("Needs Attention", len(attention))

search = st.text_input("Search", placeholder="Company, contact, niche...")

_all_matching = crm.list_deals(search=search.strip() or None)

if not all_deals:
    st.info(
        "No deals tracked yet. Use **Import past deals** above to seed the "
        "tracker from your search history, or add one manually."
    )
elif not _all_matching:
    st.info("No deals match the current search.")

# ---------------------------------------------------------------------------
# Status folders — persistent selector so you never lose your spot
# ---------------------------------------------------------------------------
if _all_matching:
    buckets = {
        s: [d for d in _all_matching if d["status"] == s]
        for s in ACTIVE_STATUSES
    }
    archived = [d for d in _all_matching if d["status"] in TERMINAL_STATUSES]

    _folder_counts = {s: len(buckets[s]) for s in ACTIVE_STATUSES}
    _folder_counts["Archive"] = len(archived)
    _folder_options = ACTIVE_STATUSES + ["Archive"]

    def _folder_label(s):
        icon = "🗄️" if s == "Archive" else STATUS_ICONS.get(s, "")
        return f"{icon} {s} ({_folder_counts[s]})"

    selected_folder = st.radio(
        "Folder",
        _folder_options,
        format_func=_folder_label,
        horizontal=True,
        key="deal_folder",
        label_visibility="collapsed",
    )
    st.markdown("---")

    if selected_folder != "Archive":
        bucket = buckets[selected_folder]
        if not bucket:
            st.caption(f"No deals in {selected_folder}.")
        for deal in bucket:
            _render_deal_card(deal)

    # ── Archive folder: terminal statuses, grouped, sorted by date sourced ──
    else:
        if not archived:
            st.caption("Nothing archived yet.")
        else:
            st.caption(
                "Deals marked Closed – No Response, Contacted – No Opportunity, "
                "or Not a Fit. Sorted by date sourced (newest first). "
                "Reopen moves a deal back to New."
            )
            for arch_status in [
                "Closed – No Response",
                "Contacted – No Opportunity",
                "Not a Fit",
            ]:
                group = [d for d in archived if d["status"] == arch_status]
                if not group:
                    continue
                group.sort(key=lambda d: d.get("created_at") or "", reverse=True)
                st.markdown(
                    f"**{STATUS_ICONS.get(arch_status, '')} {arch_status} ({len(group)})**"
                )
                for d in group:
                    c1, c2, c3, c4 = st.columns([4, 2, 3, 1])
                    label = f"**{d['company']}**"
                    if d.get("niche"):
                        label += f" · {d['niche']}"
                    c1.markdown(label)
                    c2.caption(f"Sourced {(d.get('created_at') or '?')[:10]}")
                    c3.caption(
                        f"Last activity: "
                        f"{_fmt_ts(d.get('last_activity') or d.get('created_at'))}"
                    )
                    if c4.button("Reopen", key=f"reopen_{d['id']}"):
                        crm.set_status(d["id"], "New", d["status"])
                        crm.auto_sync_deal(d["id"])
                        crm.backup_to_github()
                        st.rerun()
