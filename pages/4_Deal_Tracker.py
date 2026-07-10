import streamlit as st
from datetime import datetime, date, time as dtime, timedelta, timezone

from lib import crm
from lib.crm import STATUSES, TERMINAL_STATUSES, ACTIVITY_TYPES
from lib.outreach import generate_custom_reminder_ics, RECURRENCE_OPTIONS

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
            )
        if len(attention) > 15:
            st.caption(f"...and {len(attention) - 15} more below.")

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

# ---------------------------------------------------------------------------
# Metrics + filters
# ---------------------------------------------------------------------------
all_deals = crm.list_deals()
active_deals = [d for d in all_deals if d["status"] not in TERMINAL_STATUSES]
opps = [d for d in all_deals if d["status"] == "Opportunity"]

m1, m2, m3, m4 = st.columns(4)
m1.metric("Total Deals", len(all_deals))
m2.metric("Active", len(active_deals))
m3.metric("Opportunities", len(opps))
m4.metric("Needs Attention", len(attention))

f1, f2 = st.columns([2, 1])
with f1:
    status_filter = st.multiselect(
        "Filter by status",
        STATUSES,
        default=[],
        placeholder="All statuses",
        format_func=lambda s: f"{STATUS_ICONS.get(s, '')} {s}",
    )
with f2:
    search = st.text_input("Search", placeholder="Company, contact, niche...")

_all_matching = crm.list_deals(
    statuses=status_filter or None,
    search=search.strip() or None,
)

# Terminal-status deals live in the Archive unless explicitly filtered for
if status_filter:
    deals = _all_matching
    archived = []
else:
    deals = [d for d in _all_matching if d["status"] not in TERMINAL_STATUSES]
    archived = [d for d in _all_matching if d["status"] in TERMINAL_STATUSES]

if not deals:
    if not all_deals:
        st.info(
            "No deals tracked yet. Use **Import past deals** above to seed the "
            "tracker from your search history, or add one manually."
        )
    elif archived:
        st.info("No active deals — everything matching is in the Archive below.")
    else:
        st.info("No deals match the current filters.")

# ---------------------------------------------------------------------------
# Deal cards
# ---------------------------------------------------------------------------
for deal in deals:
    _id = deal["id"]
    icon = STATUS_ICONS.get(deal["status"], "•")
    header = (
        f"{icon} {deal['company']} — {deal['status']} · "
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
            )
            if new_status != deal["status"]:
                crm.set_status(_id, new_status, deal["status"])
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
                crm.backup_to_github()
                st.success("Notes saved.")
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
                crm.backup_to_github()
                st.rerun()
            if deal.get("next_followup") and fu_clear.button("Clear", key=f"fuclear_{_id}"):
                crm.update_deal(_id, next_followup=None)
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
            if st.button(
                f"Sync to Salesforce ({len(unsynced)} new)",
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
            st.caption(
                "Sync pushes un-synced activities as completed Tasks and logs "
                "the current status + notes to the Salesforce Account."
            )

# ---------------------------------------------------------------------------
# Archive — terminal-status deals, grouped and out of the way
# ---------------------------------------------------------------------------
if archived:
    st.markdown("---")
    with st.expander(f"🗄️ Archive ({len(archived)})"):
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
                    crm.backup_to_github()
                    st.rerun()
