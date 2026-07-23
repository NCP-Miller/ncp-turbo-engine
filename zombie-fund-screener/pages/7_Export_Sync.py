"""Export / Sync — CSVs for analysis and Salesforce round-trip.

Every GP carries a stable external ID (ZFS-<id>) so repeated Salesforce
imports UPDATE records instead of duplicating them. Contacts use
ZFSC-<id>. The import path reads a Salesforce report CSV back in and
previews every change before anything is written.
"""

import io

import pandas as pd
import streamlit as st

from zfs.ui import page_setup, save_backup
from zfs import lifecycle, scoring
from zfs.db import connect, PIPELINE_STATUSES
from zfs.settings import get_signal_settings, get_cadence

page_setup("Export / Sync — Zombie Fund Screener")
st.title("📤 Export / Sync")

settings = get_signal_settings()
cadence = get_cadence()
pool, _ = scoring.score_all(settings, cadence.get("min_signals", 1))

conn = connect()
try:
    contacts = [dict(r) for r in conn.execute(
        "SELECT c.*, g.name AS gp_name FROM contacts c "
        "LEFT JOIN gps g ON g.id = c.gp_id").fetchall()]
    activities = [dict(r) for r in conn.execute(
        "SELECT e.*, g.name AS gp_name FROM events e "
        "JOIN gps g ON g.id = e.gp_id WHERE e.kind = 'activity'").fetchall()]
    open_tasks = [dict(r) for r in conn.execute(
        "SELECT t.*, g.name AS gp_name FROM tasks t "
        "JOIN gps g ON g.id = t.gp_id "
        "WHERE t.done = 0 AND t.dismissed = 0").fetchall()]
finally:
    conn.close()


def _csv_button(label, df, filename):
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    st.download_button(label, buf.getvalue(), file_name=filename,
                       mime="text/csv", use_container_width=True)


st.markdown("#### Ranked candidate list")
rank_df = pd.DataFrame([{
    "External_Id": f"ZFS-{r['gp']['id']}",
    "GP": r["gp"]["name"],
    "Score": r["score"],
    "Signals_Fired": " ".join(s.upper() for s in r["fired"]),
    "Pension_Verified": "Yes" if r["verified"] else "",
    "Status": r["gp"]["status"],
    "State": r["gp"].get("state") or "",
    "Website": r["gp"].get("website") or "",
} for r in pool])
st.dataframe(rank_df, use_container_width=True, hide_index=True)
_csv_button("⬇️ Ranked list CSV", rank_df, "zombie_ranked_list.csv")

st.markdown("---")
st.markdown("#### Salesforce-formatted exports")
c1, c2, c3 = st.columns(3)

with c1:
    acc_df = pd.DataFrame([{
        "External_Id__c": f"ZFS-{g['id']}",
        "Name": g["name"],
        "Website": g.get("website") or "",
        "BillingState": g.get("state") or "",
        "Description": (f"Zombie screener status: {g['status']}. "
                        f"{g.get('notes') or ''}").strip(),
    } for g in lifecycle.list_gps()])
    _csv_button(f"Accounts ({len(acc_df)})", acc_df, "sf_accounts.csv")

with c2:
    def _split(nm):
        parts = (nm or "").split(None, 1)
        return (parts[0] if parts else "Unknown",
                parts[1] if len(parts) > 1 else (parts[0] if parts else "Contact"))
    con_df = pd.DataFrame([{
        "External_Id__c": f"ZFSC-{c['id']}",
        "FirstName": _split(c["name"])[0],
        "LastName": _split(c["name"])[1],
        "Title": c.get("title") or "",
        "Email": c.get("email") or "",
        "Phone": c.get("phone") or "",
        "Account_External_Id__c": f"ZFS-{c['gp_id']}" if c.get("gp_id") else "",
    } for c in contacts])
    _csv_button(f"Contacts ({len(con_df)})", con_df, "sf_contacts.csv")

with c3:
    task_rows = [{
        "Subject": f"{a.get('type') or 'Activity'}: {a['summary'][:180]}",
        "ActivityDate": (a["timestamp"] or "")[:10],
        "Status": "Completed",
        "Description": a["summary"],
        "Account_External_Id__c": f"ZFS-{a['gp_id']}",
    } for a in activities]
    task_rows += [{
        "Subject": t["description"][:200],
        "ActivityDate": t["due_date"],
        "Status": "Not Started",
        "Description": f"Priority {t['priority']} — assigned {t['assigned_to']}",
        "Account_External_Id__c": f"ZFS-{t['gp_id']}",
    } for t in open_tasks]
    task_df = pd.DataFrame(task_rows)
    _csv_button(f"Tasks ({len(task_df)})", task_df, "sf_tasks.csv")

st.markdown("---")
st.markdown("#### Import a Salesforce report (sync statuses back)")
st.caption("Upload a report CSV containing the `External_Id__c` (ZFS-…) "
           "column and a `Status` column. You'll preview every change "
           "before anything is written.")
up = st.file_uploader("Salesforce report CSV", type=["csv"])
if up is not None:
    try:
        imp = pd.read_csv(up)
        id_col = next((c for c in imp.columns
                       if "external" in c.lower()), None)
        st_col = next((c for c in imp.columns
                       if c.lower().strip() == "status"), None)
        if not id_col or not st_col:
            st.error("Need an External Id column (ZFS-…) and a Status column.")
        else:
            changes = []
            for _, row in imp.iterrows():
                ext = str(row[id_col] or "")
                if not ext.startswith("ZFS-"):
                    continue
                try:
                    gid = int(ext.replace("ZFS-", ""))
                except ValueError:
                    continue
                gp = lifecycle.get_gp(gid)
                new_status = str(row[st_col] or "").strip()
                if gp and new_status in PIPELINE_STATUSES \
                        and new_status != gp["status"]:
                    changes.append((gid, gp["name"], gp["status"], new_status))
            if not changes:
                st.info("No status changes detected.")
            else:
                st.markdown("**Preview of changes:**")
                for _, name, old, new in changes:
                    st.markdown(f"- {name}: {old} → **{new}**")
                if st.button(f"Apply {len(changes)} change(s)"):
                    for gid, _, _, new in changes:
                        lifecycle.set_status(gid, new, user="Salesforce sync")
                    save_backup()
                    st.success("Applied.")
                    st.rerun()
    except Exception as e:
        st.error(f"Could not read that CSV: {e}")
