"""Templates — write and manage the outreach email library.

Merge fields: {first_name} {firm_name} {portfolio_company} {fund_vintage}
The GP Detail page renders any template with a specific GP's data.
"""

import streamlit as st

from zfs.ui import page_setup, save_backup
from zfs import templates_lib

page_setup("Templates — Zombie Fund Screener")
st.title("📝 Templates")
st.caption("Merge fields: `{first_name}` `{firm_name}` "
           "`{portfolio_company}` `{fund_vintage}` — filled per GP on the "
           "GP Detail page.")

tpls = templates_lib.list_templates()

for t in tpls:
    with st.expander(f"{t['name']}"):
        with st.form(f"tpl_{t['id']}"):
            name = st.text_input("Name", t["name"])
            subject = st.text_input("Subject", t.get("subject") or "")
            body = st.text_area("Body", t["body"], height=220)
            c1, c2 = st.columns(2)
            if c1.form_submit_button("Save"):
                templates_lib.save_template(name, subject, body, t["id"])
                save_backup()
                st.rerun()
            if c2.form_submit_button("Delete"):
                templates_lib.delete_template(t["id"])
                save_backup()
                st.rerun()

with st.form("tpl_new", clear_on_submit=True):
    st.markdown("**New template**")
    name = st.text_input("Name", placeholder="e.g., First touch")
    subject = st.text_input("Subject",
                            placeholder="e.g., {firm_name} — quick question")
    body = st.text_area("Body", height=220, placeholder=(
        "{first_name},\n\nI've followed {firm_name} since your "
        "{fund_vintage} fund...\n"))
    if st.form_submit_button("Create") and name.strip() and body.strip():
        templates_lib.save_template(name.strip(), subject, body)
        save_backup()
        st.rerun()
