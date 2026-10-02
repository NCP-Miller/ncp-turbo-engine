"""NDA Review — upload an NDA, apply the NCP rulebook, iterate to signature.

Rules are seeded from the NCP NDA Review Handoff (Oct 2026) and editable
below. Each NDA is a named project; every uploaded version becomes a
numbered round so the negotiation history stays reviewable until the
document is executable from both sides.
"""

import streamlit as st

# Page config is owned by the app.py navigation router.


def _check_password():
    try:
        app_password = st.secrets["APP_PASSWORD"]
    except (FileNotFoundError, KeyError):
        st.error("APP_PASSWORD is not configured. Add it to secrets.toml.")
        return False

    def _entered():
        if st.session_state.get("password") == app_password:
            st.session_state["password_correct"] = True
        else:
            st.session_state["password_correct"] = False
            st.session_state["password_attempted"] = True

    if st.session_state.get("password_correct"):
        return True
    st.text_input("Enter Password", type="password",
                  on_change=_entered, key="password")
    if st.session_state.get("password_attempted"):
        st.error("Password incorrect")
    return False


if not _check_password():
    st.stop()

from lib import nda

nda.init_db()
nda.restore_if_empty()

st.title("📄 NDA Review")
st.caption(
    "Upload an NDA, set the deal gates, and get the NCP review: bucketed "
    "issues, counsel escalations, a clean NCP-position draft, and a "
    "marked review copy. Iterate round by round until it's executable."
)

# ── Rulebook ─────────────────────────────────────────────────────────
with st.expander("📘 The NCP NDA rulebook (seeded from your handoff — editable)"):
    st.caption(
        "Loaded from the NCP NDA Review Handoff (Oct 2, 2026). Edits here "
        "override the file and apply to every future review."
    )
    _rules = st.text_area("Rules", value=nda.load_rules(), height=400,
                          label_visibility="collapsed")
    if st.button("Save rulebook"):
        nda.save_rules(_rules)
        nda.backup_to_github()
        st.success("Rulebook saved — future reviews use this text.")

tab_review, tab_projects = st.tabs(["🔍 Review an NDA", "🗂️ Projects"])


def _render_review(review, skipped=None):
    """Shared renderer for one round's review output."""
    if review.get("executable"):
        st.success(f"**Executable:** yes — {review.get('executable_reasoning', '')}")
    else:
        st.warning(f"**Executable:** not yet — {review.get('executable_reasoning', '')}")
    st.markdown(f"**Report to Trey:** {review.get('summary', '')}")
    if review.get("net_discloser_note"):
        st.info(f"**Mutual-form note:** {review['net_discloser_note']}")
    if review.get("escalations"):
        st.error("**🚩 Counsel escalations (do not negotiate alone):**\n"
                 + "\n".join(f"- {e}" for e in review["escalations"]))
    edits = review.get("edits", [])
    buckets = {"non_negotiable": "🔒 Non-negotiable",
               "trade": "🔁 Credibility trades",
               "style": "✏️ Style / conforming"}
    for key, label in buckets.items():
        rows = [e for e in edits if e.get("bucket") == key]
        if rows:
            st.markdown(f"#### {label} ({len(rows)})")
            for e in rows:
                with st.container(border=True):
                    if e.get("original"):
                        st.markdown(f"~~{e['original'][:400]}~~")
                    st.markdown(f"**→ {e.get('revised', '')[:600]}**")
                    st.caption(e.get("rationale", ""))
    if review.get("accepted_as_drafted"):
        with st.expander(
                f"Accepted as drafted — listed so nothing is silent "
                f"({len(review['accepted_as_drafted'])})"):
            for a in review["accepted_as_drafted"]:
                st.markdown(f"- {a}")
    if skipped:
        st.warning(
            "**Edits that could not be auto-applied** (original text not "
            "found exactly once — apply by hand):\n"
            + "\n".join(f"- {s.get('original', '')[:120]}… "
                        f"({s.get('_skip_reason')})" for s in skipped))


# ═════════════════════ REVIEW TAB ════════════════════════════════════
with tab_review:
    projects = nda.list_projects()
    open_names = [p["name"] for p in projects if p["status"] != "executable"]
    mode = st.radio("Project", ["➕ New project"] + open_names,
                    horizontal=True, label_visibility="collapsed")

    if mode == "➕ New project":
        c1, c2 = st.columns(2)
        proj_name = c1.text_input("Project name",
                                  placeholder="e.g. Project Apollo")
        deal_type = c2.selectbox("Deal type (Gate 2)", nda.DEAL_TYPES)
        c3, c4 = st.columns(2)
        ncp_role = c3.selectbox("NCP's role (Gate 1)", nda.NCP_ROLES)
        form_source = c4.selectbox("Form source (Gate 3)", nda.FORM_SOURCES)
    else:
        proj = next(p for p in projects if p["name"] == mode)
        proj_name = proj["name"]
        deal_type, ncp_role = proj["deal_type"], proj["ncp_role"]
        form_source = proj["form_source"]
        rounds_so_far = nda.list_rounds(proj["id"])
        st.caption(
            f"{proj_name}: {deal_type} · {ncp_role} · {form_source} · "
            f"{len(rounds_so_far)} round(s) so far — uploading starts "
            f"round {len(rounds_so_far) + 1}."
        )

    up = st.file_uploader("NDA file (.docx or .pdf)", type=["docx", "pdf"])
    if st.button("Run NCP review", type="primary",
                 disabled=not (up and (proj_name or "").strip())):
        from lib.api_clients import load_api_keys, make_openai_client
        try:
            text = nda.extract_text(up.getvalue(), up.name)
        except RuntimeError as e:
            st.error(str(e))
            st.stop()
        if len(text.strip()) < 500:
            st.error("Could not extract meaningful text from that file — "
                     "if it's a scanned PDF, upload the .docx instead.")
            st.stop()

        pid = nda.create_project(proj_name.strip(), deal_type, ncp_role,
                                 form_source)
        ctx = nda.prior_context(pid)
        keys = load_api_keys()
        client = make_openai_client(api_key=keys["OPENAI_API_KEY"])
        with st.spinner("Applying the NCP rulebook (60-90 seconds)..."):
            try:
                review = nda.review_nda(client, text, deal_type, ncp_role,
                                        form_source, nda.load_rules(), ctx)
            except Exception as e:
                st.error(f"Review failed: {e}")
                st.stop()
        round_no = nda.add_round(pid, up.name, text, review)
        nda.backup_to_github()

        st.markdown(f"## Round {round_no} review — {proj_name}")
        clean_text, applied, skipped = nda.apply_edits(
            text, review.get("edits", []))
        _render_review(review, skipped)

        d1, d2 = st.columns(2)
        d1.download_button(
            "⬇️ Clean NCP-position draft (.docx)",
            data=nda.build_clean_docx(clean_text, proj_name, round_no),
            file_name=f"{proj_name.replace(' ', '_')}_R{round_no}_NCP_clean.docx",
            mime="application/vnd.openxmlformats-officedocument"
                 ".wordprocessingml.document",
            use_container_width=True,
        )
        d2.download_button(
            "⬇️ Marked review copy (.docx)",
            data=nda.build_marked_docx(text, applied, proj_name, round_no),
            file_name=f"{proj_name.replace(' ', '_')}_R{round_no}_marked.docx",
            mime="application/vnd.openxmlformats-officedocument"
                 ".wordprocessingml.document",
            use_container_width=True,
        )
        st.caption(
            "The clean draft is NCP's position as plain text — feed it to "
            "the locked compare workflow (LibreOffice CompareDocuments, "
            "author 'New Capital Partners') for the true tracked-changes "
            "redline. The marked copy is for review only."
        )

# ═════════════════════ PROJECTS TAB ══════════════════════════════════
with tab_projects:
    projects = nda.list_projects()
    if not projects:
        st.info("No NDA projects yet — run your first review in the other tab.")
    for p in projects:
        icon = "✅" if p["status"] == "executable" else "📝"
        with st.expander(f"{icon} {p['name']} — {p['status']} · "
                         f"{p['deal_type']}"):
            st.caption(f"{p['ncp_role']} · {p['form_source']} · "
                       f"created {(p['created_at'] or '')[:10]}")
            rounds = nda.list_rounds(p["id"])
            for r in rounds:
                st.markdown(f"### Round {r['round_no']} — "
                            f"{(r['created_at'] or '')[:10]} · {r['filename']}")
                review = r.get("review", {})
                clean_text, applied, skipped = nda.apply_edits(
                    r.get("original_text") or "", review.get("edits", []))
                _render_review(review, skipped)
                rd1, rd2 = st.columns(2)
                rd1.download_button(
                    "⬇️ Clean draft",
                    data=nda.build_clean_docx(clean_text, p["name"],
                                              r["round_no"]),
                    file_name=f"{p['name'].replace(' ', '_')}"
                              f"_R{r['round_no']}_NCP_clean.docx",
                    mime="application/vnd.openxmlformats-officedocument"
                         ".wordprocessingml.document",
                    key=f"cl_{r['id']}", use_container_width=True)
                rd2.download_button(
                    "⬇️ Marked copy",
                    data=nda.build_marked_docx(r.get("original_text") or "",
                                               applied, p["name"],
                                               r["round_no"]),
                    file_name=f"{p['name'].replace(' ', '_')}"
                              f"_R{r['round_no']}_marked.docx",
                    mime="application/vnd.openxmlformats-officedocument"
                         ".wordprocessingml.document",
                    key=f"mk_{r['id']}", use_container_width=True)
                st.markdown("---")
            b1, b2 = st.columns(2)
            if p["status"] != "executable":
                if b1.button("✅ Mark executable (both sides can sign)",
                             key=f"exec_{p['id']}", use_container_width=True):
                    nda.set_project_status(p["id"], "executable")
                    nda.backup_to_github()
                    st.rerun()
                b2.caption("Next counterparty turn: select this project in "
                           "the Review tab and upload their version — it "
                           "becomes the next round with full history context.")
            else:
                if b1.button("Reopen negotiation", key=f"reopen_{p['id']}",
                             use_container_width=True):
                    nda.set_project_status(p["id"], "in_review")
                    st.rerun()
