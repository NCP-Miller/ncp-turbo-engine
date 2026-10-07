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

def _review_clients():
    """Claude (preferred) + GPT-4o fallback, from secrets."""
    anthropic_client = None
    try:
        import anthropic
        anthropic_client = anthropic.Anthropic(
            api_key=st.secrets["ANTHROPIC_API_KEY"])
    except Exception:
        anthropic_client = None
    openai_client = None
    try:
        from lib.api_clients import load_api_keys, make_openai_client
        keys = load_api_keys()
        openai_client = make_openai_client(api_key=keys["OPENAI_API_KEY"])
    except Exception:
        openai_client = None
    return anthropic_client, openai_client


# ── Rulebook + engine ────────────────────────────────────────────────
with st.expander("📘 The NCP NDA rulebook (seeded from your handoff — editable)"):
    st.caption(
        "Loaded from the NCP NDA Review Handoff (updated Oct 6, 2026 — "
        "includes the full Mariner record and counsel's §4/§9 compromise "
        "assessment). Edits here override the file and apply to every "
        "future review."
    )
    _rules = st.text_area("Rules", value=nda.load_rules(), height=400,
                          label_visibility="collapsed")
    rc1, rc2, rc3 = st.columns([1, 1, 2])
    if rc1.button("Save rulebook"):
        nda.save_rules(_rules)
        nda.backup_to_github()
        st.success("Rulebook saved — future reviews use this text.")
    if rc2.button("Reset to latest handoff",
                  help="Replaces any in-app edits with the newest handoff "
                       "file shipped with the app (currently Oct 6, 2026)."):
        nda.save_rules(nda.load_rules_file())
        nda.backup_to_github()
        st.success("Rulebook reset to the latest handoff.")
        st.rerun()
    _model_pick = rc3.selectbox(
        "Negotiation brain", nda.ANTHROPIC_MODELS,
        index=nda.ANTHROPIC_MODELS.index(nda.get_review_model()),
        help="Claude with extended thinking reasons through the negotiation "
             "before drafting. Falls back down this list if your API org "
             "lacks access to the chosen model.")
    if _model_pick != nda.get_review_model():
        nda.set_review_model(_model_pick)

_ac, _oc = _review_clients()
if _ac:
    st.caption(f"🧠 Reviews run on **Claude ({nda.get_review_model()})** "
               f"with extended thinking.")
else:
    st.warning(
        "ANTHROPIC_API_KEY is not in secrets — reviews will fall back to "
        "GPT-4o. Add an Anthropic API key (console.anthropic.com) to get "
        "Claude's negotiation reasoning."
    )

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
        try:
            text = nda.extract_text(up.getvalue(), up.name)
        except RuntimeError as e:
            st.error(str(e))
            st.stop()
        if len(text.strip()) < 500:
            st.error("Could not extract meaningful text from that file — "
                     "if it's a scanned PDF, upload the .docx instead.")
            st.stop()

        _is_docx = up.name.lower().endswith(".docx")
        pid = nda.create_project(proj_name.strip(), deal_type, ncp_role,
                                 form_source)
        ctx = nda.prior_context(pid)

        # Handoff incoming-file check: a counter-round "redline" with zero
        # live markup means their changes were already accepted silently.
        if ctx and _is_docx:
            _ins_ct, _del_ct = nda.count_tracked_changes(up.getvalue())
            if _ins_ct == 0 and _del_ct == 0:
                st.warning(
                    "⚠️ This counter-round file contains **no live tracked "
                    "changes** (0 w:ins / 0 w:del) — the counterparty may "
                    "have accepted changes before sending (the Earthling "
                    "pattern). The review will diff against the prior "
                    "round; ask them to keep Track Changes on."
                )
            else:
                st.caption(f"Incoming file markup: {_ins_ct} insertions, "
                           f"{_del_ct} deletions — live redline confirmed.")

        with st.spinner("Claude is reasoning through the NDA against the "
                        "rulebook (1-3 minutes with extended thinking)..."):
            try:
                review, engine = nda.review_nda(
                    text, deal_type, ncp_role, form_source,
                    nda.load_rules(), ctx,
                    anthropic_client=_ac, openai_client=_oc)
            except Exception as e:
                st.error(f"Review failed: {e}")
                st.stop()
        review["_engine"] = engine
        round_no = nda.add_round(pid, up.name, text, review,
                                 file_blob=up.getvalue() if _is_docx else None)
        nda.backup_to_github()
        _rounds = nda.list_rounds(pid)
        st.session_state["_nda_view"] = {
            "project_id": pid,
            "round_id": _rounds[-1]["id"],
            "project_name": proj_name.strip(),
        }
        st.rerun()

# ── Persistent review display: survives download clicks and reruns ──
_view = st.session_state.get("_nda_view")
if _view:
    with tab_review:
        _vrounds = nda.list_rounds(_view["project_id"])
        _vround = next((r for r in _vrounds
                        if r["id"] == _view["round_id"]), None)
        if _vround:
            _vname = _view.get("project_name", "Project")
            h1, h2 = st.columns([5, 1])
            h1.markdown(f"## Round {_vround['round_no']} review — {_vname}")
            if h2.button("✕ Close", key="_nda_view_close",
                         use_container_width=True):
                st.session_state.pop("_nda_view", None)
                st.rerun()
            _vreview = _vround.get("review", {})
            if _vreview.get("_engine"):
                if "fallback" in _vreview["_engine"] or \
                        "no ANTHROPIC" in _vreview["_engine"]:
                    st.warning(f"Reviewed by {_vreview['_engine']}")
                else:
                    st.caption(f"Reviewed by {_vreview['_engine']}")
            _vtext = _vround.get("original_text") or ""
            _vclean, _vapplied, _vskipped = nda.apply_edits(
                _vtext, _vreview.get("edits", []))
            _render_review(_vreview, _vskipped)

            # ── Deliverables (regenerated from storage every render,
            #    so they survive any number of clicks) ───────────────
            _vblob = (nda.get_round_blob(_vround["id"])
                      if _vround.get("has_blob") else None)
            if _vblob:
                _tb, _tr = nda.build_tracked_docx(
                    _vblob, _vreview.get("edits", []))
                st.download_button(
                    "⬇️ REDLINE — Word tracked changes (.docx)",
                    data=_tb,
                    file_name=f"{_vname.replace(' ', '_')}"
                              f"_R{_vround['round_no']}_NCP_Redlined.docx",
                    mime="application/vnd.openxmlformats-officedocument"
                         ".wordprocessingml.document",
                    type="primary", use_container_width=True,
                    key="_nda_view_redline")
                _acc = "✅ PASS" if _tr["accept_audit"] else "❌ FAIL"
                _rej = "✅ PASS" if _tr["reject_audit"] else "❌ FAIL"
                st.caption(
                    f"Genuine Word tracked changes in the counterparty's "
                    f"own file · author **{_tr['author']}** · change IDs "
                    f"from {_tr['first_id']} · {_tr['applied']} edits "
                    f"applied · **Accept-audit: {_acc}** · "
                    f"**Reject-audit: {_rej}**")
                if _tr["skipped"]:
                    st.warning(
                        f"{len(_tr['skipped'])} edit(s) could not be placed "
                        f"in the redline automatically — apply by hand:\n"
                        + "\n".join(
                            f"- {s.get('original', s.get('revised', ''))[:120]}… "
                            f"({s.get('_skip_reason')})"
                            for s in _tr["skipped"]))
            else:
                st.info("Tracked-changes redline needs the counterparty's "
                        ".docx — this round was a PDF, so only the clean "
                        "and marked copies are available.")

            vd1, vd2 = st.columns(2)
            vd1.download_button(
                "⬇️ Clean NCP-position draft (.docx)",
                data=nda.build_clean_docx(_vclean, _vname,
                                          _vround["round_no"]),
                file_name=f"{_vname.replace(' ', '_')}"
                          f"_R{_vround['round_no']}_NCP_clean.docx",
                mime="application/vnd.openxmlformats-officedocument"
                     ".wordprocessingml.document",
                use_container_width=True, key="_nda_view_clean")
            vd2.download_button(
                "⬇️ Marked review copy (.docx)",
                data=nda.build_marked_docx(_vtext, _vapplied, _vname,
                                           _vround["round_no"]),
                file_name=f"{_vname.replace(' ', '_')}"
                          f"_R{_vround['round_no']}_marked.docx",
                mime="application/vnd.openxmlformats-officedocument"
                     ".wordprocessingml.document",
                use_container_width=True, key="_nda_view_marked")
        else:
            st.session_state.pop("_nda_view", None)

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
                if r.get("has_blob"):
                    _blob = nda.get_round_blob(r["id"])
                    if _blob:
                        _tb, _tr = nda.build_tracked_docx(
                            _blob, review.get("edits", []))
                        st.download_button(
                            "⬇️ REDLINE — Word tracked changes",
                            data=_tb,
                            file_name=f"{p['name'].replace(' ', '_')}"
                                      f"_R{r['round_no']}_NCP_Redlined.docx",
                            mime="application/vnd.openxmlformats-"
                                 "officedocument.wordprocessingml.document",
                            key=f"tr_{r['id']}", type="primary",
                            use_container_width=True)
                        st.caption(
                            f"Accept-audit: "
                            f"{'✅' if _tr['accept_audit'] else '❌'} · "
                            f"Reject-audit: "
                            f"{'✅' if _tr['reject_audit'] else '❌'} · "
                            f"author {_tr['author']} · IDs from "
                            f"{_tr['first_id']}")
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
