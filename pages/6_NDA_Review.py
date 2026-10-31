"""NDA Review — upload an NDA, apply the NCP rulebook, iterate to signature.

Rules are seeded from the NCP NDA Review Handoff (Oct 2026) and editable
below. Each NDA is a named project; every uploaded version becomes a
numbered round so the negotiation history stays reviewable until the
document is executable from both sides.
"""

import html
import json

import streamlit as st

# Self-heal after a partial redeploy: Streamlit can serve new page code
# alongside stale cached lib modules that lack newly added functions.
import importlib
import lib.nda_redline as R
import lib.nda as _nda_mod
if not all(hasattr(R, a) for a in ("build", "baseline_view", "diff_ops",
                                   "incoming_changes", "untracked_changes")):
    R = importlib.reload(R)
if not all(hasattr(_nda_mod, a) for a in (
        "build_deliverables", "prepare_upload", "get_signing",
        "COVERAGE_CHECKLIST", "format_counterparty_changes", "get_effort")):
    _nda_mod = importlib.reload(_nda_mod)

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
    "Upload the counterparty's NDA, set the deal gates, and get the NCP "
    "review plus two send-ready files: their own document redlined with "
    "genuine Word tracked changes, and a clean copy with every change "
    "accepted. Their formatting and filename survive. Iterate round by "
    "round until it's executable."
)

_DOCX_MIME = ("application/vnd.openxmlformats-officedocument"
              ".wordprocessingml.document")
_BUCKETS = {"non_negotiable": "🔒 Non-negotiable",
            "trade": "🔁 Credibility trade",
            "style": "✏️ Style / conforming"}
_STATUS_ICON = {"in_form": "✅ in form", "added": "➕ added",
                "revised": "✏️ revised", "not_applicable": "➖ n/a",
                "conceded": "⚠️ conceded", "missing": "❌ missing"}
_COVERAGE_LABELS = {
    "competing_investments": "Competing Investments clause",
    "residuals": "Residuals backup",
    "ci_survival_supremacy": "CI survival + supremacy",
    "dual_hat": "Dual Hat Employees",
    "no_imputed_knowledge": "No imputed knowledge",
    "data_room_supremacy": "Data room supremacy",
    "compulsion_cost": "Compulsion: sole cost",
    "compulsion_counsel": "Compulsion: counsel decides scope",
    "compulsion_regulatory": "Compulsion: regulator carve-out",
    "representatives": "Representatives definition",
    "rep_responsibility": "Joinder carve-out",
    "return_destroy": "Return / destroy + retention",
    "term": "Term per ladder",
    "non_solicit": "Non-solicit",
    "contact_restrictions": "Contact restrictions",
    "carve_outs": "Confidentiality carve-outs",
    "no_obligation": "No obligation to proceed",
    "governing_law": "Delaware + Chancery",
    "remedies": "Injunctive relief / bond",
    "liability": "Direct damages cap",
    "no_noncompete_standstill": "No non-compete / standstill",
    "party_details": "NCP party details filled",
    "countersignature": "Company countersignature",
    "defined_terms": "Defined terms consistent",
    "counsel_flags": "Counsel red flags",
}


def _review_clients():
    """Claude (preferred) + GPT-4o fallback, from secrets.

    If the Anthropic key is an org-level key (not scoped to a single
    workspace), the API requires an anthropic-workspace-id header —
    set ANTHROPIC_WORKSPACE_ID in secrets and it is sent automatically.
    """
    anthropic_client = None
    try:
        import anthropic
        _kw = {"api_key": st.secrets["ANTHROPIC_API_KEY"]}
        _ws = ""
        try:
            _ws = (st.secrets.get("ANTHROPIC_WORKSPACE_ID") or "").strip()
        except Exception:
            _ws = ""
        if _ws:
            _kw["default_headers"] = {"anthropic-workspace-id": _ws}
        anthropic_client = anthropic.Anthropic(**_kw)
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


# ── Rulebook, engine and signing details ─────────────────────────────
with st.expander("📘 Rulebook, negotiation engine and signing details"):
    st.caption(
        "Loaded from the NCP NDA Review Handoff (updated Oct 6, 2026, "
        "including the full Mariner record and counsel's §4/§9 compromise "
        "assessment). Edits here override the file and apply to every "
        "future review."
    )
    _rules = st.text_area("Rules", value=nda.load_rules(), height=400,
                          label_visibility="collapsed")
    rc1, rc2 = st.columns(2)
    if rc1.button("Save rulebook", use_container_width=True):
        nda.save_rules(_rules)
        nda.backup_to_github()
        st.success("Rulebook saved — future reviews use this text.")
    if rc2.button("Reset to latest handoff", use_container_width=True,
                  help="Replaces any in-app edits with the newest handoff "
                       "file shipped with the app (currently Oct 6, 2026)."):
        nda.save_rules(nda.load_rules_file())
        nda.backup_to_github()
        st.success("Rulebook reset to the latest handoff.")
        st.rerun()

    e1, e2, e3 = st.columns(3)
    _model_pick = e1.selectbox(
        "Negotiation brain", nda.ANTHROPIC_MODELS,
        index=nda.ANTHROPIC_MODELS.index(nda.get_review_model()),
        help="Claude reasons through the negotiation before drafting. Falls "
             "back down this list if your API workspace lacks the chosen "
             "model.")
    if _model_pick != nda.get_review_model():
        nda.set_review_model(_model_pick)
    _effort_pick = e2.selectbox(
        "Reasoning depth", nda.EFFORT_LEVELS,
        index=nda.EFFORT_LEVELS.index(nda.get_effort()),
        help="How hard Claude thinks. 'high' is the default; 'xhigh' and "
             "'max' are slower and more thorough.")
    if _effort_pick != nda.get_effort():
        nda.set_effort(_effort_pick)
    _audit_pick = e3.checkbox(
        "Senior-partner self-audit pass", value=nda.get_self_audit(),
        help="A second Claude pass checks the draft against the rulebook "
             "and the coverage checklist and fixes gaps before you see it. "
             "Roughly doubles review time.")
    if _audit_pick != nda.get_self_audit():
        nda.set_self_audit(_audit_pick)

    st.markdown("**NCP signing details** (filled into every form)")
    _sg = nda.get_signing()
    s1, s2 = st.columns(2)
    _sg_new = {
        "entity": s1.text_input("Entity", _sg["entity"]),
        "address": s2.text_input("Address", _sg["address"]),
        "signatory": s1.text_input("Signatory", _sg["signatory"]),
        "title": s2.text_input("Title", _sg["title"]),
        "email": s1.text_input("E-mail", _sg["email"]),
        "phone": s2.text_input("Phone", _sg["phone"]),
    }
    if st.button("Save signing details"):
        nda.set_signing(_sg_new)
        nda.backup_to_github()
        st.success("Signing details saved.")

_ac, _oc = _review_clients()
if _ac:
    st.caption(
        f"🧠 Reviews run on **Claude ({nda.get_review_model()})** with "
        f"adaptive thinking at **{nda.get_effort()}** effort"
        + (", then a senior-partner self-audit pass." if nda.get_self_audit()
           else "."))
else:
    st.warning(
        "ANTHROPIC_API_KEY is not in secrets — reviews will fall back to "
        "GPT-4o. Add an Anthropic API key (console.anthropic.com) to get "
        "Claude's negotiation reasoning."
    )

tab_review, tab_projects = st.tabs(["🔍 Review an NDA", "🗂️ Projects"])


# ── Rendering helpers ────────────────────────────────────────────────

def _diff_html(old, new):
    ops, _ = R.diff_ops(old, new)
    out = []
    for tag, a0, a1, b0, b1 in ops:
        if tag == "equal":
            out.append(html.escape(old[a0:a1]))
            continue
        if a1 > a0:
            out.append("<del style='color:#B00020'>"
                       f"{html.escape(old[a0:a1])}</del>")
        if b1 > b0:
            out.append("<ins style='color:#0056A7'>"
                       f"{html.escape(new[b0:b1])}</ins>")
    return "".join(out)


def _changes_md(changes):
    return "\n".join(
        f"- {_BUCKETS.get(c.get('bucket'), c.get('bucket') or '')} · "
        f"**{c.get('title', '')}** — {c.get('rationale', '')}"
        for c in changes or [])


def _render_review(review, report=None):
    """Shared renderer for one round's review output."""
    if review.get("executable"):
        st.success(f"**Executable:** yes — "
                   f"{review.get('executable_reasoning', '')}")
    else:
        st.warning(f"**Executable:** not yet — "
                   f"{review.get('executable_reasoning', '')}")
    st.markdown(f"**Report to Trey:** {review.get('summary', '')}")
    if review.get("net_discloser_note"):
        st.info(f"**Mutual-form note:** {review['net_discloser_note']}")
    if review.get("escalations"):
        st.error("**🚩 Escalations and judgment calls:**\n"
                 + "\n".join(f"- {e}" for e in review["escalations"]))
    if review.get("counterparty_moves"):
        st.markdown("#### Counterparty moves since NCP's last turn")
        for m in review["counterparty_moves"]:
            st.markdown(
                f"- ¶{m.get('para', '?')}: {m.get('what_they_did', '')} → "
                f"**{m.get('ncp_response', '')}**. {m.get('note', '')}")

    edits = review.get("edits", [])
    if review.get("format") == 2:
        base = (report or {}).get("baseline") or review.get("_para_texts") \
            or []
        labels = (report or {}).get("labels") or []
        counts = {}
        for e in edits:
            for c in e.get("changes") or []:
                counts[c.get("bucket")] = counts.get(c.get("bucket"), 0) + 1
        st.markdown(
            f"#### NCP's changes ({len(edits)} paragraphs) — "
            + " · ".join(f"{_BUCKETS[k]}: {counts[k]}"
                         for k in _BUCKETS if counts.get(k)))
        order = {"modify": 0, "delete": 1, "insert_after": 2, "insert": 2}
        for e in sorted(edits, key=lambda x: (order.get(x.get("action"), 3),
                                              x.get("para") or 0)):
            i = e.get("para")
            lab = labels[i] if isinstance(i, int) and 0 <= i < len(labels) \
                else ""
            with st.container(border=True):
                act = e.get("action")
                if act == "modify" and isinstance(i, int) and i < len(base):
                    st.caption(f"¶{i}" + (f" · clause {lab}" if lab else ""))
                    st.markdown(_diff_html(base[i], e.get("text") or ""),
                                unsafe_allow_html=True)
                elif act == "delete" and isinstance(i, int) and i < len(base):
                    st.caption(f"¶{i} deleted")
                    st.markdown("<del style='color:#B00020'>"
                                f"{html.escape(base[i])}</del>",
                                unsafe_allow_html=True)
                else:
                    st.caption(f"New clause after ¶{i}")
                    st.markdown("<ins style='color:#0056A7'>"
                                f"{html.escape(e.get('text') or '')}</ins>",
                                unsafe_allow_html=True)
                st.markdown(_changes_md(e.get("changes")))
    else:
        for key, label in _BUCKETS.items():
            rows = [e for e in edits if e.get("bucket") == key]
            if rows:
                st.markdown(f"#### {label} ({len(rows)})")
                for e in rows:
                    with st.container(border=True):
                        if e.get("original"):
                            st.markdown(f"~~{e['original'][:400]}~~")
                        st.markdown(f"**→ {e.get('revised', '')[:600]}**")
                        st.caption(e.get("rationale", ""))

    if review.get("coverage") is not None and review.get("format") == 2:
        got = {c.get("item"): c for c in review.get("coverage") or []}
        rows = []
        for key, _ in nda.COVERAGE_CHECKLIST:
            c = got.get(key, {})
            rows.append({
                "Requirement": _COVERAGE_LABELS.get(key, key),
                "Status": _STATUS_ICON.get(c.get("status"),
                                           "⚠️ not reported"),
                "¶": ", ".join(str(x) for x in c.get("paras") or []),
                "Note": c.get("note", ""),
            })
        gaps = [r for r in rows if r["Status"].startswith(("❌", "⚠️"))]
        st.markdown("#### Rulebook coverage"
                    + (f" — {len(gaps)} item(s) need your eye" if gaps
                       else " — every requirement accounted for"))
        st.dataframe(rows, hide_index=True, use_container_width=True)

    if review.get("accepted_as_drafted"):
        # Bordered container, not an expander: this renderer also runs
        # inside the Projects-tab expanders, and Streamlit forbids
        # nesting expanders.
        with st.container(border=True):
            st.markdown(f"**Accepted as drafted — listed so nothing is "
                        f"silent ({len(review['accepted_as_drafted'])})**")
            for a in review["accepted_as_drafted"]:
                st.markdown(f"- {a}")
    if review.get("fallbacks_available"):
        st.markdown("**Fallbacks still in reserve:** "
                    + "; ".join(review["fallbacks_available"]))
    if review.get("audit_notes"):
        st.markdown("**What the senior-partner pass fixed:**\n"
                    + "\n".join(f"- {a}" for a in review["audit_notes"]))
    if review.get("_audit_error"):
        st.caption(f"Self-audit pass did not complete "
                   f"({review['_audit_error']}); showing the first draft.")
    if review.get("cover_email"):
        st.markdown("**Cover email to the banker** (copy button at right)")
        st.code(review["cover_email"], language=None, wrap_lines=True)


def _round_source(rnd):
    """The .docx the round's redline is built on."""
    blob = nda.get_round_blob(rnd["id"]) if rnd.get("has_blob") else None
    if blob:
        return blob, False
    review = rnd.get("review", {})
    texts = review.get("_para_texts") or \
        (rnd.get("original_text") or "").split("\n")
    import docx
    import io
    d = docx.Document()
    for t in texts:
        d.add_paragraph(t)
    buf = io.BytesIO()
    d.save(buf)
    return buf.getvalue(), True


@st.cache_data(show_spinner=False, max_entries=64)
def _build_cached(source, review_json):
    return nda.build_deliverables(source, json.loads(review_json))


def _build_for(rnd):
    source, rebuilt = _round_source(rnd)
    review = rnd.get("review", {})
    red, clean, rep = _build_cached(
        source, json.dumps(review, sort_keys=True, default=str))
    return red, clean, rep, rebuilt


def _render_deliverables(rnd, review, key_prefix, built=None):
    """REDLINE + CLEAN downloads for one round, gated on the audits."""
    fname = rnd.get("filename") or "NDA.docx"
    try:
        red, clean, rep, rebuilt = built or _build_for(rnd)
    except Exception as e:
        st.error(f"Could not build the redline: {type(e).__name__}: {e}")
        return
    if rebuilt:
        st.info("The counterparty's original .docx is not stored for this "
                "round (restored from backup), so the redline is built on a "
                "plain rebuild of its text. Re-upload their file to get the "
                "redline in their own formatting.")
    elif review.get("_from_pdf"):
        st.info("This round was a PDF, so the redline is built on a .docx "
                "rebuilt from the PDF's text (their layout is not "
                "preserved). Ask for the Word version when you can.")
    if rep["ok"]:
        d1, d2 = st.columns(2)
        d1.download_button(
            "⬇️ REDLINE — Word tracked changes", data=red,
            file_name=nda.deliverable_name(fname, "NCP Redline"),
            mime=_DOCX_MIME, type="primary", use_container_width=True,
            key=f"{key_prefix}_redline")
        d2.download_button(
            "⬇️ CLEAN — all changes accepted", data=clean,
            file_name=nda.deliverable_name(fname, "NCP Clean"),
            mime=_DOCX_MIME, use_container_width=True,
            key=f"{key_prefix}_clean")
        st.caption(
            f"Their own document edited in place · {rep['changes']} tracked "
            f"changes by **{rep['author']}** (IDs from {rep['first_id']}) · "
            f"✅ reject-all reproduces their text · ✅ accept-all equals "
            f"NCP's position · ✅ untouched paragraphs unchanged · ✅ markup "
            f"well formed")
    else:
        st.error("**Integrity audit failed — do not send these files.** "
                 + " ".join(rep["problems"]))
        d1, d2 = st.columns(2)
        d1.download_button(
            "⚠️ Draft redline (failed audit)", data=red,
            file_name=nda.deliverable_name(fname, "NCP Redline DRAFT"),
            mime=_DOCX_MIME, use_container_width=True,
            key=f"{key_prefix}_redline")
        d2.download_button(
            "⚠️ Draft clean (failed audit)", data=clean,
            file_name=nda.deliverable_name(fname, "NCP Clean DRAFT"),
            mime=_DOCX_MIME, use_container_width=True,
            key=f"{key_prefix}_clean")
    for note in rep.get("notes") or []:
        st.caption(f"ℹ️ {note}")
    for w in rep.get("style_warnings") or []:
        st.warning(w)
    if rep.get("skipped"):
        st.warning(
            f"{len(rep['skipped'])} edit(s) from this older round could not "
            f"be placed automatically — apply by hand:\n"
            + "\n".join(f"- {s.get('original', s.get('revised', ''))[:120]}… "
                        f"({s.get('_skip_reason')})"
                        for s in rep["skipped"]))


def _engine_banner(review):
    eng = review.get("_engine") or ""
    if not eng:
        return
    if "fallback —" in eng or "no ANTHROPIC" in eng:
        st.warning(f"Reviewed by {eng}")
        if "workspace" in eng.lower():
            st.error(
                "**Why Claude didn't run:** your Anthropic API key is an "
                "org-level key, and the API requires a workspace for it. "
                "Fix either way:\n"
                "1. Add `ANTHROPIC_WORKSPACE_ID = \"wrkspc_...\"` to the "
                "app's Streamlit secrets (console.anthropic.com → Settings → "
                "Workspaces), **or**\n"
                "2. Create a new API key inside a workspace and replace "
                "`ANTHROPIC_API_KEY`.\n\n"
                "Then re-run the review.")
    else:
        st.caption(f"Reviewed by {eng}")


# ═════════════════════ REVIEW TAB ════════════════════════════════════
with tab_review:
    projects = nda.list_projects()
    open_names = [p["name"] for p in projects if p["status"] != "executable"]
    mode = st.radio("Project", ["➕ New project"] + open_names,
                    horizontal=True, label_visibility="collapsed")

    _existing = None
    _start_fresh = False
    if mode == "➕ New project":
        c1, c2 = st.columns(2)
        proj_name = c1.text_input("Project name",
                                  placeholder="e.g. Project Apollo")
        deal_type = c2.selectbox("Deal type (Gate 2)", nda.DEAL_TYPES)
        c3, c4 = st.columns(2)
        ncp_role = c3.selectbox("NCP's role (Gate 1)", nda.NCP_ROLES)
        form_source = c4.selectbox("Form source (Gate 3)", nda.FORM_SOURCES)
        _existing = next(
            (p for p in projects
             if p["name"].strip().lower() ==
             (proj_name or "").strip().lower()), None)
        if _existing:
            _n_prev = len(nda.list_rounds(_existing["id"]))
            st.warning(
                f"⚠️ **{_existing['name']}** already exists with "
                f"{_n_prev} prior round(s). Re-using a project name "
                f"continues that negotiation — that is why a re-run "
                f"comes back as Round {_n_prev + 1}.")
            _start_fresh = st.radio(
                "This run should:",
                [f"Continue the negotiation — this upload becomes "
                 f"Round {_n_prev + 1}",
                 "Start fresh at Round 1 — delete the prior round(s) "
                 "first"],
            ).startswith("Start fresh")
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
            source, from_pdf = nda.prepare_upload(up.getvalue(), up.name)
            view = R.baseline_view(source)
        except Exception as e:
            st.error(f"Could not read that file: {e}")
            st.stop()
        if sum(len(t) for t in view["texts"]) < 500:
            st.error("Could not extract meaningful text from that file — "
                     "if it's a scanned PDF, upload the .docx instead.")
            st.stop()

        if _existing and _start_fresh:
            nda.delete_project(_existing["id"])
        pid = nda.create_project(proj_name.strip(), deal_type, ncp_role,
                                 form_source)
        ctx = nda.prior_context(pid)
        prev_rounds = nda.list_rounds(pid)

        # Counter-round intake: what did the counterparty change? Live
        # markup is read directly; a file with none is diffed against
        # NCP's last position (the Earthling pattern).
        cp_changes = ""
        if prev_rounds and not from_pdf:
            tracked = R.incoming_changes(up.getvalue())
            if tracked:
                _ins_ct, _del_ct = nda.count_tracked_changes(up.getvalue())
                st.caption(f"Incoming file markup: {_ins_ct} insertions, "
                           f"{_del_ct} deletions — live redline confirmed; "
                           f"their changes are accepted into the baseline so "
                           f"NCP's markup is a single layer.")
                cp_changes = nda.format_counterparty_changes(tracked=tracked)
            else:
                st.warning(
                    "⚠️ This counter-round file contains **no live tracked "
                    "changes** — the counterparty may have accepted changes "
                    "before sending (the Earthling pattern). The review "
                    "compares it with NCP's last position; ask them to keep "
                    "Track Changes on.")
                try:
                    _, _, prev_rep, _ = _build_for(prev_rounds[-1])
                    cp_changes = nda.format_counterparty_changes(
                        untracked=R.untracked_changes(prev_rep["expected"],
                                                      view["texts"]))
                except Exception:
                    cp_changes = ""

        with st.status("Reviewing the NDA against the rulebook "
                       "(a few minutes with Fable's thinking)...",
                       expanded=True) as _status:
            try:
                review, engine = nda.review_nda(
                    R.format_for_model(view), deal_type, ncp_role,
                    form_source, nda.load_rules(), ctx, cp_changes,
                    signing=nda.get_signing(), anthropic_client=_ac,
                    openai_client=_oc, progress=_status.write)
            except Exception as e:
                _status.update(label="Review failed", state="error")
                st.error(f"Review failed: {e}")
                st.stop()
            _status.update(label="Review complete", state="complete",
                           expanded=False)
        review["_engine"] = engine
        review["_para_texts"] = view["texts"]
        review["_from_pdf"] = from_pdf
        if cp_changes:
            review["_counterparty_changes"] = cp_changes
        nda.add_round(pid, up.name, "\n".join(view["texts"]), review,
                      file_blob=source)
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
            _engine_banner(_vreview)
            try:
                _vbuilt = _build_for(_vround)
                _vrep = _vbuilt[2]
            except Exception:
                _vbuilt, _vrep = None, None
            # Deliverables first: they are what goes back to the banker.
            _render_deliverables(_vround, _vreview, "_nda_view",
                                 built=_vbuilt)
            _render_review(_vreview, _vrep)
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
                _engine_banner(review)
                try:
                    built = _build_for(r)
                except Exception:
                    built = None
                _render_deliverables(r, review, f"r{r['id']}", built=built)
                _render_review(review, built[2] if built else None)
                if st.button(f"🗑️ Delete round {r['round_no']}",
                             key=f"delr_{r['id']}"):
                    nda.delete_round(r["id"])
                    nda.backup_to_github()
                    st.session_state.pop("_nda_view", None)
                    st.rerun()
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
            dc1, dc2 = st.columns([3, 1])
            _del_ok = dc1.checkbox(
                f"Confirm: permanently delete {p['name']} and every round",
                key=f"delok_{p['id']}")
            if dc2.button("🗑️ Delete project", key=f"del_{p['id']}",
                          disabled=not _del_ok, use_container_width=True):
                nda.delete_project(p["id"])
                nda.backup_to_github()
                st.session_state.pop("_nda_view", None)
                st.rerun()
