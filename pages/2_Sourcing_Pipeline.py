import streamlit as st
import time
from datetime import datetime
from pipeline.state import PipelineState
from pipeline.orchestrator import (
    start_pipeline, resume_pipeline, restart_running_pipeline,
    pause_pipeline, stop_pipeline, add_user_feedback,
)


# ---------------------------------------------------------------------------
# STATUS DISPLAY CONSTANTS
# ---------------------------------------------------------------------------
PIPELINE_STATUS_LABELS = {
    "idle":    ("⚪", "Idle"),
    "running": ("🟢", "Running"),
    "paused":  ("🟡", "Paused"),
    "stopped": ("🔴", "Stopped"),
}

SEARCH_STATUS_LABELS = {
    "idle":              ("⚪", "Idle"),
    "searching_apollo":  ("🔍", "Searching Apollo"),
    "discovering_web":   ("🌐", "Web Discovery"),
    "waiting_for_round": ("⏳", "Between Rounds"),
    "exhausted":         ("🛑", "Exhausted"),
    "error":             ("⚠️", "Error"),
}

ANALYSIS_STATUS_LABELS = {
    "idle":               ("⚪", "Idle"),
    "filtering":          ("🔬", "Filtering Candidate"),
    "checking_conflict":  ("🛡️", "Checking Portfolio Conflict"),
    "scoring_conviction": ("🎯", "Scoring Conviction"),
    "error":              ("⚠️", "Error"),
}

WRITEUP_STATUS_LABELS = {
    "idle":             ("⚪", "Idle"),
    "generating_memo":  ("✍️", "Writing Memo"),
    "error":            ("⚠️", "Error"),
}


def _label(d, key):
    return d.get(key, ("❓", key))


# ---------------------------------------------------------------------------
# PAGE SETUP & AUTH
# ---------------------------------------------------------------------------
st.set_page_config(page_title="NCP Sourcing Pipeline", page_icon="🤖", layout="wide")


def _check_password():
    try:
        app_password = st.secrets["APP_PASSWORD"]
    except (FileNotFoundError, KeyError):
        app_password = "NCP2026"

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

# Re-attach background pipeline if one was running.
# restart_running_pipeline() checks if status == "running" and re-spawns the
# orchestrator thread — this is what makes the pipeline survive browser
# disconnects, WiFi drops, and laptop sleep/wake cycles.
state = restart_running_pipeline()
state.reload_from_disk()

st.title("🤖 NCP Autonomous Sourcing Pipeline")
st.caption(
    "Describe your target. Bots search, analyze, and write memos in the background. "
    "Browser-disconnect safe."
)

# Status banner — shows what the pipeline is currently doing
last_event = state.last_event if hasattr(state, "last_event") else None
if last_event and last_event.get("message"):
    severity = last_event.get("severity", "info")
    message = last_event.get("message", "")
    timestamp = last_event.get("timestamp", "")

    # Format timestamp as human-readable relative time
    ts_display = ""
    if timestamp:
        try:
            from datetime import datetime as _dt2, timezone as _tz2
            ts = _dt2.fromisoformat(timestamp)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=_tz2.utc)
            delta = _dt2.now(_tz2.utc) - ts
            secs = int(delta.total_seconds())
            if secs < 60:
                ts_display = f"{secs}s ago"
            elif secs < 3600:
                ts_display = f"{secs // 60}m ago"
            else:
                ts_display = f"{secs // 3600}h ago"
        except Exception:
            ts_display = ""

    full_message = f"{message}" + (f" *(updated {ts_display})*" if ts_display else "")

    if severity == "success":
        st.success(full_message)
    elif severity == "warning":
        st.warning(full_message)
    elif severity == "error":
        st.error(full_message)
    else:
        st.info(full_message)


# ---------------------------------------------------------------------------
# SIDEBAR — Projects + Pipeline Status
# ---------------------------------------------------------------------------
with st.sidebar:
    # ── Project Manager ──────────────────────────────────────────
    from pipeline.projects import (
        list_projects, save_project, load_project, new_project,
        delete_project, current_project_name, update_active_meta,
    )

    st.subheader("Projects")
    _projects = list_projects()
    _active_name = current_project_name()

    if _active_name:
        st.caption(f"Active: **{_active_name}**")
    else:
        st.caption("No project saved yet.")

    with st.expander("Manage Projects", expanded=False):
        # Save current search
        _save_name = st.text_input(
            "Project name",
            value=_active_name or "",
            placeholder="e.g., CMMC Cybersecurity VA",
            key="_proj_save_name",
        )
        if st.button("Save Current Search", use_container_width=True, key="_proj_save"):
            if _save_name.strip():
                if state.status == "running":
                    stop_pipeline()
                save_project(_save_name.strip())
                st.success(f"Saved: **{_save_name.strip()}**")
                st.rerun()
            else:
                st.warning("Enter a project name.")

        st.markdown("---")

        # Load a previous project
        if _projects:
            _proj_names = [p["name"] for p in _projects]
            _selected_proj = st.selectbox(
                "Load a saved project",
                options=_proj_names,
                index=None,
                placeholder="Select a project...",
                key="_proj_select",
            )
            if _selected_proj:
                _sel_meta = next((p for p in _projects if p["name"] == _selected_proj), {})
                _niche_display = _sel_meta.get("niche", "")[:60]
                _geo_display = _sel_meta.get("geography", "")
                _memo_display = _sel_meta.get("memo_count", 0)
                st.caption(
                    f"Niche: {_niche_display or 'N/A'}  \n"
                    f"Geography: {_geo_display or 'N/A'}  \n"
                    f"Memos: {_memo_display}"
                )
                _rc1, _rc2 = st.columns(2)
                with _rc1:
                    if st.button("Resume", key="_proj_load", use_container_width=True):
                        if _active_name and _active_name != _selected_proj:
                            save_project(_active_name)
                        if state.status == "running":
                            stop_pipeline()
                            import time as _t; _t.sleep(1)
                        load_project(_selected_proj)
                        st.rerun()
                with _rc2:
                    if st.button("Delete", key="_proj_del", use_container_width=True):
                        if _selected_proj != _active_name:
                            delete_project(_selected_proj)
                            st.success(f"Deleted: {_selected_proj}")
                            st.rerun()
                        else:
                            st.warning("Can't delete the active project.")

        st.markdown("---")

        # New project
        _new_name = st.text_input(
            "New project name",
            placeholder="e.g., IT Staffing Southeast",
            key="_proj_new_name",
        )
        if st.button("Start New Project", use_container_width=True, key="_proj_new", type="primary"):
            if _new_name.strip():
                if _active_name:
                    save_project(_active_name)
                if state.status == "running":
                    stop_pipeline()
                    import time as _t2; _t2.sleep(1)
                new_project(_new_name.strip())
                st.success(f"New project: **{_new_name.strip()}**")
                st.rerun()
            else:
                st.warning("Enter a name for the new project.")

    st.divider()

    # ── Pipeline Status ──────────────────────────────────────────
    st.subheader("Pipeline Status")
    icon, label = _label(PIPELINE_STATUS_LABELS, state.status)
    st.markdown(f"**{icon} {label}**")

    st.divider()
    st.caption("Bot Statuses")
    bot_status = state.bot_status

    s_icon, s_label = _label(SEARCH_STATUS_LABELS, bot_status.get("search", "idle"))
    a_icon, a_label = _label(ANALYSIS_STATUS_LABELS, bot_status.get("analysis", "idle"))
    w_icon, w_label = _label(WRITEUP_STATUS_LABELS, bot_status.get("writeup", "idle"))
    st.markdown(f"{s_icon} **Search:** {s_label}")
    st.markdown(f"{a_icon} **Analysis:** {a_label}")
    st.markdown(f"{w_icon} **Write-up:** {w_label}")

    st.divider()
    st.caption("Progress")
    cfg = state.config
    st.metric("Memos Completed", f"{len(state.completed_memos)} / {cfg.get('target_count') or 0}")
    st.metric("Candidates in Queue", len(state.candidate_queue))
    st.metric("Qualified Pending Memo", len(state.qualified_queue))

    created_at_iso = state.created_at
    if created_at_iso:
        try:
            from datetime import datetime as _dt, timezone as _tz
            created_dt = _dt.fromisoformat(created_at_iso.replace("Z", "+00:00"))
            delta = _dt.now(_tz.utc) - created_dt
            total_min = int(delta.total_seconds() // 60)
            if total_min < 1:
                age_str = "just now"
            elif total_min < 60:
                age_str = f"{total_min}m ago"
            else:
                hrs, mins = divmod(total_min, 60)
                age_str = f"{hrs}h {mins}m ago"
            st.caption(f"Started {age_str}")
        except Exception:
            pass

    st.divider()
    c1, c2 = st.columns(2)
    with c1:
        if st.button("⏸ Pause", disabled=state.status != "running", use_container_width=True):
            pause_pipeline()
            st.rerun()
    with c2:
        if st.button("▶ Resume", disabled=state.status != "paused", use_container_width=True):
            state.update(status="running")
            restart_running_pipeline()
            st.rerun()

    if st.button(
        "🛑 Stop",
        disabled=state.status in ("idle", "stopped"),
        use_container_width=True,
    ):
        stop_pipeline()
        st.rerun()

    with st.expander("⚠️ Reset Pipeline"):
        st.warning("This deletes all current progress, memos, and chat history.")
        if st.button("Confirm Reset", use_container_width=True):
            state.reset()
            st.rerun()

    # PE Portfolio Cache
    st.markdown("---")
    st.markdown("**PE Portfolio Cache**")

    from lib.portfolio_cache import cache_age_days, is_cache_stale, refresh_portfolio_cache

    _cache_age = cache_age_days()
    _cache_stale = is_cache_stale()

    if _cache_stale:
        st.markdown(
            "<div style='background-color:#ffcccc;padding:10px;border-radius:5px;color:#990000;font-weight:bold;'>"
            "⚠️ PE Portfolio Cache is STALE. Refresh now to avoid missing recent PE deals."
            "</div>",
            unsafe_allow_html=True,
        )
        _cache_button_label = "⚠️ Refresh Cache (STALE)"
        _cache_button_type = "primary"
    else:
        st.caption(f"Last updated: {_cache_age} days ago")
        _cache_button_label = "🔄 Refresh Cache"
        _cache_button_type = "secondary"

    if st.button(_cache_button_label, type=_cache_button_type, use_container_width=True):
        from lib.api_clients import load_api_keys, make_openai_client
        from lib.contacts import firecrawl_scrape as _fc_scrape
        _keys = load_api_keys()
        _oc = make_openai_client(api_key=_keys["OPENAI_API_KEY"])
        def _scrape_for_cache(url):
            return _fc_scrape(_keys["FIRECRAWL_API_KEY"], url)
        with st.spinner("Refreshing PE portfolio cache (this takes 15-30 minutes)..."):
            refresh_portfolio_cache(_oc, _scrape_for_cache, log_fn=lambda msg: st.sidebar.caption(msg))
        st.success("Cache refreshed!")
        st.rerun()


# ---------------------------------------------------------------------------
# MAIN AREA — Tabs
# ---------------------------------------------------------------------------
tab_chat, tab_memos, tab_overview = st.tabs(["💬 Chat with Boss Bot", "📄 Investment Memos", "🔍 Search Overview"])

# ---------------------------------------------------------------------------
# Tab: Chat with Boss Bot
# ---------------------------------------------------------------------------
with tab_chat:
    if state.status == "idle" and not state.chat_history:
        # Startup form
        with st.form("start_form"):
            from lib.constants import NCP_PRIORITY_LABEL
            niche = st.text_area(
                "What kind of companies are you looking for?",
                placeholder="e.g., independent CMMC-focused cybersecurity assessor staffing firms in the eastern US",
            )
            geo_mode = st.radio(
                "Geography",
                options=[NCP_PRIORITY_LABEL, "Custom"],
                index=0,
                horizontal=True,
            )
            custom_geo = ""
            if geo_mode == "Custom":
                custom_geo = st.text_input(
                    "Enter geography",
                    placeholder="e.g., Virginia, United States",
                )
            geography = NCP_PRIORITY_LABEL if geo_mode == NCP_PRIORITY_LABEL else custom_geo.strip()
            target_count = st.number_input(
                "How many differentiated companies should I find?",
                min_value=1,
                max_value=20,
                value=5,
                step=1,
            )
            submitted = st.form_submit_button("🚀 Start Pipeline", type="primary")

            if submitted:
                if not niche.strip():
                    st.error("Please describe what kind of companies you're looking for.")
                elif geo_mode == "Custom" and not geography:
                    st.error("Enter a geography or select NCP Priority Geography.")
                else:
                    start_pipeline(niche.strip(), geography, "A", int(target_count))
                    state.reload_from_disk()
                    state.add_chat(
                        "user",
                        f"Find me {target_count} differentiated {niche.strip()} companies in {geography.strip()}.",
                    )
                    state.add_chat(
                        "assistant",
                        f"Got it. I know what NCP looks for — founder-owned, differentiated, "
                        f"right to win, growing market, no PE on the cap table. I'll search "
                        f"multiple channels and only bring you companies I'm genuinely excited "
                        f"about (conviction 6+/10). You'll get {target_count} with investment "
                        f"memos. Give me feedback on each one and I'll learn your preferences.",
                    )
                    st.rerun()
    else:
        # Render chat history
        for msg in state.chat_history if hasattr(state, "chat_history") else []:
            with st.chat_message(msg.get("role", "assistant")):
                st.markdown(msg.get("content", ""))

        # Chat input
        if user_msg := st.chat_input("Ask, give feedback, or issue a command..."):
            state.add_chat("user", user_msg)

            try:
                from lib.api_clients import load_api_keys, make_openai_client
                from lib.constants import OPENAI_MODEL
                import json as _json
                keys = load_api_keys()
                client = make_openai_client(api_key=keys["OPENAI_API_KEY"])

                # ===== PASS 1: Intent classification =====
                _cfg = state.config or {}
                _completed = len(state.completed_memos or [])
                classifier_prompt = f"""You classify the intent of a message sent to a PE sourcing pipeline assistant.

Possible intents:
- "command": user wants to change something about the running pipeline
- "question": user is asking about state, progress, candidates, memos
- "memo_feedback": user is reacting to a specific memo and wants the search to adapt accordingly. Examples: "this one is too big, find smaller", "this is already PE-backed, exclude Shore Capital", "find more like this but in Texas", "the memo for X looks great, find 3 more like it"
- "feedback": user is sharing general reaction/criticism that doesn't clearly map to a search change
- "smalltalk": greetings, acknowledgments, off-topic

If "command", also identify the specific action and arguments. Supported commands:

1. "stop" — user wants to halt the pipeline. No args.
2. "pause" — user wants to pause. No args.
3. "resume" — user wants to resume. No args.
4. "change_geography" — args: {{"new_geography": "<location>"}}.
   Examples: "search Texas instead", "expand to the southeast", "switch geography to Atlanta".
5. "change_target_count" — args: {{"new_count": <int>}}.
   Examples: "find 5 instead of 1", "I want 3 memos", "increase target to 10".
6. "broaden_search" — user wants wider search. No args.
   Examples: "broaden", "expand the search", "look more broadly", "this is too narrow".
7. "narrow_search" — user wants more specific keywords. args: {{"new_keywords": "<comma-separated>"}}.
   Examples: "focus on cybersecurity assessors", "narrow to staffing only".
8. "find_more" — user wants ADDITIONAL memos, possibly in a new geography. Use
   when user says "find me X more" or "find me X in [place]" AFTER the pipeline
   already completed or is idle.
   args: {{"additional_count": <int>, "new_geography": "<location or null>"}}.
   Examples: "find me 3 more in Texas", "find one more in Virginia and two more
   in Maryland" (additional_count=3, new_geography="Virginia, Maryland"),
   "get me 5 more" (additional_count=5, new_geography=null — keep current).
   IMPORTANT: additional_count is how many MORE they want on top of the
   {_completed} already completed. If they say "one more in VA and two more in MD"
   that is additional_count=3.

User message: "{user_msg}"

Recent pipeline state:
- Status: {state.status}
- Niche: {_cfg.get('niche', 'none')}
- Geography: {_cfg.get('geography', 'none')}
- Target: {_cfg.get('target_count', 'none')}
- Completed memos: {_completed}

Return JSON only:
{{"intent": "command|question|memo_feedback|feedback|smalltalk", "command": {{"action": "...", "args": {{...}}}} or null, "rationale": "one sentence"}}"""

                try:
                    classify_resp = client.chat.completions.create(
                        model="gpt-4o-mini",
                        messages=[{"role": "user", "content": classifier_prompt}],
                        response_format={"type": "json_object"},
                        temperature=0,
                        timeout=15,
                    )
                    classification = _json.loads(classify_resp.choices[0].message.content or "{}")
                except Exception:
                    classification = {"intent": "question", "command": None, "rationale": "classifier failed"}

                intent = classification.get("intent", "question")
                command = classification.get("command")

                # ===== EXECUTE COMMAND OR MEMO FEEDBACK =====
                command_result = None

                # ===== HANDLE MEMO FEEDBACK INTENT (Layer 4) =====
                if intent == "memo_feedback":
                    memos = state.completed_memos or []
                    if not memos:
                        command_result = {"success": False, "message": "No memos to give feedback on yet."}
                    else:
                        # Identify which memo: most recent if not specified
                        target_memo = memos[-1]
                        user_msg_lower = user_msg.lower()
                        for m in memos:
                            if m.get("company", "").lower() in user_msg_lower:
                                target_memo = m
                                break

                        memo_text = target_memo.get("memo", "")
                        company_name = target_memo.get("company", "Unknown")
                        _p_cfg = state.config or {}

                        pivot_prompt = f"""Translate user feedback on a PE investment memo into a structured search pivot directive.

Current pipeline configuration:
- Niche: {_p_cfg.get('niche', '')}
- Geography: {_p_cfg.get('geography', '')}
- Target count: {_p_cfg.get('target_count', '')}

The memo in question:
Company: {company_name}
Memo content (first 2000 chars): {memo_text[:2000]}

User's feedback:
"{user_msg}"

Translate this feedback into a structured pivot. Return JSON only:
{{
  "rationale": "what the user wants in one sentence",
  "new_size_max": null or integer (max employees, only if user implied size is too big),
  "new_size_min": null or integer (only if user implied too small),
  "new_geography": null or string (only if user wants different geography),
  "new_niche_addition": null or string (refinement to add to niche, e.g. "must not be PE-backed already"),
  "exclude_companies": [] or list of company names to exclude from future results,
  "additional_keywords": null or string (comma-separated tags),
  "clear_queue": true or false (true if changes are significant enough to discard pending candidates),
  "user_facing_summary": "I'm pivoting to: <natural language summary>"
}}

Be specific. If user said "too big," infer a reasonable new_size_max from the memo's stated employee count (cut it in half or a third). If user said the company is already PE-backed, add the company name to exclude_companies AND add "must not currently be PE-backed" to new_niche_addition. If user said "find more like this," set clear_queue=false and don't restrict size; user wants similar candidates."""

                        try:
                            pivot_resp = client.chat.completions.create(
                                model=OPENAI_MODEL,
                                messages=[{"role": "user", "content": pivot_prompt}],
                                response_format={"type": "json_object"},
                                temperature=0,
                                timeout=20,
                            )
                            pivot_args = _json.loads(pivot_resp.choices[0].message.content or "{}")
                            command_result = state.apply_command({"action": "pivot", "args": pivot_args})
                            if command_result and command_result.get("restart"):
                                restart_running_pipeline()
                        except Exception as e:
                            command_result = {"success": False, "message": f"Could not translate feedback into pivot: {e}"}

                if intent == "command" and command:
                    command_result = state.apply_command(command)
                    if command_result and command_result.get("restart"):
                        restart_running_pipeline()

                # ===== PASS 2: Conversational response =====
                def _build_system_prompt(st_obj):
                    p_config = st_obj.config or {}
                    p_status = st_obj.status
                    p_bot_status = st_obj.bot_status or {}
                    p_last_event = st_obj.last_event or {}
                    p_memo_count = len(st_obj.completed_memos or [])
                    p_qualified_count = len(st_obj.qualified_queue or [])
                    p_candidate_count = len(st_obj.candidate_queue or [])

                    memos_summary = "None yet."
                    if st_obj.completed_memos:
                        names = [m.get("company", "Unknown") for m in st_obj.completed_memos]
                        memos_summary = ", ".join(names)

                    cmd_section = ""
                    if command_result:
                        cmd_section = f"\n\nIMPORTANT: I just executed a command from the user's message. Result: {command_result.get('message')} (success: {command_result.get('success')}). Acknowledge this naturally in your reply."
                    elif intent == "feedback":
                        cmd_section = "\n\nThe user gave general feedback. Acknowledge what you understood and tell them you've noted it for the analyst."

                    return f"""You are the Boss Bot for Trey Miller's NCP sourcing pipeline at New Capital Partners (lower-middle-market PE firm).

Your role: natural conversation with Trey about pipeline progress and findings. Be concise, professional, direct. Talk to him as a peer.

CURRENT PIPELINE STATE:
Pipeline status: {p_status}
Niche: {p_config.get('niche') or 'Not configured'}
Geography: {p_config.get('geography') or 'Not configured'}
Target memos: {p_config.get('target_count') or 'Not set'}
Completed: {p_memo_count} ({memos_summary})
Qualified pending memo: {p_qualified_count}
Candidates in queue: {p_candidate_count}
Search bot: {p_bot_status.get('search', 'idle')}
Analysis bot: {p_bot_status.get('analysis', 'idle')}
Write-up bot: {p_bot_status.get('writeup', 'idle')}
Last event: {p_last_event.get('message', 'None')}{cmd_section}

RULES:
- Don't invent facts not in the state above
- Keep responses 1-3 sentences typical
- If a command was just executed, confirm it conversationally without being robotic
- If the user's feedback or command is unclear, ask a clarifying question"""

                system_prompt = _build_system_prompt(state)
                messages = [{"role": "system", "content": system_prompt}]
                for m in (state.chat_history or [])[-20:]:
                    role = m.get("role", "user")
                    content = m.get("content", "")
                    if role in ("user", "assistant") and content:
                        messages.append({"role": role, "content": content})

                resp = client.chat.completions.create(
                    model=OPENAI_MODEL,
                    messages=messages,
                    temperature=0.4,
                    timeout=30,
                )
                reply = (resp.choices[0].message.content or "").strip()
                if not reply:
                    reply = "I didn't catch that — could you rephrase?"

            except Exception as e:
                reply = f"Sorry, I had trouble responding: {e}"

            state.add_chat("assistant", reply)
            st.rerun()

# ---------------------------------------------------------------------------
# Tab: Investment Memos
# ---------------------------------------------------------------------------
with tab_memos:
    if not state.completed_memos:
        st.info(
            "No memos yet. The Write-up Bot will populate this as qualified companies "
            "are identified."
        )
    else:
        # Download all memos
        combined_md = ""
        for memo in state.completed_memos:
            combined_md += f"# {memo['company']}\n\n" + memo["memo"] + "\n\n---\n\n"
        st.download_button(
            "📥 Download All Memos",
            data=combined_md,
            file_name=f"NCP_memos_{datetime.now().strftime('%Y%m%d_%H%M')}.md",
            mime="text/markdown",
        )

        # Individual memo expanders
        for memo in state.completed_memos:
            row = memo.get("row", {})
            conv = row.get("Conviction", "")
            is_closest_fit = memo.get("closest_fit", False)
            conv_label = f" — Conviction {conv}/10" if conv else ""
            if is_closest_fit:
                conv_label += " ⚠️ CLOSEST FIT"
            with st.expander(
                f"{memo['company']} — {row.get('City', '')}, {row.get('State', '')}{conv_label}"
            ):
                # Closest-fit warning banner
                if is_closest_fit:
                    cf_reason = memo.get("closest_fit_reason", "Below conviction threshold")
                    st.warning(
                        f"**Closest Fit — Not a recommendation.** This company came closest to qualifying "
                        f"but didn't clear the conviction bar. Reason: {cf_reason}. "
                        f"Review it to help calibrate whether the bar is too high or the niche is too narrow."
                    )

                # Conviction pitch banner
                conv_pitch = row.get("Conviction Pitch", "")
                if conv_pitch:
                    st.info(f"**Why we're excited:** {conv_pitch}")

                left, right = st.columns(2)
                with left:
                    st.markdown(f"**City/State:** {row.get('City', 'N/A')}, {row.get('State', 'N/A')}")
                    st.markdown(f"**Employees:** {row.get('Employees', 'N/A')}")
                    st.markdown(f"**Est. EBITDA:** {row.get('Est. EBITDA', 'N/A')}")
                    st.markdown(f"**Website:** {row.get('Website', 'N/A')}")
                with right:
                    if conv:
                        st.markdown(f"**Conviction:** {conv}/10")
                    st.markdown(f"**Differentiated:** {row.get('Differentiated', 'N/A')}")
                    st.markdown(f"**Priority:** {row.get('Priority', 'N/A')}")
                    st.markdown(f"**Growth:** {row.get('Growth', 'N/A')}")
                    st.markdown(f"**Txn Readiness:** {row.get('Txn Readiness', 'N/A')}")

                st.markdown("---")
                st.markdown(memo["memo"])

                if row.get("Email") and row.get("Email") != "N/A":
                    st.markdown(
                        f"**Contact:** {row.get('CEO/Owner Name', '')} — {row.get('Email')}"
                    )

                # ── Salesforce + Outreach Actions ─────────────────────
                _co_key = memo['company'].replace(' ', '_')
                act_cols = st.columns(3)

                with act_cols[0]:
                    if st.button(
                        "Add to Salesforce",
                        key=f"sf_{_co_key}",
                        type="primary",
                        use_container_width=True,
                    ):
                        try:
                            from lib.salesforce import (
                                sf_login, push_to_salesforce,
                                find_existing_account,
                            )
                            from lib.api_clients import get_secret
                            sf = sf_login(
                                get_secret("SF_USERNAME"),
                                get_secret("SF_PASSWORD"),
                                get_secret("SF_CONSUMER_KEY"),
                                get_secret("SF_CONSUMER_SECRET"),
                                get_secret("SF_SECURITY_TOKEN", ""),
                            )
                            existing = find_existing_account(sf, memo["company"])
                            if existing:
                                st.warning(
                                    f"**{memo['company']}** already exists "
                                    f"in Salesforce (Account: `{existing}`)."
                                )
                            else:
                                acct_id, contact_id = push_to_salesforce(sf, row)
                                st.success(
                                    f"Created in Salesforce — "
                                    f"Account: `{acct_id}` | Contact: `{contact_id}`"
                                )
                        except Exception as e:
                            st.error(f"Salesforce error: {e}")

                with act_cols[1]:
                    if st.button(
                        "Draft Outreach",
                        key=f"draft_{_co_key}",
                        use_container_width=True,
                    ):
                        try:
                            from lib.outreach import draft_cold_email
                            from lib.api_clients import load_api_keys, make_openai_client
                            _keys = load_api_keys()
                            _oc = make_openai_client(api_key=_keys["OPENAI_API_KEY"])
                            _thesis = {}
                            try:
                                import json as _json2
                                with open("ncp_thesis.json") as f:
                                    _thesis = _json2.load(f)
                            except Exception:
                                pass
                            enriched = {**row, "_niche": cfg.get("niche", "")}
                            draft = draft_cold_email(_oc, enriched, _thesis)
                            st.session_state[f"_pipe_draft_subj_{_co_key}"] = draft["subject"]
                            st.session_state[f"_pipe_draft_body_{_co_key}"] = draft["body"]
                            st.rerun()
                        except Exception as e:
                            st.error(f"Email drafting error: {e}")

                with act_cols[2]:
                    _has_draft = st.session_state.get(f"_pipe_draft_body_{_co_key}")
                    if _has_draft:
                        from lib.outreach import make_mailto_url
                        _to = row.get("Email", "")
                        if _to == "N/A":
                            _to = row.get("Email Estimate", "")
                        _mailto = make_mailto_url(
                            _to,
                            st.session_state.get(f"_pipe_draft_subj_{_co_key}", ""),
                            st.session_state.get(f"_pipe_draft_body_{_co_key}", ""),
                        )
                        st.link_button(
                            "Open in Outlook",
                            url=_mailto,
                            use_container_width=True,
                        )
                    else:
                        st.button(
                            "Open in Outlook",
                            key=f"outlook_{_co_key}",
                            disabled=True,
                            help="Draft an email first",
                            use_container_width=True,
                        )

                # Show draft + log-to-SF + calendar if available
                _draft_body = st.session_state.get(f"_pipe_draft_body_{_co_key}")
                if _draft_body:
                    st.markdown("---")
                    st.markdown(f"**Subject:** {st.session_state.get(f'_pipe_draft_subj_{_co_key}', '')}")
                    st.text_area(
                        "Email Draft (edit before sending)",
                        value=_draft_body,
                        height=200,
                        key=f"draft_editor_{_co_key}",
                    )
                    _act_c1, _act_c2, _act_c3 = st.columns([1, 1, 2])
                    with _act_c1:
                        _log_clicked = st.button("Log to Salesforce", key=f"log_sf_{_co_key}", use_container_width=True)
                    with _act_c2:
                        from lib.outreach import generate_followup_ics
                        _cn = row.get("CEO/Owner Name", "Contact")
                        _ph = row.get("Phone", "")
                        if _ph == "N/A":
                            _ph = ""
                        _em = row.get("Email", "")
                        if _em == "N/A":
                            _em = row.get("Email Estimate", "")
                        _ics = generate_followup_ics(
                            memo["company"], _cn, phone=_ph, email=_em,
                        )
                        st.download_button(
                            "Add Reminders to Calendar",
                            data=_ics,
                            file_name=f"followup_{_co_key}.ics",
                            mime="text/calendar",
                            key=f"ics_{_co_key}",
                            use_container_width=True,
                        )
                    with _act_c3:
                        st.caption(
                            "**1)** Send via Outlook **2)** Add follow-up reminders "
                            "to your calendar **3)** Log to Salesforce"
                        )

                    if _log_clicked:
                        try:
                            from lib.salesforce import (
                                sf_login, find_existing_account,
                                find_contact_for_account,
                                log_outreach_activity,
                                create_followup_tasks,
                            )
                            from lib.api_clients import get_secret
                            sf = sf_login(
                                get_secret("SF_USERNAME"),
                                get_secret("SF_PASSWORD"),
                                get_secret("SF_CONSUMER_KEY"),
                                get_secret("SF_CONSUMER_SECRET"),
                                get_secret("SF_SECURITY_TOKEN", ""),
                            )
                            acct_id = find_existing_account(sf, memo["company"])
                            if not acct_id:
                                st.warning("Add to Salesforce first.")
                            else:
                                contact_id = find_contact_for_account(sf, acct_id)
                                task_id = log_outreach_activity(
                                    sf, acct_id, contact_id,
                                    st.session_state.get(f"_pipe_draft_subj_{_co_key}", ""),
                                    _draft_body,
                                )
                                call_id, fu_id = create_followup_tasks(
                                    sf, acct_id, contact_id, memo["company"],
                                )
                                st.success(
                                    f"Logged — Email: `{task_id}` | "
                                    f"Call follow-up (tomorrow): `{call_id}` | "
                                    f"Email follow-up (day 3): `{fu_id}`"
                                )
                        except Exception as e:
                            st.error(f"Salesforce logging error: {e}")

                st.markdown("---")

                # Feedback buttons
                fb_key = f"fb_{memo['company'].replace(' ', '_')}"
                fb_cols = st.columns(3)
                with fb_cols[0]:
                    if st.button("👍 Interested", key=f"{fb_key}_yes"):
                        from lib.feedback import save_feedback
                        save_feedback(memo["company"], "Interested — wants to pursue", niche=cfg.get("niche"), verdict="liked")
                        st.success("Noted — we'll find more like this.")
                with fb_cols[1]:
                    if st.button("👎 Pass", key=f"{fb_key}_no"):
                        from lib.feedback import save_feedback
                        save_feedback(memo["company"], "Passed", niche=cfg.get("niche"), verdict="rejected")
                        st.info("Noted.")
                with fb_cols[2]:
                    fb_text = st.text_input("Feedback", key=f"{fb_key}_text", placeholder="e.g., too big, not differentiated enough")
                    if fb_text:
                        from lib.feedback import save_feedback
                        save_feedback(memo["company"], fb_text, niche=cfg.get("niche"), verdict="caveats")
                        st.info("Feedback saved — future searches will learn from this.")


# ---------------------------------------------------------------------------
# Tab: Search Overview (filtration funnel)
# ---------------------------------------------------------------------------
with tab_overview:
    try:
        fs = state.filter_stats
    except (AttributeError, KeyError):
        fs = {}

    total = fs.get("total_sourced", 0)

    if total == 0:
        st.info("No candidates analyzed yet. Start a search to see the filtration funnel.")
    else:
        st.subheader("Filtration Funnel")

        pf_size = fs.get("pre_filtered_size", 0)
        pf_struct = fs.get("pre_filtered_structural", 0)
        pf_block = fs.get("pre_filtered_blocklist", 0)
        pf_niche = fs.get("pre_filtered_niche", 0)
        pf_total = pf_size + pf_struct + pf_block + pf_niche

        deep_failed = fs.get("deep_analysis_failed", 0)
        pe = fs.get("pe_backed", 0)
        conflict = fs.get("portfolio_conflict", 0)
        low_diff = fs.get("low_differentiation", 0)
        qualified = fs.get("qualified", 0)
        memos = len(state.completed_memos or [])

        sent_to_deep = total - pf_total
        remaining_queue = len(state.candidate_queue or [])

        st.markdown(f"""
| Stage | Count | % of Total |
|-------|------:|----------:|
| **Candidates Sourced** | **{total}** | 100% |
| Pre-filtered *(zero API cost)* | {pf_total} | {pf_total*100//max(total,1)}% |
| &nbsp;&nbsp;&nbsp;&nbsp;Size caps | {pf_size} | |
| &nbsp;&nbsp;&nbsp;&nbsp;Gov / nonprofit / public | {pf_struct} | |
| &nbsp;&nbsp;&nbsp;&nbsp;Name blocklist | {pf_block} | |
| &nbsp;&nbsp;&nbsp;&nbsp;No niche signal | {pf_niche} | |
| Sent to deep analysis | {sent_to_deep} | {sent_to_deep*100//max(total,1)}% |
| &nbsp;&nbsp;&nbsp;&nbsp;Failed AI relevance / filters | {deep_failed} | |
| &nbsp;&nbsp;&nbsp;&nbsp;PE-backed | {pe} | |
| &nbsp;&nbsp;&nbsp;&nbsp;Portfolio conflict | {conflict} | |
| &nbsp;&nbsp;&nbsp;&nbsp;Low conviction | {low_diff} | |
| **Qualified** | **{qualified}** | {qualified*100//max(total,1)}% |
| **Memos Generated** | **{memos}** | |
| Still in queue | {remaining_queue} | |
""")

        st.divider()

        if pf_total > 0:
            st.caption("Pre-filter effectiveness")
            cols = st.columns(4)
            cols[0].metric("Size", pf_size)
            cols[1].metric("Structural", pf_struct)
            cols[2].metric("Blocklist", pf_block)
            cols[3].metric("No Niche Signal", pf_niche)

            savings_pct = pf_total * 100 // max(total, 1)
            st.success(
                f"Pre-filters saved {pf_total} API calls ({savings_pct}% of candidates blocked at zero cost)."
            )

        if sent_to_deep > 0 and qualified > 0:
            hit_rate = qualified * 100 // max(sent_to_deep, 1)
            st.info(f"Deep analysis hit rate: {hit_rate}% ({qualified} qualified out of {sent_to_deep} analyzed)")


# ---------------------------------------------------------------------------
# AUTO-REFRESH POLLING + project metadata sync
# ---------------------------------------------------------------------------
if current_project_name():
    update_active_meta()

if state.status == "running":
    time.sleep(3)
    st.rerun()
