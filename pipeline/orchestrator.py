"""Pipeline orchestrator — background daemon-thread coordinator.

Runs three bot stages (Search, Analysis, Write-up) in a loop,
persisting all state to disk via PipelineState.
"""

import threading
import time
import json
import re
import concurrent.futures

from lib.api_clients import load_api_keys, make_openai_client
from lib.ai_params import suggest_search_params
from lib.apollo_search import search_organizations, web_discovery_pass
from lib.contacts import firecrawl_scrape, clean_domain
from lib.worker import process_single_company
from lib.ncp_portfolio import check_portfolio_conflict
from lib.constants import OPENAI_MODEL
from pipeline.state import PipelineState


_thread = None
_thread_lock = threading.Lock()


# ---------------------------------------------------------------------------
# PUBLIC API
# ---------------------------------------------------------------------------
def start_pipeline(niche, geography, strategy="A", target_count=5):
    """Initialize a fresh pipeline run and start the background loop."""
    state = PipelineState()
    state.reset()
    state.update(
        config={
            "niche": niche,
            "geography": geography,
            "strategy": strategy,
            "target_count": target_count,
        },
        status="running",
    )
    _ensure_thread_running()
    return state


def resume_pipeline():
    """Reload existing state from disk. Does NOT auto-start the orchestrator —
    call start_pipeline() (or the Resume button) explicitly."""
    state = PipelineState()
    return state


def restart_running_pipeline():
    """Re-attach orchestrator thread if state.status == 'running'.
    Used by the Resume button after an explicit user action."""
    state = PipelineState()
    if state.status == "running":
        _ensure_thread_running()
    return state


def pause_pipeline():
    """Pause the pipeline (loop stays alive but idles)."""
    state = PipelineState()
    state.update(status="paused")


def stop_pipeline():
    """Stop the pipeline (loop will exit on next iteration)."""
    state = PipelineState()
    state.update(status="stopped")


def add_user_feedback(text):
    """Append user feedback to the pipeline state."""
    state = PipelineState()
    state.add_feedback(text)


# ---------------------------------------------------------------------------
# INTERNAL — THREAD MANAGEMENT
# ---------------------------------------------------------------------------
def _ensure_thread_running():
    global _thread
    with _thread_lock:
        if _thread is None or not _thread.is_alive():
            _thread = threading.Thread(target=_run_loop, daemon=True)
            _thread.start()


# ---------------------------------------------------------------------------
# INTERNAL — MEMO GENERATION
# ---------------------------------------------------------------------------
def _generate_memo(client, row, niche):
    """Generate a structured 1-page investment memo for a qualified candidate."""
    company = row.get("Company", "Unknown")
    description = row.get("Description", "")
    city = row.get("City", "")
    state_abbr = row.get("State", "")
    employees = row.get("Employees", "")
    ebitda = row.get("Est. EBITDA", "")
    differentiated = row.get("Differentiated", "")
    priority = row.get("Priority", "")
    growth = row.get("Growth", "")

    ebitda_caveat = (
        f"Estimated {ebitda} EBITDA (heuristic based on employee count; requires confirmation)"
        if ebitda
        else "EBITDA not available; primary diligence item"
    )

    prompt = f"""Write a concise 1-page investment memo for a lower middle market private equity firm
evaluating the following company as a potential acquisition target in the "{niche}" space.

Company: {company}
Location: {city}, {state_abbr}
Employees: {employees}
Est. EBITDA: {ebitda_caveat}
Description: {description}
Differentiation Score: {differentiated}
Priority Score: {priority}
Growth Score: {growth}

CRITICAL — Honesty rules for this memo:
- The "Est. EBITDA" value above is a heuristic estimate based on employee count, not real financial data. Always present it with the caveat that it requires confirmation via diligence.
- Do NOT invent ownership status, growth rates, customer concentration, or financial metrics that aren't provided in the input data.
- If you don't have a fact, write "Not available; primary diligence item" rather than speculating.
- The memo is a starting point for analyst review, not a final document. Bias toward honest gaps over polished invention.

Structure the memo with exactly these five sections:

1. **Company Overview** — What the company does, where it operates, approximate scale.
2. **Differentiated Value Proposition** — What makes this company stand out from competitors in the niche.
3. **Market Opportunity & Growth** — TAM/SAM dynamics, secular tailwinds, growth levers.
4. **NCP Fit Rationale** — Why this fits NCP's lower middle market, services-oriented thesis.
5. **Key Risks & Diligence Items** — Top 3-5 risks or open questions for due diligence.

Keep it factual and concise. If information is limited, note what needs further diligence
rather than speculating. Write in a professional PE memo tone."""

    try:
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            timeout=45,
        )
        return resp.choices[0].message.content
    except Exception as e:
        return f"[Memo generation failed: {e}]"


# ---------------------------------------------------------------------------
# INTERNAL — MAIN LOOP
# ---------------------------------------------------------------------------
def _run_loop():
    """Background loop that coordinates Search → Analysis → Write-up."""
    # Load API keys once for the entire loop lifetime
    try:
        keys = load_api_keys()
    except RuntimeError as e:
        print(f"[Orchestrator] Failed to load API keys: {e}")
        return

    client = make_openai_client(api_key=keys["OPENAI_API_KEY"])
    apollo_key = keys["APOLLO_API_KEY"]
    firecrawl_key = keys["FIRECRAWL_API_KEY"]
    user_agent = keys["HTTP_USER_AGENT"]

    # Curried firecrawl scraper for web_discovery_pass
    def _scrape(url):
        return firecrawl_scrape(firecrawl_key, url)

    # PE portfolio cache — refresh in background if stale
    from lib.portfolio_cache import is_cache_stale, refresh_portfolio_cache

    if is_cache_stale():
        print("[Orchestrator] PE portfolio cache is stale. Refreshing in background...")
        def _bg_refresh():
            try:
                refresh_portfolio_cache(client, _scrape, log_fn=print)
                print("[Orchestrator] Portfolio cache refresh complete.")
            except Exception as e:
                print(f"[Orchestrator] Portfolio cache refresh failed: {e}")
        threading.Thread(target=_bg_refresh, daemon=True).start()

    # Sticky-status helper: "exhausted" must not be overwritten to "idle"
    _STICKY_STATUSES = {"exhausted"}

    def _resolve_final_status(bot_key, new_status, current_bot_status):
        """Return new_status unless the current status is sticky."""
        if current_bot_status.get(bot_key) in _STICKY_STATUSES:
            return current_bot_status[bot_key]
        return new_status

    # Search-strategy state
    search_params = None      # populated on first search iteration
    industry_index = 0        # which industry we're on
    industries_done = False
    web_discovery_done = False
    search_exhausted = False
    _search_round = 1
    _last_known_geography = None  # detect mid-flight geography changes

    # Create state once, reuse across iterations (mtime-cached reload)
    state = PipelineState()

    while True:
        try:
            state.reload_from_disk()

            config = state.config
            niche = config["niche"]
            geography = config["geography"]
            strategy = config.get("strategy", "A")
            target_count = config["target_count"]

            # --- Detect mid-flight config changes ---
            # If pivot_signal was set, force search params to recompute
            if config.get("pivot_signal"):
                search_params = None
                industries_done = False
                web_discovery_done = False
                industry_index = 0
                search_exhausted = False
                state.batch_update(config={"pivot_signal": False})
                state.set_event("pivoted", "Search pivoted based on user feedback. Restarting with new parameters.", "info")

            # If broaden_signal was set, force search params to reset and broaden
            if config.get("broaden_signal"):
                search_params = None  # Force recompute
                industries_done = False
                web_discovery_done = False
                industry_index = 0
                _search_round = max(_search_round, 3)  # Jump to broader round
                search_exhausted = False
                # Clear the signal
                state.batch_update(config={"broaden_signal": False})
                state.set_event("broadened", "Search broadened by user request. Restarting with wider parameters.", "info")

            # If geography was changed, force search reset
            if _last_known_geography != geography:
                search_params = None
                industries_done = False
                web_discovery_done = False
                industry_index = 0
                search_exhausted = False
                _last_known_geography = geography
                state.set_event("geography_changed", f"Geography changed to {geography}. Restarting search.", "info")

            # If additional_keywords were added, append them to current search keyword list
            if config.get("additional_keywords") and search_params:
                extra = [k.strip() for k in config["additional_keywords"].split(",") if k.strip()]
                existing = search_params.get("keywords", "")
                existing_list = [k.strip() for k in existing.split(",") if k.strip()]
                combined = list(set(existing_list + extra))
                search_params["keywords"] = ", ".join(combined)
                # Clear the signal so we don't re-add every iteration
                state.batch_update(config={"additional_keywords": ""})
                state.set_event("narrowed", f"Search narrowed with new keywords: {', '.join(extra)}.", "info")

            # --- Emit starting event ---
            if state.status == "running" and state.last_event.get("type") not in ("searching_apollo", "discovering_web", "analyzing", "writing_memo"):
                state.set_event("starting", f"Pipeline starting. Niche: {niche}, geography: {geography}, target: {target_count} memos.", "info")

            # --- Check stop/pause/complete conditions ---
            if state.status == "stopped":
                state.set_event("stopped", "Pipeline stopped by user.", "info")
                print("[Orchestrator] Pipeline stopped.")
                break
            if state.status == "paused":
                time.sleep(5)
                continue
            if len(state.completed_memos) >= target_count:
                state.set_event("done", f"Pipeline complete! All {target_count} memos generated. Click 'Investment Memos' tab to review.", "success")
                state.update(status="idle")
                print(f"[Orchestrator] Reached target of {target_count} memos. Done.")
                break

            # Track final statuses for end-of-iteration reconciliation
            search_final = None
            analysis_final = None
            writeup_final = None

            # =======================================================================
            # SEARCH BOT
            # =======================================================================
            try:
                if search_exhausted:
                    search_final = "exhausted"
                elif len(state.candidate_queue) >= 20:
                    search_final = "idle"
                else:
                    # First iteration: ask AI for Apollo search params
                    if search_params is None:
                        search_params = suggest_search_params(client, niche)

                    industries = search_params.get("industries", [])
                    keyword_tags = [
                        t.strip()
                        for t in (search_params.get("keywords") or "").split(",")
                        if t.strip()
                    ]

                    # Determine max_pages and keyword behavior based on round
                    if _search_round == 1:
                        round_max_pages = 3
                        round_keyword_tags = keyword_tags
                    elif _search_round == 2:
                        round_max_pages = 6
                        round_keyword_tags = keyword_tags
                    else:
                        round_max_pages = 10
                        round_keyword_tags = None  # broaden search

                    seen_domains = set(state.seen_domains)
                    seen_names = set(state.seen_names)

                    if not industries_done and industry_index < len(industries):
                        # Apollo industry search
                        industry = industries[industry_index]
                        state.batch_update(bot_status={"search": "searching_apollo"})
                        state.set_event("searching_apollo", f"Searching Apollo for {industry} companies in {geography} (round {_search_round} of 4).", "info")
                        print(f"[Search Bot] Round {_search_round} — Apollo industry search: {industry} (max_pages={round_max_pages})")
                        orgs = search_organizations(
                            apollo_key,
                            industries=[industry],
                            location_input=geography,
                            keyword_tags=round_keyword_tags if industry_index == 0 else None,
                            max_pages=round_max_pages,
                        )

                        # Accumulate new orgs and seen-set additions, then batch-write once
                        new_orgs = []
                        new_domains = []
                        new_names = []
                        for org in orgs:
                            domain = clean_domain(org.get("website_url"))
                            name_lower = (org.get("name") or "").strip().lower()
                            if domain and domain in seen_domains:
                                continue
                            if name_lower and name_lower in seen_names:
                                continue
                            new_orgs.append(org)
                            if domain:
                                seen_domains.add(domain)
                                new_domains.append(domain)
                            if name_lower:
                                seen_names.add(name_lower)
                                new_names.append(name_lower)

                        if new_orgs:
                            state.add_candidates_batch(new_orgs, new_domains, new_names)

                        industry_index += 1
                        if industry_index >= len(industries):
                            industries_done = True

                        search_final = "idle"

                    elif industries_done and not web_discovery_done:
                        # Web discovery pass
                        state.batch_update(bot_status={"search": "discovering_web"})
                        state.set_event("discovering_web", f"Web discovery pass — round {_search_round} of 4.", "info")
                        print(f"[Search Bot] Round {_search_round} — Web discovery pass")
                        discovered_orgs = web_discovery_pass(
                            client,
                            _scrape,
                            clean_domain,
                            niche,
                            geography,
                            seen_domains,
                            seen_names,
                            user_agent=user_agent,
                        )

                        # Batch-write all discovered orgs + their seen entries
                        new_domains = []
                        new_names = []
                        for org in discovered_orgs:
                            domain = clean_domain(org.get("website_url"))
                            name_lower = (org.get("name") or "").strip().lower()
                            if domain:
                                new_domains.append(domain)
                            if name_lower:
                                new_names.append(name_lower)

                        if discovered_orgs:
                            state.add_candidates_batch(discovered_orgs, new_domains, new_names)

                        web_discovery_done = True
                        print(f"[Search Bot] Round {_search_round} complete.")

                        # Reset for next round
                        if _search_round < 4:
                            _search_round += 1
                            industry_index = 0
                            industries_done = False
                            web_discovery_done = False
                            print(f"[Search Bot] Starting round {_search_round}.")
                            search_final = "waiting_for_round"
                        else:
                            # Round 4 done — check if we should truly exhaust
                            state.reload_from_disk()
                            if not state.candidate_queue and len(state.completed_memos) < target_count:
                                search_exhausted = True
                                state.set_event("exhausted", f"Search exhausted after 4 rounds. Found {len(state.completed_memos)}/{target_count} memos. Try broadening niche or geography, or lowering target count.", "warning")
                                state.batch_update(
                                    bot_status={"search": "exhausted"},
                                    status="stopped",
                                )
                                print(
                                    f"[Search Bot] Exhausted all 4 search rounds. "
                                    f"Only {len(state.completed_memos)}/{target_count} memos completed. "
                                    f"Could not find enough qualified companies. Stopping pipeline."
                                )
                                search_final = "exhausted"
                            else:
                                search_exhausted = True
                                search_final = "exhausted"

                    else:
                        search_final = "idle"

            except Exception as e:
                print(f"[Search Bot] Error: {e}")
                search_final = "error"

            # =======================================================================
            # ANALYSIS BOT
            # =======================================================================
            try:
                state.reload_from_disk()
                if not state.candidate_queue:
                    analysis_final = "idle"
                else:
                    state.batch_update(bot_status={"analysis": "filtering"})

                    org = state.pop_candidate()
                    if org:
                        comp_name = org.get("name", "Unknown")

                        # Apply per-run size overrides if configured
                        override_max = config.get("override_size_max")
                        override_min = config.get("override_size_min")
                        emp = org.get("estimated_num_employees", 0) or 0
                        skip_analysis = False
                        if override_max is not None and emp > override_max:
                            state.set_event("filtered_oversize", f"Filtered out {comp_name}: {emp} employees > override max {override_max}", "info")
                            print(f"[Analysis Bot] Filtered out: {comp_name} (size > override max {override_max})")
                            analysis_final = "idle"
                            skip_analysis = True
                        elif override_min is not None and emp < override_min:
                            state.set_event("filtered_undersize", f"Filtered out {comp_name}: {emp} employees < override min {override_min}", "info")
                            print(f"[Analysis Bot] Filtered out: {comp_name} (size < override min {override_min})")
                            analysis_final = "idle"
                            skip_analysis = True

                        if not skip_analysis:
                            state.set_event("analyzing", f"Analyzing candidate: {comp_name}. {len(state.candidate_queue)} more in queue.", "info")
                            print(f"[Analysis Bot] Processing: {comp_name}")
                            row = process_single_company(
                                org,
                                niche,
                                strategy,
                                openai_client=client,
                                apollo_api_key=apollo_key,
                                firecrawl_api_key=firecrawl_key,
                                user_agent=user_agent,
                            )
                            if row:
                                # PE-backed check via portfolio cache (cache-first, news fallback)
                                from lib.filters import check_pe_backed
                                pe_check = check_pe_backed(client, row.get("Company", ""))
                                if pe_check.get("is_pe_backed"):
                                    evidence = pe_check.get("evidence", "PE-backed")
                                    print(f"[Analysis Bot] Filtered out: {comp_name} ({evidence})")
                                else:
                                    # Portfolio conflict check
                                    state.batch_update(bot_status={"analysis": "checking_conflict"})
                                    conflict = check_portfolio_conflict(
                                        client,
                                        row.get("Company", ""),
                                        row.get("Description", ""),
                                    )
                                    diff = row.get("Differentiated", "Low")
                                    if not conflict.get("conflicts") and diff in ("High", "Medium"):
                                        state.add_qualified(row)
                                        state.set_event("qualified", f"Qualified candidate: {comp_name} (Diff={diff}). {len(state.completed_memos)+len(state.qualified_queue)+1}/{target_count} memos so far.", "success")
                                        print(f"[Analysis Bot] Qualified: {comp_name} (Diff={diff})")
                                    else:
                                        reason = "portfolio conflict" if conflict.get("conflicts") else f"Diff={diff}"
                                        print(f"[Analysis Bot] Filtered out: {comp_name} ({reason})")
                            else:
                                print(f"[Analysis Bot] Filtered out: {comp_name} (did not pass filters)")

                    analysis_final = "idle"

            except Exception as e:
                print(f"[Analysis Bot] Error: {e}")
                analysis_final = "error"

            # =======================================================================
            # WRITE-UP BOT
            # =======================================================================
            try:
                state.reload_from_disk()
                if not state.qualified_queue:
                    writeup_final = "idle"
                else:
                    state.batch_update(bot_status={"writeup": "generating_memo"})

                    row = state.pop_qualified()
                    if row:
                        comp_name = row.get("Company", "Unknown")
                        state.set_event("writing_memo", f"Generating investment memo for {comp_name}.", "info")
                        print(f"[Write-up Bot] Generating memo: {comp_name}")
                        memo_text = _generate_memo(client, row, niche)
                        memo = {
                            "company": comp_name,
                            "row": row,
                            "memo": memo_text,
                        }
                        state.add_memo(memo)
                        state.set_event("memo_complete", f"Memo complete: {comp_name}. {len(state.completed_memos)}/{target_count} done.", "success")
                        print(f"[Write-up Bot] Memo complete: {comp_name} ({len(state.completed_memos)}/{target_count})")

                    writeup_final = "idle"

            except Exception as e:
                print(f"[Write-up Bot] Error: {e}")
                writeup_final = "error"

            # =======================================================================
            # END-OF-ITERATION — reconcile all bot statuses in one write
            # =======================================================================
            current_bot_status = dict(state.bot_status)
            final_update = {}
            if search_final is not None:
                final_update["search"] = _resolve_final_status("search", search_final, current_bot_status)
            if analysis_final is not None:
                final_update["analysis"] = _resolve_final_status("analysis", analysis_final, current_bot_status)
            if writeup_final is not None:
                final_update["writeup"] = _resolve_final_status("writeup", writeup_final, current_bot_status)
            if final_update:
                state.batch_update(bot_status=final_update)

        except Exception as e:
            state.set_event("error", f"Pipeline error: {e}. Attempting to continue.", "error")
            print(f"[Orchestrator] Iteration error: {e}")
            time.sleep(2)
            continue

        time.sleep(1)
