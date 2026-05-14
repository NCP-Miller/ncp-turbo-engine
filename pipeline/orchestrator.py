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
from lib.ai_params import suggest_search_params, refine_search_params
from lib.apollo_search import search_organizations, web_discovery_pass
from lib.contacts import firecrawl_scrape, clean_domain
from lib.filters import is_buyable_structure, is_obvious_mismatch, quick_niche_prefilter
from lib.worker import process_single_company
from lib.enrichment import score_conviction, _load_thesis
from lib.feedback import load_feedback
from lib.ncp_portfolio import check_portfolio_conflict
from lib.constants import OPENAI_MODEL
from pipeline.state import PipelineState

CONVICTION_THRESHOLD = 6
ANALYSIS_WORKERS = 3   # candidates processed in parallel by the analysis bot


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
def _generate_memo(client, row, niche, thesis=None):
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
    conviction = row.get("Conviction", "")
    conviction_pitch = row.get("Conviction Pitch", "")

    if thesis is None:
        thesis = _load_thesis()

    ebitda_caveat = (
        f"Estimated {ebitda} EBITDA (heuristic based on employee count; requires confirmation)"
        if ebitda
        else "EBITDA not available; primary diligence item"
    )

    excitement_signals = thesis.get("excitement_signals", [])
    excitement_text = chr(10).join(f"- {s}" for s in excitement_signals) if excitement_signals else ""

    prompt = f"""Write a concise 1-page investment memo for {thesis.get('firm', 'a lower middle market PE firm')}
evaluating the following company as a potential acquisition target in the "{niche}" space.

Company: {company}
Location: {city}, {state_abbr}
Employees: {employees}
Est. EBITDA: {ebitda_caveat}
Description: {description}
Conviction Score: {conviction}/10
Analyst's Initial Pitch: {conviction_pitch}
Differentiation: {differentiated}
Priority: {priority}
Growth: {growth}

What excites NCP about a deal:
{excitement_text}

Conviction bar: {thesis.get('conviction_bar', '')}

CRITICAL — Honesty rules:
- Est. EBITDA is a heuristic. Always caveat it.
- Do NOT invent ownership, growth rates, financials, or customer data.
- If you lack a fact, write "Not available; primary diligence item."
- Bias toward honest gaps over polished invention.

Structure the memo with exactly these sections:

1. **Why We're Excited** — 2-3 sentences. Lead with the SPECIFIC reason this company
   stands out. What is the "right to win"? Why should Trey want to take this call?
   Use the analyst pitch as a starting point but make it sharper and more specific.
2. **Company Overview** — What they do, where they operate, approximate scale.
3. **Differentiated Value Proposition** — What moat or advantage they hold.
4. **Market Opportunity & Growth** — TAM/SAM, secular tailwinds, growth levers.
5. **NCP Fit Rationale** — Why this fits the lower middle market, services-oriented,
   founder-owned thesis specifically.
6. **Key Risks & Diligence Items** — Top 3-5 risks or open questions.

Write in a professional PE memo tone. Factual and concise."""

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
# INTERNAL — PARALLEL ANALYSIS HELPERS
# ---------------------------------------------------------------------------
def _analyze_single(org, niche, strategy, config, search_params,
                    client, apollo_key, firecrawl_key, user_agent, thesis):
    """Analyze a single candidate end-to-end (pre-filters + deep + scoring).

    Returns a result dict the orchestrator uses to update state safely
    from the main thread. Keys:
      - "outcome": one of "pre_filtered_size", "pre_filtered_structural",
        "pre_filtered_blocklist", "pre_filtered_niche", "deep_analysis_failed",
        "pe_backed", "portfolio_conflict", "qualified", "near_miss"
      - "company": company name
      - "row": enriched row dict (when applicable)
      - "reason": human-readable reason string
      - "score": conviction score (when applicable)
    """
    comp_name = org.get("name", "Unknown")

    # 1. Per-run size overrides
    override_max = config.get("override_size_max")
    override_min = config.get("override_size_min")
    emp = org.get("estimated_num_employees", 0) or 0
    if override_max is not None and emp > override_max:
        return {"outcome": "pre_filtered_size", "company": comp_name,
                "reason": f"size {emp} > max {override_max}"}
    if override_min is not None and emp < override_min:
        return {"outcome": "pre_filtered_size", "company": comp_name,
                "reason": f"size {emp} < min {override_min}"}

    # 2. Structural filter
    buyable, reason = is_buyable_structure(org, strategy)
    if not buyable:
        return {"outcome": "pre_filtered_structural", "company": comp_name,
                "reason": reason}

    # 3. Name/description blocklist
    mismatch, reason = is_obvious_mismatch(org, niche, strategy)
    if mismatch:
        return {"outcome": "pre_filtered_blocklist", "company": comp_name,
                "reason": reason}

    # 4. Niche relevance pre-filter
    _niche_kw_list = []
    _niche_ind_list = []
    if search_params:
        _niche_kw_list = [
            k.strip()
            for k in (search_params.get("keywords") or "").split(",")
            if k.strip()
        ]
        _niche_ind_list = search_params.get("industries") or []
    niche_pass, reason = quick_niche_prefilter(
        org, niche, _niche_kw_list, _niche_ind_list,
    )
    if not niche_pass:
        return {"outcome": "pre_filtered_niche", "company": comp_name,
                "reason": reason}

    # 5. Deep analysis
    row = process_single_company(
        org, niche, strategy,
        openai_client=client,
        apollo_api_key=apollo_key,
        firecrawl_api_key=firecrawl_key,
        user_agent=user_agent,
    )
    if not row:
        return {"outcome": "deep_analysis_failed", "company": comp_name,
                "reason": "did not pass filters"}

    # 6. PE-backed check
    from lib.filters import check_pe_backed
    pe_check = check_pe_backed(client, row.get("Company", ""))
    if pe_check.get("is_pe_backed"):
        return {"outcome": "pe_backed", "company": comp_name,
                "reason": pe_check.get("evidence", "PE-backed"), "row": row}

    # 7. Portfolio conflict check
    conflict = check_portfolio_conflict(
        client, row.get("Company", ""), row.get("Description", ""),
    )
    if conflict.get("conflicts"):
        return {"outcome": "portfolio_conflict", "company": comp_name,
                "reason": "portfolio conflict", "row": row}

    # 8. Conviction scoring — the final gate
    feedback_history = load_feedback()
    conv_score, conv_pitch, conv_reason = score_conviction(
        client, comp_name, row.get("Description", ""),
        niche, row, thesis=thesis, feedback_history=feedback_history,
    )
    row["Conviction"] = conv_score
    row["Conviction Pitch"] = conv_pitch
    row["Conviction Reasoning"] = conv_reason

    if conv_score >= CONVICTION_THRESHOLD:
        return {"outcome": "qualified", "company": comp_name,
                "row": row, "score": conv_score, "pitch": conv_pitch}
    return {"outcome": "near_miss", "company": comp_name,
            "row": row, "score": conv_score, "reason": conv_reason}


def _process_candidate_batch(batch, niche, strategy, config, search_params,
                             client, apollo_key, firecrawl_key, user_agent,
                             thesis, state):
    """Run _analyze_single on a batch of candidates in parallel.

    All state updates happen in the main thread after results come back
    to avoid SQLite write contention.
    """
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=ANALYSIS_WORKERS) as ex:
        futures = [
            ex.submit(
                _analyze_single, org, niche, strategy, config, search_params,
                client, apollo_key, firecrawl_key, user_agent, thesis,
            )
            for org in batch
        ]
        for fut in concurrent.futures.as_completed(futures):
            try:
                results.append(fut.result())
            except Exception as e:
                print(f"[Analysis Bot] Worker error: {e}")

    # Apply results to state sequentially (single-threaded writes)
    for r in results:
        outcome = r.get("outcome")
        comp_name = r.get("company", "Unknown")
        state.increment_filter_stat("total_sourced")

        if outcome == "pre_filtered_size":
            state.increment_filter_stat("pre_filtered_size")
            print(f"[Analysis Bot] Pre-filtered: {comp_name} ({r['reason']})")
        elif outcome == "pre_filtered_structural":
            state.increment_filter_stat("pre_filtered_structural")
            print(f"[Analysis Bot] Pre-filtered: {comp_name} ({r['reason']})")
        elif outcome == "pre_filtered_blocklist":
            state.increment_filter_stat("pre_filtered_blocklist")
            print(f"[Analysis Bot] Pre-filtered: {comp_name} ({r['reason']})")
        elif outcome == "pre_filtered_niche":
            state.increment_filter_stat("pre_filtered_niche")
            print(f"[Analysis Bot] Pre-filtered: {comp_name} ({r['reason']})")
        elif outcome == "deep_analysis_failed":
            state.increment_filter_stat("deep_analysis_failed")
            print(f"[Analysis Bot] Filtered out: {comp_name} (deep analysis failed)")
        elif outcome == "pe_backed":
            state.increment_filter_stat("pe_backed")
            print(f"[Analysis Bot] Filtered out: {comp_name} ({r['reason']})")
        elif outcome == "portfolio_conflict":
            state.increment_filter_stat("portfolio_conflict")
            print(f"[Analysis Bot] Filtered out: {comp_name} (portfolio conflict)")
        elif outcome == "qualified":
            state.add_qualified(r["row"])
            state.increment_filter_stat("qualified")
            state.set_event(
                "qualified",
                f"Excited about {comp_name} (conviction {r['score']}/10): {r['pitch'][:120]}",
                "success",
            )
            print(f"[Analysis Bot] Qualified: {comp_name} (conviction={r['score']}/10)")
        elif outcome == "near_miss":
            state.increment_filter_stat("low_differentiation")
            state.add_near_miss(
                r["row"],
                f"Conviction {r['score']}/10: {r['reason']}",
            )
            print(f"[Analysis Bot] Below conviction bar: {comp_name} ({r['score']}/10)")


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

    _thesis = _load_thesis()

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

    # Per-round metrics for adaptive refinement
    _round_start_total_sourced = 0
    _round_start_qualified = 0
    _round_start_memo_count = 0

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

            from lib.constants import NCP_PRIORITY_LABEL, NCP_PRIORITY_APOLLO_LOCATIONS
            if geography == NCP_PRIORITY_LABEL:
                apollo_locations = NCP_PRIORITY_APOLLO_LOCATIONS
                web_geography = "Eastern United States"
            else:
                apollo_locations = geography
                web_geography = geography

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
                    if len(state.candidate_queue) > 200:
                        print(f"[Search Bot] Queue capped at {len(state.candidate_queue)}. Waiting for analysis to drain.")
                    search_final = "idle"
                else:
                    # First iteration: ask AI for Apollo search params
                    if search_params is None:
                        search_params = suggest_search_params(client, niche)
                        # Snapshot baseline metrics for round 1 evaluation
                        _round_start_total_sourced = state.filter_stats.get("total_sourced", 0)
                        _round_start_qualified = state.filter_stats.get("qualified", 0)
                        _round_start_memo_count = len(state.completed_memos or [])

                    industries = search_params.get("industries", [])
                    keyword_tags = [
                        t.strip()
                        for t in (search_params.get("keywords") or "").split(",")
                        if t.strip()
                    ]

                    # Determine max_pages based on round; keyword tags always active
                    if _search_round == 1:
                        round_max_pages = 3
                    elif _search_round == 2:
                        round_max_pages = 6
                    else:
                        round_max_pages = 10
                    round_keyword_tags = keyword_tags

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
                            location_input=apollo_locations,
                            keyword_tags=round_keyword_tags,
                            max_pages=round_max_pages,
                        )

                        # Accumulate new orgs, capping to prevent queue bloat
                        max_new = max(0, 200 - len(state.candidate_queue))
                        new_orgs = []
                        new_domains = []
                        new_names = []
                        for org in orgs:
                            if len(new_orgs) >= max_new:
                                break
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
                            print(f"[Search Bot] Added {len(new_orgs)} candidates (queue: {len(state.candidate_queue)+len(new_orgs)})")

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
                            web_geography,
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
                            # --- ADAPTIVE REFINEMENT: refine params before next round ---
                            state.reload_from_disk()
                            current_total = state.filter_stats.get("total_sourced", 0)
                            current_qualified = state.filter_stats.get("qualified", 0)
                            sourced_this_round = current_total - _round_start_total_sourced
                            qualified_this_round = current_qualified - _round_start_qualified

                            # Top descriptions from this round's qualified memos
                            top_descs = []
                            recent_memos = (state.completed_memos or [])[_round_start_memo_count:]
                            for m in recent_memos[-5:]:
                                d = (m.get("row") or {}).get("Description") or ""
                                if d:
                                    top_descs.append(d)

                            # Common pre-filter reasons (heuristic)
                            fs = state.filter_stats
                            common_reasons = []
                            if fs.get("pre_filtered_niche", 0) > 5:
                                common_reasons.append(f"{fs['pre_filtered_niche']} candidates filtered for not matching niche signal")
                            if fs.get("pre_filtered_structural", 0) > 5:
                                common_reasons.append(f"{fs['pre_filtered_structural']} filtered as government/nonprofit/public")
                            if fs.get("pe_backed", 0) > 3:
                                common_reasons.append(f"{fs['pe_backed']} filtered as PE-backed")
                            if fs.get("low_differentiation", 0) > 3:
                                common_reasons.append(f"{fs['low_differentiation']} scored too low on conviction")

                            try:
                                refined = refine_search_params(
                                    client, niche, search_params,
                                    {
                                        "prev_round": _search_round,
                                        "candidates_found": sourced_this_round,
                                        "candidates_qualified": qualified_this_round,
                                        "top_qualified_descriptions": top_descs,
                                        "common_filter_reasons": common_reasons,
                                    },
                                )
                                if refined:
                                    search_params = {
                                        "industries": refined["industries"],
                                        "keywords": refined["keywords"],
                                    }
                                    state.set_event(
                                        "refined",
                                        f"Round {_search_round + 1} refined: {refined.get('rationale', '')}",
                                        "info",
                                    )
                                    print(
                                        f"[Search Bot] Refined for round {_search_round + 1}: "
                                        f"industries={refined['industries']}, keywords='{refined['keywords']}'"
                                    )
                            except Exception as e:
                                print(f"[Search Bot] Refinement step failed: {e}")

                            # Snapshot metrics for next round
                            _round_start_total_sourced = current_total
                            _round_start_qualified = current_qualified
                            _round_start_memo_count = len(state.completed_memos or [])

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

                                # --- Closest-fit memo: if 0 memos, surface the best near-miss ---
                                try:
                                    near_misses = state.near_misses
                                except (AttributeError, KeyError):
                                    near_misses = []

                                if len(state.completed_memos) == 0 and near_misses:
                                    best = max(near_misses, key=lambda m: m.get("row", {}).get("Conviction", 0))
                                    best_row = best.get("row", {})
                                    best_reason = best.get("reason", "")
                                    best_name = best_row.get("Company", "Unknown")
                                    print(f"[Search Bot] Generating closest-fit memo for: {best_name}")
                                    state.set_event("writing_closest_fit", f"No companies met the conviction bar. Generating closest-fit memo for {best_name}.", "warning")
                                    memo_text = _generate_memo(client, best_row, niche)
                                    memo = {
                                        "company": best_name,
                                        "row": best_row,
                                        "memo": memo_text,
                                        "closest_fit": True,
                                        "closest_fit_reason": best_reason,
                                    }
                                    state.add_memo(memo)
                                    state.set_event(
                                        "exhausted",
                                        f"Search exhausted. No companies met conviction bar. Closest fit: {best_name} ({best_reason}). Review in Investment Memos tab.",
                                        "warning",
                                    )
                                else:
                                    state.set_event("exhausted", f"Search exhausted after 4 rounds. Found {len(state.completed_memos)}/{target_count} memos. Try broadening niche or geography, or lowering target count.", "warning")

                                state.batch_update(
                                    bot_status={"search": "exhausted"},
                                    status="stopped",
                                )
                                print(
                                    f"[Search Bot] Exhausted all 4 search rounds. "
                                    f"{len(state.completed_memos)}/{target_count} memos completed."
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
            # ANALYSIS BOT — parallel processing
            # =======================================================================
            try:
                state.reload_from_disk()
                if not state.candidate_queue:
                    analysis_final = "idle"
                else:
                    state.batch_update(bot_status={"analysis": "filtering"})

                    # Pop a batch of candidates for parallel processing
                    batch = []
                    for _ in range(ANALYSIS_WORKERS):
                        org = state.pop_candidate()
                        if org is None:
                            break
                        batch.append(org)

                    if batch:
                        state.set_event(
                            "analyzing",
                            f"Analyzing {len(batch)} candidates in parallel. {len(state.candidate_queue)} more in queue.",
                            "info",
                        )
                        _process_candidate_batch(
                            batch, niche, strategy, config, search_params,
                            client, apollo_key, firecrawl_key, user_agent,
                            _thesis, state,
                        )

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
                        memo_text = _generate_memo(client, row, niche, thesis=_thesis)
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

            # =======================================================================
            # STALL DETECTION — stop if nothing left to do but target isn't met
            # =======================================================================
            if (search_exhausted
                    and not state.candidate_queue
                    and not state.qualified_queue
                    and len(state.completed_memos) < target_count):
                state.set_event(
                    "exhausted",
                    f"Search exhausted — found {len(state.completed_memos)}/{target_count} memos. "
                    f"Try broadening niche or geography, or ask for more in a different region.",
                    "warning",
                )
                state.batch_update(
                    bot_status={"search": "exhausted"},
                    status="stopped",
                )
                print(
                    f"[Orchestrator] Stall detected: search exhausted, queues empty, "
                    f"{len(state.completed_memos)}/{target_count}. Stopping."
                )
                break

        except Exception as e:
            state.set_event("error", f"Pipeline error: {e}. Attempting to continue.", "error")
            print(f"[Orchestrator] Iteration error: {e}")
            time.sleep(2)
            continue

        time.sleep(1)
