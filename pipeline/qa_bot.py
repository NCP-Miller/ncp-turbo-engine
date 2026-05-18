"""Pipeline QA Bot — monitors funnel health and auto-corrects anomalies.

Runs periodically during the orchestrator loop. Analyzes filter stats,
detects patterns that indicate misconfigured searches, and triggers
corrective actions (param refinement, relevance relaxation, pivots).

Only fires each diagnosis ONCE to avoid spamming the chat.
"""

RELEVANCE_FAIL_CEILING = 0.90
PRE_FILTER_GOV_CEILING = 0.70
DEEP_ANALYSIS_MIN_SAMPLE = 30
QUALIFIED_ZERO_AFTER = 50

_fired_codes = set()


def reset():
    """Clear fired codes — call at the start of each pipeline run."""
    _fired_codes.clear()


def diagnose(filter_stats, completed_memo_count, target_count):
    """Analyze the current funnel and return NEW findings only.

    Each finding fires at most once per pipeline run.
    """
    findings = []

    total = filter_stats.get("total_sourced", 0)
    if total < DEEP_ANALYSIS_MIN_SAMPLE:
        return findings

    pre_structural = filter_stats.get("pre_filtered_structural", 0)
    pre_blocklist = filter_stats.get("pre_filtered_blocklist", 0)
    pre_niche = filter_stats.get("pre_filtered_niche", 0)
    pre_size = filter_stats.get("pre_filtered_size", 0)
    deep_failed = filter_stats.get("deep_analysis_failed", 0)
    pe_backed = filter_stats.get("pe_backed", 0)
    qualified = filter_stats.get("qualified", 0)

    total_pre_filtered = pre_structural + pre_blocklist + pre_niche + pre_size
    sent_to_deep = total - total_pre_filtered

    if total > 0 and pre_structural / total > PRE_FILTER_GOV_CEILING:
        if "broad_search_params" not in _fired_codes:
            _fired_codes.add("broad_search_params")
            findings.append({
                "severity": "warning",
                "code": "broad_search_params",
                "message": (
                    f"{pre_structural}/{total} candidates ({pre_structural*100//total}%) "
                    f"filtered as gov/nonprofit/public. Refining Apollo search parameters."
                ),
                "action": "refine_params",
            })

    if sent_to_deep >= DEEP_ANALYSIS_MIN_SAMPLE and deep_failed > 0:
        fail_rate = deep_failed / sent_to_deep
        if fail_rate > RELEVANCE_FAIL_CEILING:
            if "total_relevance_failure" not in _fired_codes:
                _fired_codes.add("total_relevance_failure")
                findings.append({
                    "severity": "critical",
                    "code": "total_relevance_failure",
                    "message": (
                        f"{deep_failed}/{sent_to_deep} candidates ({int(fail_rate*100)}%) "
                        f"failed AI relevance. Forcing search parameter pivot."
                    ),
                    "action": "pivot_search",
                })
        elif fail_rate > 0.75:
            if "high_relevance_failure" not in _fired_codes:
                _fired_codes.add("high_relevance_failure")
                findings.append({
                    "severity": "warning",
                    "code": "high_relevance_failure",
                    "message": (
                        f"{int(fail_rate*100)}% of candidates failing AI relevance. "
                        f"Refining search parameters."
                    ),
                    "action": "refine_params",
                })

    if sent_to_deep >= QUALIFIED_ZERO_AFTER and qualified == 0:
        if deep_failed < sent_to_deep * 0.5:
            if "zero_qualified_low_conviction" not in _fired_codes:
                _fired_codes.add("zero_qualified_low_conviction")
                findings.append({
                    "severity": "warning",
                    "code": "zero_qualified_low_conviction",
                    "message": (
                        f"0 qualified after {sent_to_deep} candidates analyzed. "
                        f"Companies pass relevance but fail conviction. Consider broadening niche."
                    ),
                    "action": "broaden_niche",
                })

    if sent_to_deep >= DEEP_ANALYSIS_MIN_SAMPLE and pe_backed > 0:
        pe_rate = pe_backed / sent_to_deep
        if pe_rate > 0.30:
            if "high_pe_rate" not in _fired_codes:
                _fired_codes.add("high_pe_rate")
                findings.append({
                    "severity": "info",
                    "code": "high_pe_rate",
                    "message": f"{int(pe_rate*100)}% of candidates are PE-backed. Niche may be heavily consolidated.",
                    "action": None,
                })

    return findings


def recommend_action(findings):
    """Return the highest-priority action from a list of findings, or None."""
    priority = {"pivot_search": 3, "refine_params": 2, "broaden_niche": 1}
    best_action = None
    best_priority = -1
    best_finding = None

    for f in findings:
        action = f.get("action")
        if action and priority.get(action, 0) > best_priority:
            best_action = action
            best_priority = priority[action]
            best_finding = f

    return best_action, best_finding
