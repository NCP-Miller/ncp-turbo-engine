"""Pipeline QA Bot — monitors funnel health and auto-corrects anomalies.

Runs periodically during the orchestrator loop. Analyzes filter stats,
detects patterns that indicate misconfigured searches, and triggers
corrective actions (param refinement, relevance relaxation, pivots).
"""

# Thresholds — when a ratio exceeds these, the QA bot intervenes
RELEVANCE_FAIL_CEILING = 0.90     # >90% failing AI relevance = search mismatch
PRE_FILTER_GOV_CEILING = 0.70     # >70% gov/nonprofit = search params too broad
DEEP_ANALYSIS_MIN_SAMPLE = 30     # wait for at least N candidates before diagnosing
QUALIFIED_ZERO_AFTER = 50         # 0 qualified after N deep-analyzed = problem
NEAR_MISS_RATIO_FLOOR = 0.05     # if <5% even reach near-miss, filter is broken


def diagnose(filter_stats, completed_memo_count, target_count):
    """Analyze the current funnel and return a list of findings.

    Each finding is a dict:
        severity: "critical" | "warning" | "info"
        code: short machine-readable tag
        message: human-readable explanation
        action: recommended corrective action or None
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
    portfolio_conflict = filter_stats.get("portfolio_conflict", 0)
    low_diff = filter_stats.get("low_differentiation", 0)
    qualified = filter_stats.get("qualified", 0)

    total_pre_filtered = pre_structural + pre_blocklist + pre_niche + pre_size
    sent_to_deep = total - total_pre_filtered

    # --- Check 1: Massive gov/nonprofit pre-filter rate ---
    if total > 0 and pre_structural / total > PRE_FILTER_GOV_CEILING:
        findings.append({
            "severity": "warning",
            "code": "broad_search_params",
            "message": (
                f"{pre_structural}/{total} candidates ({pre_structural*100//total}%) "
                f"filtered as gov/nonprofit/public. Apollo search parameters are "
                f"returning the wrong types of companies."
            ),
            "action": "refine_params",
        })

    # --- Check 2: 100% AI relevance failure ---
    if sent_to_deep >= DEEP_ANALYSIS_MIN_SAMPLE and deep_failed > 0:
        fail_rate = deep_failed / sent_to_deep
        if fail_rate > RELEVANCE_FAIL_CEILING:
            findings.append({
                "severity": "critical",
                "code": "total_relevance_failure",
                "message": (
                    f"{deep_failed}/{sent_to_deep} candidates ({int(fail_rate*100)}%) "
                    f"failed AI relevance. The niche description may not match what "
                    f"Apollo is returning. Search parameters need a major pivot."
                ),
                "action": "pivot_search",
            })
        elif fail_rate > 0.75:
            findings.append({
                "severity": "warning",
                "code": "high_relevance_failure",
                "message": (
                    f"{int(fail_rate*100)}% of candidates failing AI relevance. "
                    f"Search is finding mostly wrong companies."
                ),
                "action": "refine_params",
            })

    # --- Check 3: Zero qualified after significant analysis ---
    if sent_to_deep >= QUALIFIED_ZERO_AFTER and qualified == 0:
        if deep_failed < sent_to_deep * 0.5:
            findings.append({
                "severity": "warning",
                "code": "zero_qualified_low_conviction",
                "message": (
                    f"0 qualified after {sent_to_deep} candidates analyzed. "
                    f"Companies are passing relevance but failing conviction scoring. "
                    f"The niche may need to be broader or the conviction threshold lowered."
                ),
                "action": "broaden_niche",
            })

    # --- Check 4: High PE-backed rate ---
    if sent_to_deep >= DEEP_ANALYSIS_MIN_SAMPLE and pe_backed > 0:
        pe_rate = pe_backed / sent_to_deep
        if pe_rate > 0.30:
            findings.append({
                "severity": "info",
                "code": "high_pe_rate",
                "message": (
                    f"{int(pe_rate*100)}% of analyzed candidates are PE-backed. "
                    f"This niche may be heavily consolidated."
                ),
                "action": None,
            })

    # --- Check 5: Progress check ---
    if completed_memo_count > 0 and completed_memo_count >= target_count:
        findings.append({
            "severity": "info",
            "code": "target_reached",
            "message": f"Target of {target_count} memos reached.",
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
