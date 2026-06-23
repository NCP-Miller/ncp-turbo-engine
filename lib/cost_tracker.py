"""Cost estimation and real-time tracking for pipeline API spend.

Costs are estimates based on published pricing and average token counts
observed in production. Actual costs may vary by ~20%.

Pricing (as of 2025):
  - GPT-4o:       $2.50 / 1M input, $10.00 / 1M output
  - GPT-4o-mini:  $0.15 / 1M input, $0.60  / 1M output
  - Firecrawl:    ~$0.05 per scrape (varies by plan)
  - Apollo:       included in subscription (tracked by call count only)
"""

# ---------------------------------------------------------------------------
# Per-call cost estimates (USD)
# ---------------------------------------------------------------------------
# GPT-4o pricing: $2.50 / 1M input tokens, $10.00 / 1M output tokens
# GPT-4o-mini:    $0.15 / 1M input tokens, $0.60  / 1M output tokens

COST_OPENAI = {
    "classify_niche":        0.001,  # gpt-4o-mini, ~500 in / 100 out
    "suggest_search_params": 0.007,  # gpt-4o, ~2k in / ~200 out
    "refine_search_params":  0.009,  # gpt-4o, ~3k in / ~150 out
    "web_discovery_extract": 0.07,   # gpt-4o, ~25k in / ~500 out
    "relevance_check":       0.006,  # gpt-4o, ~1.5k in / ~200 out
    "description":           0.05,   # gpt-4o, ~20k in / ~150 out
    "differentiation":       0.006,  # gpt-4o, ~2k in / ~100 out
    "priority":              0.007,  # gpt-4o, ~2.5k in / ~100 out
    "growth_score":          0.02,   # gpt-4o, ~8k in / ~100 out
    "transaction_readiness": 0.02,   # gpt-4o, ~8k in / ~100 out
    "pe_vc_web_check":       0.04,   # gpt-4o, ~15k in / ~200 out
    "portfolio_conflict":    0.006,  # gpt-4o, ~2k in / ~100 out
    "conviction_scoring":    0.01,   # gpt-4o, ~3.5k in / ~150 out
    "contact_extraction":    0.04,   # gpt-4o, ~15k in / ~100 out (per page)
    "email_guess":           0.008,  # gpt-4o, ~2.5k in / ~150 out
    "memo_generation":       0.02,   # gpt-4o, ~3k in / ~1k out
    "pe_backed_check":       0.005,  # gpt-4o, ~1.5k in / ~100 out
}

COST_FIRECRAWL = 0.005  # per scrape (varies by plan; ~$0.001-$0.01)

COST_APOLLO = {
    "org_search":     0.00,  # included in subscription
    "people_search":  0.00,
    "enrichment":     0.00,
    "bulk_enrich":    0.00,
}

# ---------------------------------------------------------------------------
# Per-candidate cost model (for pre-search estimates)
# ---------------------------------------------------------------------------

# Candidates that pass pre-filters go through deep analysis.
# Average calls per candidate that reaches deep analysis:
_DEEP_ANALYSIS_COSTS = {
    "openai": (
        COST_OPENAI["relevance_check"]
        + COST_OPENAI["description"]
        + COST_OPENAI["differentiation"]
        + COST_OPENAI["priority"]
        + COST_OPENAI["growth_score"]
        + COST_OPENAI["transaction_readiness"]
        + COST_OPENAI["pe_vc_web_check"]
        + COST_OPENAI["portfolio_conflict"]
        + COST_OPENAI["conviction_scoring"]
        + COST_OPENAI["contact_extraction"] * 3  # avg 3 pages scraped
        + COST_OPENAI["email_guess"] * 0.3        # ~30% need email guess
    ),
    "firecrawl": COST_FIRECRAWL * 30,  # avg 30 scrapes per candidate (incl Google/LinkedIn)
}

COST_PER_DEEP_CANDIDATE = _DEEP_ANALYSIS_COSTS["openai"] + _DEEP_ANALYSIS_COSTS["firecrawl"]

# Per-round fixed costs
COST_PER_SEARCH_ROUND = (
    COST_OPENAI["suggest_search_params"]
    + COST_OPENAI["web_discovery_extract"]
    + COST_OPENAI["refine_search_params"]
)

COST_PER_MEMO = COST_OPENAI["memo_generation"]


def estimate_search_cost(target_count):
    """Estimate total cost for a pipeline run targeting `target_count` memos.

    Returns dict with low/mid/high estimates and breakdown.
    """
    rounds = 4

    # Heuristic: ~12-20 candidates analyzed per memo produced
    candidates_per_memo_low = 10
    candidates_per_memo_mid = 15
    candidates_per_memo_high = 25

    def _calc(candidates_per_memo):
        total_deep = target_count * candidates_per_memo
        search_cost = rounds * COST_PER_SEARCH_ROUND
        analysis_cost = total_deep * COST_PER_DEEP_CANDIDATE
        memo_cost = target_count * COST_PER_MEMO
        return {
            "search": round(search_cost, 2),
            "analysis": round(analysis_cost, 2),
            "memos": round(memo_cost, 2),
            "total": round(search_cost + analysis_cost + memo_cost, 2),
            "candidates_analyzed": total_deep,
        }

    low = _calc(candidates_per_memo_low)
    mid = _calc(candidates_per_memo_mid)
    high = _calc(candidates_per_memo_high)

    return {"low": low, "mid": mid, "high": high}


# ---------------------------------------------------------------------------
# Default state for a fresh run
# ---------------------------------------------------------------------------

def default_cost_state():
    return {
        "openai": 0.0,
        "firecrawl": 0.0,
        "apollo": 0.0,
        "total": 0.0,
        "call_counts": {
            "openai": 0,
            "firecrawl": 0,
            "apollo": 0,
        },
    }
