"""Persistent feedback log — survives across pipeline runs.

Stores Trey's reactions to individual companies so the conviction scorer
and search strategy can learn from past decisions.
"""

import json
import os
from datetime import datetime, timezone

_FEEDBACK_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "pipeline_data", "feedback_log.json"
)


def _ensure_dir():
    os.makedirs(os.path.dirname(_FEEDBACK_PATH), exist_ok=True)


def load_feedback():
    try:
        with open(_FEEDBACK_PATH) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def save_feedback(company_name, feedback_text, niche=None, verdict=None):
    """Append a feedback entry.

    Args:
        company_name: Which company the feedback is about.
        feedback_text: Trey's raw feedback.
        niche: The niche being searched (for context).
        verdict: "liked", "rejected", or "caveats" (optional; inferred by caller).
    """
    _ensure_dir()
    entries = load_feedback()
    entries.append({
        "company": company_name,
        "feedback": feedback_text,
        "niche": niche,
        "verdict": verdict,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })
    with open(_FEEDBACK_PATH, "w") as f:
        json.dump(entries, f, indent=2)
