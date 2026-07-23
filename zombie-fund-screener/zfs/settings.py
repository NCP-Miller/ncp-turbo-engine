"""Signal settings, saved presets, and cadence configuration.

All settings live as JSON in the settings table so they persist and can
be backed up. Every signal has: enabled (on/off), weight (0-10), and its
own thresholds. Changing any of these re-scores instantly from stored
data — nothing is re-fetched.
"""

from zfs.db import get_setting, set_setting

# The ten signals with their default weights and thresholds (from the spec)
DEFAULT_SIGNALS = {
    "s1":  {"name": "Form D vintage, no successor fund", "enabled": True,
            "weight": 8, "thresholds": {"age_years": 9}},
    "s2":  {"name": "Form ADV decline pattern", "enabled": True, "weight": 8,
            "thresholds": {"raum_pct": 30, "raum_years": 3,
                           "emp_pct": 40, "fund_age": 10,
                           "use_raum": True, "use_emp": True, "use_age": True}},
    "s3":  {"name": "Stale GP website", "enabled": True, "weight": 5,
            "thresholds": {"stale_years": 2}},
    "s4":  {"name": "Portfolio companies held too long", "enabled": True,
            "weight": 7, "thresholds": {"hold_years": 8}},
    "s5":  {"name": "GP team decay (LinkedIn checklist)", "enabled": True,
            "weight": 4, "thresholds": {"decline_pct": 50, "hire_years": 3}},
    "s6":  {"name": "Pension fund performance", "enabled": True, "weight": 10,
            "thresholds": {"vintage_max": 2016, "dpi_max": 0.7,
                           "nav_floor_m": 5.0}},
    "s7":  {"name": "No exits in N years", "enabled": True, "weight": 6,
            "thresholds": {"exit_years": 3}},
    "s8":  {"name": "Portfolio company decay", "enabled": True, "weight": 4,
            "thresholds": {"stale_years": 2, "logic": "AND"}},
    "s9":  {"name": "Provider changes / fund extensions", "enabled": True,
            "weight": 5, "thresholds": {"window_years": 3}},
    "s10": {"name": "UCC lien activity", "enabled": True, "weight": 4,
            "thresholds": {"window_years": 3, "amendment_max": 3}},
}

DEFAULT_CADENCE = {
    # business days until the auto follow-up task after each unanswered touch
    "intervals": [3, 7, 14],
    "max_touches": 4,
    "stale_days": 21,          # "stale relationship" threshold for Today view
    "min_signals": 1,          # signals required to enter the candidate pool
}


def get_signal_settings():
    stored = get_setting("signals", None)
    if not stored:
        return {k: dict(v) for k, v in DEFAULT_SIGNALS.items()}
    # merge stored over defaults so new signals/fields appear automatically
    merged = {}
    for k, v in DEFAULT_SIGNALS.items():
        s = dict(v)
        if k in stored:
            s.update({kk: vv for kk, vv in stored[k].items() if kk != "name"})
            s["thresholds"] = {**v["thresholds"],
                               **(stored[k].get("thresholds") or {})}
        merged[k] = s
    return merged


def save_signal_settings(settings):
    set_setting("signals", settings)


def get_cadence():
    stored = get_setting("cadence", None)
    return {**DEFAULT_CADENCE, **(stored or {})}


def save_cadence(cad):
    set_setting("cadence", cad)


# ── Saved presets ("Wide net", "High conviction", ...) ────────────────

def list_presets():
    return get_setting("presets", {})


def save_preset(name, settings):
    presets = list_presets()
    presets[name] = settings
    set_setting("presets", presets)


def delete_preset(name):
    presets = list_presets()
    presets.pop(name, None)
    set_setting("presets", presets)
