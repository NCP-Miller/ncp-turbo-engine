"""Cross-run sourcing history ledger.

Persists previously sourced companies to a CSV so future runs can skip them.
"""

import os
from datetime import datetime

import pandas as pd

from lib.contacts import clean_domain


# Default location: project-root/sourcing_history.csv
_DEFAULT_HISTORY_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "sourcing_history.csv",
)


def get_history_path(path=None):
    """Return the configured history-file path."""
    return path or _DEFAULT_HISTORY_FILE


def load_history(path=None):
    """Load the set of {company_name_lower, domain_lower} keys from the history CSV."""
    p = get_history_path(path)
    if not os.path.exists(p):
        return set()
    try:
        df = pd.read_csv(p)
        keys = set()
        for _, r in df.iterrows():
            name = str(r.get("company", "")).strip().lower()
            domain = str(r.get("domain", "")).strip().lower()
            if name:
                keys.add(name)
            if domain and domain != "nan":
                keys.add(domain)
        return keys
    except Exception:
        return set()


def save_history(rows, path=None):
    """Append a list of result rows to the history CSV."""
    p = get_history_path(path)
    new_rows = []
    for r in rows:
        domain = clean_domain(r.get("Website")) or ""
        new_rows.append({
            "company": (r.get("Company") or "").strip(),
            "domain":  domain,
            "date":    datetime.now().strftime("%Y-%m-%d"),
            "niche":   r.get("_niche", ""),
        })
    new_df = pd.DataFrame(new_rows)
    if os.path.exists(p):
        try:
            old = pd.read_csv(p)
            new_df = pd.concat([old, new_df], ignore_index=True)
        except Exception:
            pass
    new_df.to_csv(p, index=False)


def company_in_history(org, history_keys):
    """Return True if a company (by name or domain) is in the history set."""
    name = (org.get("name") or "").strip().lower()
    domain = clean_domain(org.get("website_url")) or ""
    return (name in history_keys) or (domain and domain.lower() in history_keys)


def clear_history(path=None):
    """Delete the history CSV if it exists."""
    p = get_history_path(path)
    if os.path.exists(p):
        os.remove(p)