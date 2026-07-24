"""Shared fuzzy-matching helper for the importers.

Corporate names carry legal-suffix noise ("LLC", "L.P.", "Inc") that
drags similarity scores down, so we normalize before scoring:
lowercase, strip punctuation, drop legal suffixes. Scoring uses
token_sort_ratio — strict enough that supersets don't fake a 100%.
"""

import re

from rapidfuzz import fuzz, process, utils

_SUFFIXES = {
    "llc", "l.l.c", "lp", "l.p", "llp", "l.l.p", "inc", "ltd", "co",
    "corp", "corporation", "company", "gp", "sarl", "sa",
}


def norm_name(name):
    s = utils.default_process(str(name or ""))     # lowercase, strip punct
    tokens = [t for t in s.split() if t not in _SUFFIXES]
    return " ".join(tokens)


def best_match(name, choices):
    """Match a normalized name against a {display_name: value} dict.

    Returns (display_name, score) or (None, 0).
    """
    if not choices:
        return None, 0
    normed = {norm_name(c): c for c in choices}
    hit = process.extractOne(norm_name(name), list(normed),
                             scorer=fuzz.token_sort_ratio)
    if not hit:
        return None, 0
    return normed[hit[0]], hit[1]
