"""Signal 6 importer — public pension PE performance files.

Works for CalPERS' published PEP performance file and any similar
CSV/Excel from other pensions (Washington SIB, Texas TRS, Oregon PERF,
Florida SBA): you download the file, drop it in, map the columns once
(the importer guesses them for you), and confirm any fuzzy matches.

This is the highest-quality signal — a confirmed row gives the GP a
"verified" badge on the dashboard.
"""

import io

import pandas as pd
from zfs.importers.matching import best_match

from zfs.db import connect, now

_AUTO_ACCEPT = 90
_QUEUE_FLOOR = 60


def read_file(file_bytes, filename):
    """Load a CSV or Excel file into a DataFrame of strings."""
    if filename.lower().endswith((".xlsx", ".xls")):
        return pd.read_excel(io.BytesIO(file_bytes), dtype=str)
    return pd.read_csv(io.BytesIO(file_bytes), dtype=str,
                       encoding_errors="replace", on_bad_lines="skip")


def guess_columns(df):
    """Best-guess the column mapping; the UI lets the user override."""
    def find(*needles, exclude=()):
        for c in df.columns:
            lc = str(c).lower()
            if all(n in lc for n in needles) and \
                    not any(x in lc for x in exclude):
                return c
        return None
    return {
        "fund_name": find("fund", exclude=("vintage",)) or find("name"),
        "vintage": find("vintage") or find("year"),
        "committed": find("commit") or find("capital committed"),
        "nav": find("nav") or find("remaining") or find("market value"),
        "dpi": find("dpi") or find("distributed"),
        "irr": find("irr") or find("net irr"),
    }


def _num(v):
    try:
        return float(str(v).replace(",", "").replace("$", "")
                     .replace("%", "").strip())
    except (ValueError, TypeError):
        return None


def import_pension(df, mapping, source_label):
    """Import mapped rows and fuzzy-match fund names to GPs/funds.

    >= 90 similarity auto-links (confirmed). 60-89 goes to the
    confirmation queue. Below 60 the row is stored unmatched so nothing
    is lost, but it won't score until you link it by hand.
    Returns a summary dict.
    """
    conn = connect()
    try:
        gps = [dict(r) for r in conn.execute(
            "SELECT id, name FROM gps").fetchall()]
        fund_rows = [dict(r) for r in conn.execute(
            "SELECT id, gp_id, name FROM funds").fetchall()]
    finally:
        conn.close()
    # Match against BOTH firm names and fund names (fund names in pension
    # reports usually contain the firm name, e.g. "Alpha Capital Fund III")
    lookup = {g["name"]: ("gp", g["id"], None) for g in gps}
    for f in fund_rows:
        lookup[f["name"]] = ("fund", f["gp_id"], f["id"])

    auto = queued = unmatched = dupes = 0
    conn = connect()
    try:
        for _, row in df.iterrows():
            fund_name = str(row.get(mapping["fund_name"]) or "").strip()
            if not fund_name or fund_name.lower() == "nan":
                continue
            vintage = _num(row.get(mapping["vintage"])) if mapping.get("vintage") else None
            rec = {
                "fund_name": fund_name,
                "vintage_year": int(vintage) if vintage else None,
                "committed": _num(row.get(mapping["committed"])) if mapping.get("committed") else None,
                "nav": _num(row.get(mapping["nav"])) if mapping.get("nav") else None,
                "dpi": _num(row.get(mapping["dpi"])) if mapping.get("dpi") else None,
                "irr": _num(row.get(mapping["irr"])) if mapping.get("irr") else None,
            }
            dup = conn.execute(
                "SELECT 1 FROM pension_rows WHERE fund_name = ? AND "
                "source = ?", (fund_name, source_label)).fetchone()
            if dup:
                dupes += 1
                continue

            bname, bscore = best_match(fund_name, lookup)
            gp_id = fund_id = None
            confirmed = 0
            if bname and bscore >= _AUTO_ACCEPT:
                _, gp_id, fund_id = lookup[bname]
                confirmed = 1
                auto += 1
            cur = conn.execute(
                """INSERT INTO pension_rows (gp_id, fund_id, source,
                   fund_name, vintage_year, committed, nav, dpi, irr,
                   confirmed, imported_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (gp_id, fund_id, source_label, rec["fund_name"],
                 rec["vintage_year"], rec["committed"], rec["nav"],
                 rec["dpi"], rec["irr"], confirmed, now()))
            if bname and _QUEUE_FLOOR <= bscore < _AUTO_ACCEPT:
                _, cand_gp, _ = lookup[bname]
                conn.execute(
                    """INSERT INTO match_queue (kind, row_ref,
                       gp_id_candidate, matched_name, score, created_at)
                       VALUES ('pension', ?, ?, ?, ?, ?)""",
                    (cur.lastrowid, cand_gp, fund_name, bscore, now()))
                queued += 1
            elif not confirmed:
                unmatched += 1

        conn.execute(
            "INSERT INTO refresh_log (source, last_run, detail) VALUES "
            "('pension', ?, ?) ON CONFLICT(source) DO UPDATE SET "
            "last_run = excluded.last_run, detail = excluded.detail",
            (now(), f"{source_label}: {auto} linked, {queued} queued, "
                    f"{unmatched} unmatched"))
        conn.commit()
    finally:
        conn.close()
    return {"auto": auto, "queued": queued, "unmatched": unmatched,
            "dupes": dupes}
