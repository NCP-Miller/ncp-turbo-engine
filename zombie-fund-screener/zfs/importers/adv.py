"""Signal 2 importer — SEC Form ADV bulk data (RAUM, headcount, funds).

The SEC publishes the full adviser dataset monthly as CSV/ZIP under
"Information About Registered Investment Advisers" (Foia data sets):
https://www.sec.gov/foia/docs/form-adv-data
You download the monthly file yourself and drop it here — the importer
parses it in chunks (the files are large), keeps only the fields we
need, and stores one snapshot per GP per month so trends emerge.

Matching (spec rule — never auto-merge low confidence):
  - CRD number on the GP record -> exact join, always trusted.
  - Otherwise rapidfuzz name match: >= 93 auto-accepted, 75-92 goes to
    the confirmation queue for your yes/no, below 75 ignored.

If the ZIP also contains a Schedule D 7.B.1 file, fund service
providers (auditor/administrator/prime broker/custodian) are captured
per snapshot and diffed against the previous snapshot -> Signal 9.
"""

import io
import json
import zipfile

import pandas as pd
from zfs.importers.matching import best_match

from zfs.db import connect, now

_AUTO_ACCEPT = 93
_QUEUE_FLOOR = 75
_CHUNK = 20000


def _find_col(columns, *needles, exclude=()):
    """Find the first column whose lowercase name contains all needles."""
    for c in columns:
        lc = str(c).lower()
        if all(n in lc for n in needles) and not any(x in lc for x in exclude):
            return c
    return None


def _num(v):
    try:
        return float(str(v).replace(",", "").replace("$", "").strip())
    except (ValueError, TypeError):
        return None


def _iter_csv_frames(file_bytes, filename):
    """Yield (name, DataFrame-chunks-iterator) for each CSV in the upload."""
    if filename.lower().endswith(".zip"):
        zf = zipfile.ZipFile(io.BytesIO(file_bytes))
        for info in zf.infolist():
            if info.filename.lower().endswith(".csv"):
                yield info.filename, pd.read_csv(
                    zf.open(info), dtype=str, chunksize=_CHUNK,
                    encoding_errors="replace", on_bad_lines="skip",
                    low_memory=False)
    else:
        yield filename, pd.read_csv(
            io.BytesIO(file_bytes), dtype=str, chunksize=_CHUNK,
            encoding_errors="replace", on_bad_lines="skip",
            low_memory=False)


def import_adv(file_bytes, filename, snapshot_date, progress_cb=None):
    """Parse an ADV monthly file and snapshot every GP we can match.

    Returns a plain-English summary dict.
    """
    conn = connect()
    try:
        gps = [dict(r) for r in conn.execute(
            "SELECT id, name, crd_number FROM gps").fetchall()]
    finally:
        conn.close()
    by_crd = {str(g["crd_number"]).strip(): g for g in gps
              if g.get("crd_number")}
    names = {g["name"]: g for g in gps}

    # matched: gp_id -> (quality, snap). quality 999 = exact CRD join,
    # else the fuzzy score — a weaker match never overwrites a stronger one.
    matched, queued, provider_rows = {}, [], []
    main_seen = False

    def _keep(gp_id, quality, snap):
        prev = matched.get(gp_id)
        if prev is None or quality > prev[0]:
            matched[gp_id] = (quality, snap)

    for csv_name, chunks in _iter_csv_frames(file_bytes, filename):
        first = True
        cols = {}
        for chunk in chunks:
            if first:
                first = False
                c = chunk.columns
                cols = {
                    "crd": _find_col(c, "crd"),
                    "name": (_find_col(c, "primary", "business", "name")
                             or _find_col(c, "legal", "name")
                             or _find_col(c, "business", "name")),
                    "raum": (_find_col(c, "regulatory", "assets")
                             or _find_col(c, "5f", "2", "c")
                             or _find_col(c, "raum")),
                    "emp": (_find_col(c, "total", "employees")
                            or _find_col(c, "5a")
                            or _find_col(c, "employees", exclude=("non",))),
                    # Schedule D 7.B.1 provider columns (if this file is 7B1)
                    "fund_name": _find_col(c, "fund", "name"),
                    "auditor": _find_col(c, "auditor", "name")
                               or _find_col(c, "auditing", "firm"),
                    "admin": _find_col(c, "administrator", "name"),
                }
                is_main = bool(cols["crd"] and cols["name"] and cols["raum"])
                is_7b1 = bool(cols["fund_name"] and
                              (cols["auditor"] or cols["admin"]))
                if not is_main and not is_7b1:
                    break                      # not a file we understand
                if is_main:
                    main_seen = True

            for _, row in chunk.iterrows():
                if cols.get("crd") and cols.get("raum") and cols.get("name") \
                        and not (cols.get("fund_name") and
                                 (cols.get("auditor") or cols.get("admin"))):
                    crd = str(row.get(cols["crd"]) or "").strip()
                    nm = str(row.get(cols["name"]) or "").strip()
                    if not nm:
                        continue
                    snap = {
                        "crd": crd,
                        "name": nm,
                        "raum": _num(row.get(cols["raum"])),
                        "employees": (int(_num(row.get(cols["emp"])) or 0)
                                      if cols.get("emp") else None),
                    }
                    gp = by_crd.get(crd)
                    if gp:
                        _keep(gp["id"], 999, snap)
                        continue
                    # fuzzy fallback against GP names
                    bname, bscore = best_match(nm, names)
                    if bname and bscore >= _AUTO_ACCEPT:
                        _keep(names[bname]["id"], bscore, snap)
                    elif bname and bscore >= _QUEUE_FLOOR:
                        queued.append((names[bname]["id"], nm,
                                       bscore, snap))
                elif cols.get("fund_name"):
                    fn = str(row.get(cols["fund_name"]) or "").strip()
                    if not fn:
                        continue
                    for role, col in (("Auditor", cols.get("auditor")),
                                      ("Administrator", cols.get("admin"))):
                        if col:
                            prov = str(row.get(col) or "").strip()
                            if prov:
                                provider_rows.append((fn, role, prov))

    # ── write snapshots ──────────────────────────────────────────────
    conn = connect()
    saved = dupes = changes = 0
    try:
        for gp_id, (_, snap) in matched.items():
            dup = conn.execute(
                "SELECT 1 FROM adv_snapshots WHERE gp_id = ? AND "
                "snapshot_date = ?", (gp_id, snapshot_date)).fetchone()
            if dup:
                dupes += 1
                continue
            conn.execute(
                """INSERT INTO adv_snapshots (gp_id, crd_number,
                   snapshot_date, raum, employees, funds_json, fetched_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (gp_id, snap["crd"], snapshot_date, snap["raum"],
                 snap["employees"], "[]", now()))
            # remember the CRD so future imports join exactly
            conn.execute(
                "UPDATE gps SET crd_number = COALESCE(crd_number, ?) "
                "WHERE id = ?", (snap["crd"] or None, gp_id))
            saved += 1

        for gp_id, nm, score, snap in queued:
            conn.execute(
                """INSERT INTO match_queue (kind, payload, gp_id_candidate,
                   matched_name, score, created_at)
                   VALUES ('adv', ?, ?, ?, ?, ?)""",
                (json.dumps({**snap, "snapshot_date": snapshot_date}),
                 gp_id, nm, score, now()))

        # ── Signal 9: provider diffs vs the previous snapshot ────────
        if provider_rows:
            fund_names = {}
            for r in conn.execute(
                    "SELECT f.name, f.gp_id FROM funds f "
                    "JOIN gps g ON g.id = f.gp_id WHERE g.killed = 0"
            ).fetchall():
                fund_names[r["name"]] = r["gp_id"]
            for fn, role, prov in provider_rows:
                bfund, bscore = best_match(fn, fund_names)
                if not bfund or bscore < _AUTO_ACCEPT:
                    continue
                gp_id = fund_names[bfund]
                prev = conn.execute(
                    """SELECT provider FROM adv_fund_providers
                       WHERE gp_id = ? AND fund_name = ? AND role = ?
                       AND snapshot_date < ?
                       ORDER BY snapshot_date DESC LIMIT 1""",
                    (gp_id, bfund, role, snapshot_date)).fetchone()
                if prev and prev["provider"] and \
                        prev["provider"].lower() != prov.lower():
                    conn.execute(
                        """INSERT INTO provider_changes (gp_id, fund_name,
                           provider_role, old_provider, new_provider,
                           change_date, detected_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?)""",
                        (gp_id, bfund, role, prev["provider"], prov,
                         snapshot_date, now()))
                    changes += 1
                dup = conn.execute(
                    "SELECT 1 FROM adv_fund_providers WHERE gp_id = ? AND "
                    "fund_name = ? AND role = ? AND snapshot_date = ?",
                    (gp_id, bfund, role, snapshot_date)).fetchone()
                if not dup:
                    conn.execute(
                        """INSERT INTO adv_fund_providers (gp_id,
                           snapshot_date, fund_name, role, provider)
                           VALUES (?, ?, ?, ?, ?)""",
                        (gp_id, snapshot_date, bfund, role, prov))

        conn.execute(
            "INSERT INTO refresh_log (source, last_run, detail) VALUES "
            "('adv', ?, ?) ON CONFLICT(source) DO UPDATE SET "
            "last_run = excluded.last_run, detail = excluded.detail",
            (now(), f"{saved} snapshots, {len(queued)} queued, "
                    f"{changes} provider changes"))
        conn.commit()
    finally:
        conn.close()

    return {"saved": saved, "queued": len(queued), "dupes": dupes,
            "provider_changes": changes, "main_file_found": main_seen}


# ── Confirmation queue helpers (shared with pension) ─────────────────

def pending_matches(kind=None):
    conn = connect()
    try:
        q = ("SELECT m.*, g.name AS gp_name FROM match_queue m "
             "JOIN gps g ON g.id = m.gp_id_candidate "
             "WHERE m.status = 'pending'")
        params = []
        if kind:
            q += " AND m.kind = ?"
            params.append(kind)
        return [dict(r) for r in conn.execute(q, params).fetchall()]
    finally:
        conn.close()


def confirm_match(match_id):
    """User said yes: apply the queued data."""
    conn = connect()
    try:
        m = conn.execute("SELECT * FROM match_queue WHERE id = ?",
                         (match_id,)).fetchone()
        if not m:
            return
        if m["kind"] == "adv":
            snap = json.loads(m["payload"] or "{}")
            dup = conn.execute(
                "SELECT 1 FROM adv_snapshots WHERE gp_id = ? AND "
                "snapshot_date = ?",
                (m["gp_id_candidate"], snap.get("snapshot_date"))).fetchone()
            if not dup:
                conn.execute(
                    """INSERT INTO adv_snapshots (gp_id, crd_number,
                       snapshot_date, raum, employees, funds_json, fetched_at)
                       VALUES (?, ?, ?, ?, ?, '[]', ?)""",
                    (m["gp_id_candidate"], snap.get("crd"),
                     snap.get("snapshot_date"), snap.get("raum"),
                     snap.get("employees"), now()))
        elif m["kind"] == "pension" and m["row_ref"]:
            conn.execute(
                "UPDATE pension_rows SET gp_id = ?, confirmed = 1 "
                "WHERE id = ?", (m["gp_id_candidate"], m["row_ref"]))
        conn.execute("UPDATE match_queue SET status = 'confirmed' "
                     "WHERE id = ?", (match_id,))
        conn.commit()
    finally:
        conn.close()


def reject_match(match_id):
    conn = connect()
    try:
        conn.execute("UPDATE match_queue SET status = 'rejected' "
                     "WHERE id = ?", (match_id,))
        conn.commit()
    finally:
        conn.close()
