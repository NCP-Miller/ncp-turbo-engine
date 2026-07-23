"""GitHub data-branch backup for the Zombie Fund Screener.

Streamlit Cloud wipes the local disk on every redeploy, so the SQLite
file is ephemeral there. This module mirrors the whole database to a
JSON file (zombie/zombie_screener.json) on the same orphan 'data'
branch the other NCP apps use, and restores by MERGING — additive only,
so a wiped container can never clobber real work.

Merge rules (mirrors the hard lesson from the Deal Tracker):
  - Rows missing locally are inserted.
  - Kill decisions survive: if the backup says a GP is killed and the
    local copy isn't (and was never resurrected after that kill), the
    kill is adopted. Kills must outlive every redeploy.
  - Existing local rows keep their values; only NULL/empty fields fill.
  - Before overwriting the remote backup, the remote is merged in first
    whenever it holds more GPs than local.

Needs GITHUB_TOKEN and GITHUB_REPO in Streamlit secrets or env vars —
the same ones the sourcing app already uses.
"""

import base64
import json
import os

import requests

from zfs.db import connect, init_db, now, name_key

_BRANCH = "data"
_API = "https://api.github.com"
_PATH = "zombie/zombie_screener.json"

# Tables to mirror. gps first so foreign keys resolve on restore.
_TABLES = ["gps", "funds", "portfolio_companies", "contacts", "events",
           "tasks", "templates", "settings", "edgar_filings",
           "adv_snapshots", "wayback_checks", "pension_rows",
           "provider_changes", "refresh_log"]


def _credentials():
    token = os.environ.get("GITHUB_TOKEN")
    repo = os.environ.get("GITHUB_REPO")
    if not token or not repo:
        try:
            import streamlit as st
            token = token or st.secrets.get("GITHUB_TOKEN")
            repo = repo or st.secrets.get("GITHUB_REPO")
        except Exception:
            pass
    return token, repo


def _headers(token):
    return {"Authorization": f"token {token}",
            "Accept": "application/vnd.github+json"}


def is_configured():
    token, repo = _credentials()
    return bool(token and repo)


def _read_remote(token, repo):
    url = f"{_API}/repos/{repo}/contents/{_PATH}"
    r = requests.get(url, headers=_headers(token),
                     params={"ref": _BRANCH}, timeout=20)
    if r.status_code != 200:
        return None, None
    data = r.json()
    content = data.get("content") or ""
    if content.strip():
        try:
            return json.loads(base64.b64decode(content).decode()), data.get("sha")
        except Exception:
            return None, data.get("sha")
    dl = data.get("download_url")
    if dl:
        try:
            resp = requests.get(dl, headers=_headers(token), timeout=30)
            if resp.status_code == 200:
                return resp.json(), data.get("sha")
        except Exception:
            pass
    return None, data.get("sha")


def _write_remote(token, repo, payload):
    _, sha = _read_remote(token, repo)
    url = f"{_API}/repos/{repo}/contents/{_PATH}"
    body = {
        "message": "Backup zombie screener",
        "branch": _BRANCH,
        "content": base64.b64encode(
            json.dumps(payload, default=str).encode()).decode(),
    }
    if sha:
        body["sha"] = sha
    r = requests.put(url, headers=_headers(token), json=body, timeout=30)
    return r.status_code in (200, 201)


def export_all():
    init_db()
    conn = connect()
    try:
        dump = {}
        for t in _TABLES:
            dump[t] = [dict(r) for r in
                       conn.execute(f"SELECT * FROM {t}").fetchall()]
        return dump
    finally:
        conn.close()


def merge_export(data):
    """Additive merge of a backup dump into the local DB."""
    init_db()
    conn = connect()
    added = {"gps": 0, "other": 0}
    try:
        # 1. GPs — keyed by name_key; build old-id -> new-id map
        id_map = {}
        for g in data.get("gps", []):
            ck = g.get("name_key") or name_key(g.get("name", ""))
            if not ck:
                continue
            local = conn.execute(
                "SELECT * FROM gps WHERE name_key = ?", (ck,)).fetchone()
            if local is None:
                cols = [c for c in g.keys() if c != "id"]
                conn.execute(
                    f"INSERT INTO gps ({','.join(cols)}) "
                    f"VALUES ({','.join('?' * len(cols))})",
                    [g.get(c) for c in cols])
                new_id = conn.execute(
                    "SELECT id FROM gps WHERE name_key = ?", (ck,)
                ).fetchone()["id"]
                added["gps"] += 1
            else:
                new_id = local["id"]
                updates = {}
                # kills must survive wipes: adopt the backup's kill unless
                # the local copy was resurrected after that kill
                if g.get("killed") and not local["killed"]:
                    res = local["resurrected_at"]
                    if not res or res < (g.get("killed_at") or ""):
                        updates.update({
                            "killed": 1,
                            "kill_category": g.get("kill_category"),
                            "kill_reason": g.get("kill_reason"),
                            "killed_at": g.get("killed_at"),
                        })
                # statuses: adopt backup status only if local is still 'New'
                if (local["status"] or "New") == "New" and \
                        (g.get("status") or "New") != "New":
                    updates["status"] = g["status"]
                # fill empty manual fields
                for f in ("website", "linkedin_url", "crd_number", "city",
                          "state", "notes", "li_current_headcount",
                          "li_peak_headcount", "li_junior_hire_recent",
                          "li_notes", "last_exit_date", "first_surfaced_at",
                          "seen_at"):
                    if (local[f] in (None, "")) and g.get(f) not in (None, ""):
                        updates[f] = g[f]
                if updates:
                    sets = ", ".join(f"{k} = ?" for k in updates)
                    conn.execute(
                        f"UPDATE gps SET {sets}, updated_at = ? WHERE id = ?",
                        (*updates.values(), now(), new_id))
            id_map[g.get("id")] = new_id

        # 2. Child tables — dedup on natural keys, remap gp_id
        def _merge_children(table, rows, key_fields, extra_map=None):
            count = 0
            for r in rows:
                gid = id_map.get(r.get("gp_id"), r.get("gp_id"))
                if gid is None and table not in ("templates", "settings",
                                                 "refresh_log"):
                    continue
                keys, params = [], []
                for kf in key_fields:
                    val = gid if kf == "gp_id" else r.get(kf)
                    if val is None:
                        keys.append(f"{kf} IS NULL")
                    else:
                        keys.append(f"{kf} = ?")
                        params.append(val)
                exists = conn.execute(
                    f"SELECT 1 FROM {table} WHERE {' AND '.join(keys)}",
                    params).fetchone()
                if exists:
                    continue
                cols = [c for c in r.keys() if c != "id"]
                vals = []
                for c in cols:
                    if c == "gp_id":
                        vals.append(gid)
                    elif extra_map and c in extra_map:
                        vals.append(extra_map[c].get(r.get(c)))
                    else:
                        vals.append(r.get(c))
                conn.execute(
                    f"INSERT INTO {table} ({','.join(cols)}) "
                    f"VALUES ({','.join('?' * len(cols))})", vals)
                count += 1
            return count

        added["other"] += _merge_children(
            "funds", data.get("funds", []), ["gp_id", "name"])
        added["other"] += _merge_children(
            "portfolio_companies", data.get("portfolio_companies", []),
            ["gp_id", "name"])
        added["other"] += _merge_children(
            "contacts", data.get("contacts", []), ["gp_id", "name", "email"])
        added["other"] += _merge_children(
            "events", data.get("events", []), ["gp_id", "timestamp", "summary"])
        added["other"] += _merge_children(
            "tasks", data.get("tasks", []),
            ["gp_id", "description", "due_date"])
        added["other"] += _merge_children(
            "templates", data.get("templates", []), ["name"])
        added["other"] += _merge_children(
            "edgar_filings", data.get("edgar_filings", []),
            ["gp_id", "fund_name", "filing_date"])
        added["other"] += _merge_children(
            "adv_snapshots", data.get("adv_snapshots", []),
            ["gp_id", "snapshot_date"])
        added["other"] += _merge_children(
            "wayback_checks", data.get("wayback_checks", []),
            ["gp_id", "url", "checked_at"])
        added["other"] += _merge_children(
            "pension_rows", data.get("pension_rows", []),
            ["gp_id", "fund_name", "source"])
        added["other"] += _merge_children(
            "provider_changes", data.get("provider_changes", []),
            ["gp_id", "fund_name", "provider_role", "change_date"])

        # settings: fill only keys we don't have locally
        for s in data.get("settings", []):
            exists = conn.execute("SELECT 1 FROM settings WHERE key = ?",
                                  (s.get("key"),)).fetchone()
            if not exists:
                conn.execute("INSERT INTO settings (key, value) VALUES (?, ?)",
                             (s.get("key"), s.get("value")))
        conn.commit()
    finally:
        conn.close()
    return added


def backup():
    """Merge remote in (if it has more GPs), then push the full export."""
    token, repo = _credentials()
    if not token or not repo:
        return False
    try:
        remote, _ = _read_remote(token, repo)
        local = export_all()
        if remote and len(remote.get("gps", [])) > len(local.get("gps", [])):
            merge_export(remote)
            local = export_all()
        return _write_remote(token, repo, local)
    except Exception:
        return False


def restore_merge():
    """Merge the remote backup into local. Called once per session."""
    token, repo = _credentials()
    if not token or not repo:
        return None
    try:
        remote, _ = _read_remote(token, repo)
        if remote:
            return merge_export(remote)
    except Exception:
        pass
    return None
