"""GitHub-based persistent backup for pipeline projects and feedback.

Stores project state as JSON files on an orphan 'data' branch in the
same GitHub repo. Survives Streamlit Community Cloud hibernation/restarts.

Requires GITHUB_TOKEN and GITHUB_REPO in Streamlit secrets or environment.
"""

import base64
import json
import os
import sqlite3
import time

import requests

_BRANCH = "data"
_API = "https://api.github.com"


def _get_credentials():
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
    return {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def is_configured():
    token, repo = _get_credentials()
    return bool(token and repo)


def _ensure_branch(token, repo):
    url = f"{_API}/repos/{repo}/git/refs/heads/{_BRANCH}"
    r = requests.get(url, headers=_headers(token), timeout=10)
    if r.status_code == 200:
        return True
    commit_url = f"{_API}/repos/{repo}/git/commits"
    commit_resp = requests.post(
        commit_url,
        headers=_headers(token),
        json={
            "message": "Initialize data branch",
            "tree": "4b825dc642cb6eb9a060e54bf8d69288fbee4904",
            "parents": [],
        },
        timeout=10,
    )
    if commit_resp.status_code not in (200, 201):
        return False
    commit_sha = commit_resp.json()["sha"]
    ref_url = f"{_API}/repos/{repo}/git/refs"
    ref_resp = requests.post(
        ref_url,
        headers=_headers(token),
        json={"ref": f"refs/heads/{_BRANCH}", "sha": commit_sha},
        timeout=10,
    )
    return ref_resp.status_code in (200, 201)


def _read_file(token, repo, path, ref=None):
    url = f"{_API}/repos/{repo}/contents/{path}"
    r = requests.get(url, headers=_headers(token), params={"ref": ref or _BRANCH}, timeout=15)
    if r.status_code != 200:
        return None, None
    data = r.json()
    content_b64 = data.get("content") or ""
    if content_b64.strip():
        content = base64.b64decode(content_b64).decode("utf-8")
        return content, data.get("sha")
    # Files >1MB have empty content — use download_url or raw fallback
    download_url = data.get("download_url")
    if download_url:
        try:
            dl = requests.get(download_url, headers=_headers(token), timeout=30)
            if dl.status_code == 200:
                return dl.text, data.get("sha")
        except Exception:
            pass
    # Raw URL fallback for historical refs
    if ref:
        raw_url = f"https://raw.githubusercontent.com/{repo}/{ref}/{path}"
        try:
            dl = requests.get(raw_url, headers=_headers(token), timeout=30)
            if dl.status_code == 200:
                return dl.text, data.get("sha")
        except Exception:
            pass
    return None, None


def _write_file(token, repo, path, content, message="Update backup"):
    url = f"{_API}/repos/{repo}/contents/{path}"
    encoded = base64.b64encode(content.encode("utf-8")).decode("ascii")
    _, existing_sha = _read_file(token, repo, path)
    payload = {
        "message": message,
        "content": encoded,
        "branch": _BRANCH,
    }
    if existing_sha:
        payload["sha"] = existing_sha
    r = requests.put(url, headers=_headers(token), json=payload, timeout=60)
    return r.status_code in (200, 201)


def _recover_project_from_history(token, repo, slug):
    """Find the best historical backup version and return it.

    Scans recent commits for the version with the most reviewed_near_misses
    (not just the first with any memos). Returns the state_dict directly.
    """
    path = f"projects/{slug}.json"
    commits_url = f"{_API}/repos/{repo}/commits"
    params = {"sha": _BRANCH, "path": path, "per_page": 30}
    try:
        r = requests.get(commits_url, headers=_headers(token), params=params, timeout=15)
        if r.status_code != 200:
            return None
        commits = r.json()
    except Exception:
        return None

    best_data = None
    best_reviewed = -1
    best_sha = None

    for commit in commits:
        sha = commit.get("sha")
        if not sha:
            continue
        content, _ = _read_file(token, repo, path, ref=sha)
        if not content:
            continue
        try:
            data = json.loads(content)
            memos = data.get("completed_memos", [])
            reviewed = data.get("reviewed_near_misses", [])
            if len(memos) > 0 and len(reviewed) > best_reviewed:
                best_data = data
                best_reviewed = len(reviewed)
                best_sha = sha
                if best_reviewed >= 70:
                    break
        except (json.JSONDecodeError, TypeError):
            continue

    if best_data and best_sha:
        print(f"[Backup] Best version for '{slug}': {len(best_data.get('completed_memos', []))} memos, "
              f"{best_reviewed} reviewed from commit {best_sha[:8]}")
    return best_data


# ---------------------------------------------------------------------------
# STATE EXPORT / IMPORT
# ---------------------------------------------------------------------------

def export_db_to_json(db_path):
    if not os.path.exists(db_path):
        return None
    conn = sqlite3.connect(db_path, timeout=10)
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    rows = conn.execute("SELECT key, value FROM pipeline_state").fetchall()
    conn.close()
    state = {}
    for key, value in rows:
        try:
            state[key] = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            state[key] = value
    return state


def import_json_to_db(db_path, state_dict):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS pipeline_state (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
    )
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute("DELETE FROM pipeline_state")
        for key, value in state_dict.items():
            conn.execute(
                "INSERT INTO pipeline_state (key, value) VALUES (?, ?)",
                (key, json.dumps(value)),
            )
        conn.execute("COMMIT")
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# PUBLIC API — backup / restore
# ---------------------------------------------------------------------------

def backup_project(slug, db_path):
    token, repo = _get_credentials()
    if not token or not repo:
        return False
    _ensure_branch(token, repo)
    state = export_db_to_json(db_path)
    if state is None:
        return False
    content = json.dumps(state, indent=2, default=str)
    return _write_file(
        token, repo, f"projects/{slug}.json", content,
        message=f"Backup project: {slug}",
    )


def backup_projects_index(projects_list):
    token, repo = _get_credentials()
    if not token or not repo:
        return False
    _ensure_branch(token, repo)
    content = json.dumps(projects_list, indent=2)
    return _write_file(token, repo, "projects.json", content, message="Update project index")


def backup_feedback(feedback_entries):
    token, repo = _get_credentials()
    if not token or not repo:
        return False
    _ensure_branch(token, repo)
    content = json.dumps(feedback_entries, indent=2)
    return _write_file(token, repo, "feedback_log.json", content, message="Update feedback log")


def backup_crm(crm_dict):
    """Push the full CRM export (deals + activities) to the data branch."""
    token, repo = _get_credentials()
    if not token or not repo:
        return False
    _ensure_branch(token, repo)
    content = json.dumps(crm_dict, indent=2, default=str)
    return _write_file(token, repo, "crm_data.json", content, message="Backup CRM")


def restore_crm():
    """Read the CRM backup from the data branch. Returns dict or None."""
    token, repo = _get_credentials()
    if not token or not repo:
        return None
    content, _ = _read_file(token, repo, "crm_data.json")
    if not content:
        return None
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        return None


def restore_all():
    """Pull all projects and feedback from GitHub into pipeline_data/.

    Call this on app startup if pipeline_data/ is empty.
    Returns number of projects restored, or -1 if not configured.
    """
    token, repo = _get_credentials()
    if not token or not repo:
        return -1

    from pipeline.state import STATE_DIR, DB_PATH

    os.makedirs(STATE_DIR, exist_ok=True)
    restored = 0

    index_content, _ = _read_file(token, repo, "projects.json")
    if not index_content:
        return 0

    try:
        projects = json.loads(index_content)
    except (json.JSONDecodeError, TypeError):
        return 0

    projects_file = os.path.join(STATE_DIR, "projects.json")
    with open(projects_file, "w", encoding="utf-8") as f:
        json.dump(projects, f, indent=2)

    active_slug = None
    for p in projects:
        slug = p.get("slug")
        if not slug:
            continue
        if p.get("active"):
            active_slug = slug

        project_content, _ = _read_file(token, repo, f"projects/{slug}.json")
        if not project_content:
            continue
        try:
            state_dict = json.loads(project_content)
        except (json.JSONDecodeError, TypeError):
            continue

        # If the current backup is empty, check git history for a version
        # that had actual data (memos). This handles cases where a bug or
        # empty-state save overwrote a good backup.
        memos_in_backup = len(state_dict.get("completed_memos", []))
        if memos_in_backup == 0:
            print(f"[Backup] '{slug}' backup has 0 memos — checking history for recoverable data...")
            recovered_data = _recover_project_from_history(token, repo, slug)
            if recovered_data:
                state_dict = recovered_data
                print(f"[Backup] '{slug}' recovered {len(state_dict.get('completed_memos', []))} memos, "
                      f"{len(state_dict.get('reviewed_near_misses', []))} reviewed from history.")

        recovered_count = len(state_dict.get("completed_memos", []))
        if recovered_count > 0 and p.get("memo_count", 0) == 0:
            p["memo_count"] = recovered_count

        db_dest = os.path.join(STATE_DIR, f"project_{slug}.db")
        import_json_to_db(db_dest, state_dict)
        restored += 1

    # Re-save the projects index with any corrected memo counts
    with open(projects_file, "w", encoding="utf-8") as f:
        json.dump(projects, f, indent=2)

    if active_slug:
        active_db = os.path.join(STATE_DIR, f"project_{active_slug}.db")
        if os.path.exists(active_db):
            import shutil
            conn = sqlite3.connect(active_db, timeout=10)
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            conn.close()
            shutil.copy2(active_db, DB_PATH)

    feedback_content, _ = _read_file(token, repo, "feedback_log.json")
    if feedback_content:
        feedback_path = os.path.join(STATE_DIR, "feedback_log.json")
        with open(feedback_path, "w", encoding="utf-8") as f:
            f.write(feedback_content)

    return restored
