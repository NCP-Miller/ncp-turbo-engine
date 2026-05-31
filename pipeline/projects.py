"""Project manager — save, archive, and resume pipeline searches.

Each project is a named snapshot of the pipeline state DB. The active
pipeline always runs from ``pipeline_data/state.db``. When saving or
switching projects, the DB file is copied to/from a project-specific file.

Projects are automatically backed up to a GitHub 'data' branch so they
survive Streamlit Community Cloud restarts/hibernation.
"""

import json
import os
import re
import shutil
import sqlite3
from datetime import datetime, timezone

from pipeline.state import STATE_DIR, DB_PATH, PipelineState
from lib.github_backup import (
    is_configured as _gh_configured,
    backup_project as _gh_backup_project,
    backup_projects_index as _gh_backup_index,
    backup_feedback as _gh_backup_feedback,
    restore_all as _gh_restore_all,
)

PROJECTS_FILE = os.path.join(STATE_DIR, "projects.json")

_restore_attempted = False


def _checkpoint_wal(db_path):
    """Force a WAL checkpoint so all data is flushed to the main DB file.

    Without this, shutil.copy2 misses recent writes that are still in the
    WAL, causing data loss when saving or loading projects.
    """
    if not os.path.exists(db_path):
        return
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.close()
    except Exception:
        pass


def _ensure_dir():
    os.makedirs(STATE_DIR, exist_ok=True)


def ensure_restored():
    """On first call, restore projects from GitHub if local data is missing."""
    global _restore_attempted
    if _restore_attempted:
        return
    _restore_attempted = True
    if os.path.exists(PROJECTS_FILE):
        return
    if not _gh_configured():
        return
    try:
        count = _gh_restore_all()
        if count > 0:
            print(f"[Projects] Restored {count} project(s) from GitHub backup.")
    except Exception as e:
        print(f"[Projects] GitHub restore failed: {e}")


def _load_index():
    _ensure_dir()
    ensure_restored()
    if not os.path.exists(PROJECTS_FILE):
        return []
    with open(PROJECTS_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def _save_index(projects):
    _ensure_dir()
    with open(PROJECTS_FILE, "w", encoding="utf-8") as f:
        json.dump(projects, f, indent=2)


def _slugify(name):
    slug = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
    return slug[:60] or "project"


def _db_path_for(slug):
    return os.path.join(STATE_DIR, f"project_{slug}.db")


def list_projects():
    """Return list of saved projects (dicts with name, slug, etc.)."""
    return _load_index()


def current_project_name():
    """Return the name of the active project, or None."""
    for p in _load_index():
        if p.get("active"):
            return p["name"]
    return None


def _snapshot_meta():
    """Read current state.db and return metadata."""
    try:
        ps = PipelineState()
        cfg = ps.config or {}
        return {
            "niche": cfg.get("niche") or "",
            "geography": cfg.get("geography") or "",
            "memo_count": len(ps.completed_memos or []),
            "status": ps.status,
        }
    except Exception:
        return {"niche": "", "geography": "", "memo_count": 0, "status": "unknown"}


def save_project(name):
    """Save the current pipeline state as a named project.

    If a project with this name already exists, it is overwritten.
    Returns the project dict with 'backup_status' indicating GitHub result.
    """
    _ensure_dir()
    slug = _slugify(name)
    db_dest = _db_path_for(slug)

    if os.path.exists(DB_PATH):
        _checkpoint_wal(DB_PATH)
        shutil.copy2(DB_PATH, db_dest)

    meta = _snapshot_meta()
    projects = _load_index()

    entry = {
        "name": name,
        "slug": slug,
        "db_file": os.path.basename(db_dest),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "niche": meta["niche"],
        "geography": meta["geography"],
        "memo_count": meta["memo_count"],
        "active": True,
        "backup_status": "not_configured",
    }

    projects = [p for p in projects if p["slug"] != slug]
    for p in projects:
        p["active"] = False
    projects.append(entry)
    _save_index(projects)

    if _gh_configured():
        try:
            remote_memos = _remote_memo_count(slug)
            local_memos = meta["memo_count"]
            if local_memos == 0 and remote_memos > 0:
                entry["backup_status"] = "skipped_protection"
                print(f"[Projects] BLOCKED save-backup for '{name}': local has 0 memos but remote has {remote_memos}. Refusing to overwrite.")
            else:
                ok1 = _gh_backup_project(slug, db_dest)
                ok2 = _gh_backup_index(projects)
                if ok1 and ok2:
                    from lib.github_backup import _get_credentials, _read_file
                    token, repo = _get_credentials()
                    content, _ = _read_file(token, repo, f"projects/{slug}.json")
                    if content and len(content) > 50:
                        import json as _vjson
                        _vdata = _vjson.loads(content)
                        _v_memos = _vdata.get("completed_memos", [])
                        entry["backup_status"] = "verified"
                        entry["backup_memo_count"] = len(_v_memos)
                        print(f"[Projects] Backup VERIFIED for '{name}': {len(_v_memos)} memos on GitHub.")
                    else:
                        entry["backup_status"] = "unverified"
                        print(f"[Projects] Backup uploaded but verification read-back was empty for '{name}'.")
                else:
                    entry["backup_status"] = "failed"
                    print(f"[Projects] GitHub backup API returned failure for '{name}'.")
        except Exception as e:
            entry["backup_status"] = "error"
            print(f"[Projects] GitHub backup failed for '{name}': {e}")

    return entry


def load_project(name):
    """Load a previously saved project as the active pipeline.

    Copies the project's DB to state.db. The caller must stop the
    orchestrator thread BEFORE calling this.  If the local DB is empty
    but GitHub history has data, automatically recovers.

    Returns the project dict, or None if not found.
    """
    projects = _load_index()
    target = None
    for p in projects:
        if p["name"] == name or p["slug"] == _slugify(name):
            target = p
            break
    if target is None:
        return None

    db_src = os.path.join(STATE_DIR, target["db_file"])
    if not os.path.exists(db_src):
        return None

    # Check if local DB is empty — if so, try recovering from GitHub history
    _checkpoint_wal(db_src)
    try:
        from lib.github_backup import export_db_to_json
        local_state = export_db_to_json(db_src)
        local_memos = len((local_state or {}).get("completed_memos", []))
        if local_memos == 0 and _gh_configured():
            from lib.github_backup import _recover_project_from_history, _get_credentials, import_json_to_db, _read_file
            token, repo = _get_credentials()
            if token and repo:
                print(f"[Projects] '{target['slug']}' has 0 memos locally — attempting recovery from GitHub history...")
                recovered = _recover_project_from_history(token, repo, target["slug"])
                if recovered:
                    content, _ = _read_file(token, repo, f"projects/{target['slug']}.json")
                    if content:
                        import json as _rj
                        state_dict = _rj.loads(content)
                        import_json_to_db(db_src, state_dict)
                        recovered_count = len(state_dict.get("completed_memos", []))
                        target["memo_count"] = recovered_count
                        print(f"[Projects] RECOVERED {recovered_count} memos for '{name}'.")
    except Exception as e:
        print(f"[Projects] Recovery check failed: {e}")

    _checkpoint_wal(db_src)
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    shutil.copy2(db_src, DB_PATH)

    for p in projects:
        p["active"] = p["slug"] == target["slug"]
    _save_index(projects)
    return target


def new_project(name):
    """Archive the current state and start a fresh pipeline.

    The caller must stop the orchestrator thread BEFORE calling this.
    Returns the new (empty) project dict.
    """
    _ensure_dir()

    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    ps = PipelineState()
    ps.reset()

    slug = _slugify(name)
    entry = {
        "name": name,
        "slug": slug,
        "db_file": f"project_{slug}.db",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "niche": "",
        "geography": "",
        "memo_count": 0,
        "active": True,
    }

    projects = _load_index()
    for p in projects:
        p["active"] = False
    projects = [p for p in projects if p["slug"] != slug]
    projects.append(entry)
    _save_index(projects)
    return entry


def delete_project(name):
    """Delete a saved project and its DB file."""
    projects = _load_index()
    target = None
    for p in projects:
        if p["name"] == name or p["slug"] == _slugify(name):
            target = p
            break
    if target is None:
        return False

    db_file = os.path.join(STATE_DIR, target["db_file"])
    if os.path.exists(db_file):
        os.remove(db_file)

    projects = [p for p in projects if p["slug"] != target["slug"]]
    _save_index(projects)
    return True


_last_backup_time = 0.0
_BACKUP_INTERVAL = 30  # seconds between GitHub backups


def _remote_memo_count(slug):
    """Check how many memos the GitHub backup currently has for this project."""
    try:
        from lib.github_backup import _get_credentials, _read_file
        token, repo = _get_credentials()
        if not token or not repo:
            return 0
        content, _ = _read_file(token, repo, f"projects/{slug}.json")
        if not content:
            return 0
        import json as _j
        data = _j.loads(content)
        return len(data.get("completed_memos", []))
    except Exception:
        return 0


def update_active_meta():
    """Refresh the metadata (memo_count, niche, etc.) for the active project.
    Periodically backs up the full DB to GitHub so data survives redeploys."""
    import time as _time
    global _last_backup_time
    projects = _load_index()
    meta = _snapshot_meta()
    active_slug = None
    for p in projects:
        if p.get("active"):
            p["niche"] = meta["niche"]
            p["geography"] = meta["geography"]
            p["memo_count"] = meta["memo_count"]
            p["status"] = meta["status"]
            active_slug = p.get("slug")
            break
    _save_index(projects)

    now = _time.time()
    if active_slug and _gh_configured() and (now - _last_backup_time) > _BACKUP_INTERVAL:
        _last_backup_time = now
        try:
            local_memos = meta["memo_count"]
            remote_memos = _remote_memo_count(active_slug)
            if local_memos == 0 and remote_memos > 0:
                print(f"[Projects] BLOCKED auto-backup: local has 0 memos but remote has {remote_memos}. Refusing to overwrite.")
                return

            db_file = _db_path_for(active_slug)
            if os.path.exists(DB_PATH):
                _checkpoint_wal(DB_PATH)
                shutil.copy2(DB_PATH, db_file)
            if os.path.exists(db_file):
                _gh_backup_project(active_slug, db_file)
                _gh_backup_index(projects)
        except Exception as e:
            print(f"[Projects] Auto-backup failed: {e}")
