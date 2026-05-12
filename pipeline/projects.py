"""Project manager — save, archive, and resume pipeline searches.

Each project is a named snapshot of the pipeline state DB. The active
pipeline always runs from ``pipeline_data/state.db``. When saving or
switching projects, the DB file is copied to/from a project-specific file.
"""

import json
import os
import re
import shutil
from datetime import datetime, timezone

from pipeline.state import STATE_DIR, DB_PATH, PipelineState

PROJECTS_FILE = os.path.join(STATE_DIR, "projects.json")


def _ensure_dir():
    os.makedirs(STATE_DIR, exist_ok=True)


def _load_index():
    _ensure_dir()
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
            "niche": cfg.get("niche", ""),
            "geography": cfg.get("geography", ""),
            "memo_count": len(ps.completed_memos or []),
            "status": ps.status,
        }
    except Exception:
        return {"niche": "", "geography": "", "memo_count": 0, "status": "unknown"}


def save_project(name):
    """Save the current pipeline state as a named project.

    If a project with this name already exists, it is overwritten.
    Returns the project dict.
    """
    _ensure_dir()
    slug = _slugify(name)
    db_dest = _db_path_for(slug)

    if os.path.exists(DB_PATH):
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
    }

    projects = [p for p in projects if p["slug"] != slug]
    for p in projects:
        p["active"] = False
    projects.append(entry)
    _save_index(projects)
    return entry


def load_project(name):
    """Load a previously saved project as the active pipeline.

    Copies the project's DB to state.db. The caller must stop the
    orchestrator thread BEFORE calling this.

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

    if os.path.exists(DB_PATH):
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


def update_active_meta():
    """Refresh the metadata (memo_count, niche, etc.) for the active project."""
    projects = _load_index()
    meta = _snapshot_meta()
    for p in projects:
        if p.get("active"):
            p["niche"] = meta["niche"]
            p["geography"] = meta["geography"]
            p["memo_count"] = meta["memo_count"]
            p["status"] = meta["status"]
            break
    _save_index(projects)
