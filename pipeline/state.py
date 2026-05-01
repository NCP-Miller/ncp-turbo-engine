import json
import os
import sqlite3
import threading
from datetime import datetime, timezone


STATE_DIR = "pipeline_data"
DB_PATH = os.path.join(STATE_DIR, "state.db")
MAX_SEEN_ENTRIES = 5000


def _default_state():
    return {
        "config": {"niche": None, "geography": None, "strategy": None, "target_count": None},
        "status": "idle",
        "bot_status": {"search": "idle", "analysis": "idle", "writeup": "idle"},
        "candidate_queue": [],
        "qualified_queue": [],
        "completed_memos": [],
        "chat_history": [],
        "user_feedback": [],
        "seen_domains": [],
        "seen_names": [],
        "last_event": {
            "type": "idle",
            "message": "Pipeline ready. Configure and click Start.",
            "timestamp": None,
            "severity": "info",
        },
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def _migrate_from_json_if_present(conn):
    """One-time migration: load legacy state.json into SQLite, then rename it."""
    legacy_path = os.path.join(STATE_DIR, "state.json")
    if not os.path.exists(legacy_path):
        return
    try:
        with open(legacy_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        print(f"[PipelineState] Could not read legacy state.json for migration ({e}). Skipping.")
        return

    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute("DELETE FROM pipeline_state")
        for key, value in data.items():
            conn.execute(
                "INSERT OR REPLACE INTO pipeline_state (key, value) VALUES (?, ?)",
                (key, json.dumps(value)),
            )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    backup_path = legacy_path + ".legacy.bak"
    try:
        os.replace(legacy_path, backup_path)
        print(f"[PipelineState] Migrated legacy state.json -> state.db (backup at {backup_path})")
    except OSError as e:
        print(f"[PipelineState] Migration succeeded but could not rename state.json: {e}")


class PipelineState:
    def __init__(self):
        self._lock = threading.Lock()
        os.makedirs(STATE_DIR, exist_ok=True)
        self._conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30.0, isolation_level=None)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA busy_timeout=30000")
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS pipeline_state (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        _migrate_from_json_if_present(self._conn)
        # Populate defaults if table is empty
        row = self._conn.execute("SELECT COUNT(*) FROM pipeline_state").fetchone()
        if row[0] == 0:
            defaults = _default_state()
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                for key, value in defaults.items():
                    self._conn.execute(
                        "INSERT INTO pipeline_state (key, value) VALUES (?, ?)",
                        (key, json.dumps(value)),
                    )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def _get(self, key):
        row = self._conn.execute(
            "SELECT value FROM pipeline_state WHERE key = ?", (key,)
        ).fetchone()
        if row is None:
            raise KeyError(key)
        return json.loads(row[0])

    def _set(self, key, value):
        self._conn.execute(
            "UPDATE pipeline_state SET value = ? WHERE key = ?",
            (json.dumps(value), key),
        )

    def _mutate_list(self, key, fn):
        """BEGIN IMMEDIATE, load list, apply fn, save, COMMIT. Returns fn's result."""
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            current = self._get(key)
            result = fn(current)
            self._set(key, current)
            self._conn.execute("COMMIT")
            return result
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    # ------------------------------------------------------------------
    # Public API — signatures unchanged
    # ------------------------------------------------------------------

    def set_event(self, event_type, message, severity="info"):
        """Set the most recent significant event for UI display.

        event_type: short slug like 'searching', 'analyzing', 'memo_complete', 'exhausted', 'error'
        message: user-facing English description
        severity: 'info' | 'success' | 'warning' | 'error'
        """
        from datetime import datetime, timezone
        event = {
            "type": event_type,
            "message": message,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "severity": severity,
        }
        with self._lock:
            self._set("last_event", event)

    def save(self):
        """Backward-compat no-op. SQLite writes are immediate."""
        pass

    def reload_from_disk(self):
        """Backward-compat no-op. SQLite always reads fresh."""
        pass

    def update(self, **kwargs):
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                for key, value in kwargs.items():
                    # Only update keys that exist in the DB
                    row = self._conn.execute(
                        "SELECT 1 FROM pipeline_state WHERE key = ?", (key,)
                    ).fetchone()
                    if row is not None:
                        self._set(key, value)
                self._conn.execute("COMMIT")
            except sqlite3.OperationalError:
                self._conn.execute("ROLLBACK")
                raise
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def batch_update(self, *, status=None, bot_status=None, config=None):
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                if status is not None:
                    self._set("status", status)
                if bot_status is not None:
                    current = self._get("bot_status")
                    current.update(bot_status)
                    self._set("bot_status", current)
                if config is not None:
                    current = self._get("config")
                    current.update(config)
                    self._set("config", current)
                self._conn.execute("COMMIT")
            except sqlite3.OperationalError:
                self._conn.execute("ROLLBACK")
                raise
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def add_candidate(self, org):
        with self._lock:
            self._mutate_list("candidate_queue", lambda lst: lst.append(org))

    def add_candidates_batch(self, orgs, seen_domains_to_add=None, seen_names_to_add=None):
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                queue = self._get("candidate_queue")
                queue.extend(orgs)
                self._set("candidate_queue", queue)

                if seen_domains_to_add:
                    domains = self._get("seen_domains")
                    existing = set(domains)
                    for d in seen_domains_to_add:
                        if d not in existing:
                            domains.append(d)
                            existing.add(d)
                    if len(domains) > MAX_SEEN_ENTRIES:
                        domains = domains[-MAX_SEEN_ENTRIES:]
                    self._set("seen_domains", domains)

                if seen_names_to_add:
                    names = self._get("seen_names")
                    existing = set(names)
                    for n in seen_names_to_add:
                        if n not in existing:
                            names.append(n)
                            existing.add(n)
                    if len(names) > MAX_SEEN_ENTRIES:
                        names = names[-MAX_SEEN_ENTRIES:]
                    self._set("seen_names", names)

                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def pop_candidate(self):
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                queue = self._get("candidate_queue")
                if queue:
                    item = queue.pop(0)
                    self._set("candidate_queue", queue)
                    self._conn.execute("COMMIT")
                    return item
                self._conn.execute("COMMIT")
                return None
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def add_qualified(self, row):
        with self._lock:
            self._mutate_list("qualified_queue", lambda lst: lst.append(row))

    def pop_qualified(self):
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                queue = self._get("qualified_queue")
                if queue:
                    item = queue.pop(0)
                    self._set("qualified_queue", queue)
                    self._conn.execute("COMMIT")
                    return item
                self._conn.execute("COMMIT")
                return None
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def add_memo(self, memo):
        with self._lock:
            self._mutate_list("completed_memos", lambda lst: lst.append(memo))

    def add_chat(self, role, content):
        with self._lock:
            self._mutate_list("chat_history", lambda lst: lst.append({"role": role, "content": content}))

    def add_feedback(self, text):
        with self._lock:
            self._mutate_list("user_feedback", lambda lst: lst.append(text))

    def add_seen_domain(self, domain):
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                domains = self._get("seen_domains")
                if domain not in domains:
                    domains.append(domain)
                    self._set("seen_domains", domains)
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def add_seen_name(self, name):
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                names = self._get("seen_names")
                if name not in names:
                    names.append(name)
                    self._set("seen_names", names)
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def reset(self):
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute("DELETE FROM pipeline_state")
                defaults = _default_state()
                for key, value in defaults.items():
                    self._conn.execute(
                        "INSERT INTO pipeline_state (key, value) VALUES (?, ?)",
                        (key, json.dumps(value)),
                    )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def apply_command(self, command):
        """Apply a structured command to pipeline state.

        command: {"action": str, "args": dict}
        Returns: {"success": bool, "message": str}
        """
        action = command.get("action", "").lower()
        args = command.get("args") or {}

        if action == "stop":
            self.update(status="stopped")
            return {"success": True, "message": "Pipeline stopped."}
        if action == "pause":
            self.update(status="paused")
            return {"success": True, "message": "Pipeline paused."}
        if action == "resume":
            self.update(status="running")
            return {"success": True, "message": "Pipeline resumed (will restart on next page interaction)."}
        if action == "change_geography":
            new_geo = args.get("new_geography")
            if not new_geo:
                return {"success": False, "message": "No new geography provided."}
            self.batch_update(config={"geography": new_geo})
            return {"success": True, "message": f"Geography changed to: {new_geo}. New candidates will use this. Existing queue preserved."}
        if action == "change_target_count":
            try:
                new_count = int(args.get("new_count"))
            except (TypeError, ValueError):
                return {"success": False, "message": "Invalid target count."}
            self.batch_update(config={"target_count": new_count})
            return {"success": True, "message": f"Target count changed to {new_count}."}
        if action == "broaden_search":
            with self._lock:
                self._conn.execute("BEGIN IMMEDIATE")
                try:
                    self._set("candidate_queue", [])
                    current_config = self._get("config")
                    current_config["broaden_signal"] = True
                    self._set("config", current_config)
                    self._conn.execute("COMMIT")
                except Exception:
                    self._conn.execute("ROLLBACK")
                    raise
            return {"success": True, "message": "Search broadened. Queue cleared; next round will use wider parameters."}
        if action == "narrow_search":
            new_keywords = args.get("new_keywords", "")
            if not new_keywords:
                return {"success": False, "message": "No new keywords provided."}
            with self._lock:
                self._conn.execute("BEGIN IMMEDIATE")
                try:
                    current_config = self._get("config")
                    current_config["additional_keywords"] = new_keywords
                    self._set("config", current_config)
                    self._conn.execute("COMMIT")
                except Exception:
                    self._conn.execute("ROLLBACK")
                    raise
            return {"success": True, "message": f"Search narrowed with keywords: {new_keywords}."}
        if action == "pivot":
            pivot = args
            summary = pivot.get("user_facing_summary") or "Pivoting search."
            applied = []
            with self._lock:
                self._conn.execute("BEGIN IMMEDIATE")
                try:
                    current_config = self._get("config")

                    if pivot.get("new_size_max") is not None:
                        try:
                            current_config["override_size_max"] = int(pivot["new_size_max"])
                            applied.append(f"max employees -> {pivot['new_size_max']}")
                        except (TypeError, ValueError):
                            pass
                    if pivot.get("new_size_min") is not None:
                        try:
                            current_config["override_size_min"] = int(pivot["new_size_min"])
                            applied.append(f"min employees -> {pivot['new_size_min']}")
                        except (TypeError, ValueError):
                            pass
                    if pivot.get("new_geography"):
                        current_config["geography"] = pivot["new_geography"]
                        applied.append(f"geography -> {pivot['new_geography']}")
                    if pivot.get("new_niche_addition"):
                        existing_niche = current_config.get("niche") or ""
                        current_config["niche"] = f"{existing_niche}. Additional constraint: {pivot['new_niche_addition']}"
                        applied.append("niche refined")
                    if pivot.get("additional_keywords"):
                        current_config["additional_keywords"] = pivot["additional_keywords"]
                        applied.append(f"keywords += {pivot['additional_keywords']}")

                    self._set("config", current_config)

                    if pivot.get("clear_queue"):
                        self._set("candidate_queue", [])
                        applied.append("queue cleared")

                    if pivot.get("exclude_companies"):
                        seen = self._get("seen_names")
                        seen_set = set(seen)
                        for name in pivot["exclude_companies"]:
                            n = (name or "").strip().lower()
                            if n and n not in seen_set:
                                seen.append(n)
                                seen_set.add(n)
                        self._set("seen_names", seen)
                        applied.append(f"excluded {len(pivot['exclude_companies'])} companies")

                    # Set pivot_signal so orchestrator can react
                    current_config = self._get("config")
                    current_config["pivot_signal"] = True
                    self._set("config", current_config)

                    self._conn.execute("COMMIT")
                except Exception:
                    self._conn.execute("ROLLBACK")
                    raise

            applied_str = "; ".join(applied) if applied else "no changes detected"
            return {"success": True, "message": f"Pivot applied. {applied_str}. {summary}"}

        return {"success": False, "message": f"Unknown command: {action}"}

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        try:
            return self._get(name)
        except KeyError:
            raise AttributeError(f"PipelineState has no attribute '{name}'")
