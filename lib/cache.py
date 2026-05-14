"""Simple disk cache for expensive API calls (Firecrawl, Apollo, OpenAI, news).

Uses SQLite in pipeline_data/cache.db. Thread-safe. TTL-based expiry.
"""

import hashlib
import json
import os
import sqlite3
import threading
import time

_DB_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "pipeline_data", "cache.db"
)
_lock = threading.Lock()
_LOCAL = threading.local()

DEFAULT_TTL = 7 * 24 * 3600  # 7 days


def _get_conn():
    conn = getattr(_LOCAL, "conn", None)
    if conn is None:
        os.makedirs(os.path.dirname(_DB_PATH), exist_ok=True)
        conn = sqlite3.connect(_DB_PATH, timeout=10)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(
            "CREATE TABLE IF NOT EXISTS cache "
            "(key TEXT PRIMARY KEY, value TEXT, expires_at REAL)"
        )
        conn.commit()
        _LOCAL.conn = conn
    return conn


def _make_key(namespace, *args):
    raw = json.dumps([namespace] + list(args), sort_keys=True, default=str)
    return hashlib.sha256(raw.encode()).hexdigest()


def get(namespace, *args):
    conn = _get_conn()
    key = _make_key(namespace, *args)
    row = conn.execute(
        "SELECT value, expires_at FROM cache WHERE key = ?", (key,)
    ).fetchone()
    if row is None:
        return None
    if row[1] < time.time():
        conn.execute("DELETE FROM cache WHERE key = ?", (key,))
        conn.commit()
        return None
    try:
        return json.loads(row[0])
    except (json.JSONDecodeError, TypeError):
        return None


def put(namespace, *args, value, ttl=DEFAULT_TTL):
    conn = _get_conn()
    key = _make_key(namespace, *args)
    expires_at = time.time() + ttl
    conn.execute(
        "INSERT OR REPLACE INTO cache (key, value, expires_at) VALUES (?, ?, ?)",
        (key, json.dumps(value, default=str), expires_at),
    )
    conn.commit()


def clear_expired():
    conn = _get_conn()
    conn.execute("DELETE FROM cache WHERE expires_at < ?", (time.time(),))
    conn.commit()


def clear_all():
    conn = _get_conn()
    conn.execute("DELETE FROM cache")
    conn.commit()
