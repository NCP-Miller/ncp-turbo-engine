"""GP lifecycle: create, status pipeline, kill/resurrect, NEW badges.

Everything here is user-owned state. Automated refreshes never call
these functions — they only write to evidence tables.
"""

from zfs.db import connect, init_db, now, name_key, PIPELINE_STATUSES


def add_gp(name, website=None, city=None, state=None, crd_number=None,
           linkedin_url=None, notes=""):
    """Add a GP by hand (or return the existing id if the name is known)."""
    init_db()
    conn = connect()
    try:
        existing = conn.execute(
            "SELECT id FROM gps WHERE name_key = ?", (name_key(name),)
        ).fetchone()
        if existing:
            return existing["id"], False
        cur = conn.execute(
            """INSERT INTO gps (name, name_key, website, city, state,
                                crd_number, linkedin_url, notes,
                                created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (name.strip(), name_key(name), website, city, state, crd_number,
             linkedin_url, notes, now(), now()),
        )
        conn.commit()
        return cur.lastrowid, True
    finally:
        conn.close()


def get_gp(gp_id):
    conn = connect()
    try:
        r = conn.execute("SELECT * FROM gps WHERE id = ?", (gp_id,)).fetchone()
        return dict(r) if r else None
    finally:
        conn.close()


def list_gps(include_killed=False):
    conn = connect()
    try:
        q = "SELECT * FROM gps"
        if not include_killed:
            q += " WHERE killed = 0"
        q += " ORDER BY name COLLATE NOCASE"
        return [dict(r) for r in conn.execute(q).fetchall()]
    finally:
        conn.close()


def update_gp(gp_id, **fields):
    """Update manual GP fields. Only whitelisted columns are writable."""
    allowed = {"name", "website", "linkedin_url", "crd_number", "city",
               "state", "notes", "li_current_headcount", "li_peak_headcount",
               "li_junior_hire_recent", "li_notes", "last_exit_date",
               "exit_last_checked", "first_surfaced_at", "seen_at"}
    fields = {k: v for k, v in fields.items() if k in allowed}
    if not fields:
        return
    conn = connect()
    try:
        sets = ", ".join(f"{k} = ?" for k in fields)
        conn.execute(f"UPDATE gps SET {sets}, updated_at = ? WHERE id = ?",
                     (*fields.values(), now(), gp_id))
        conn.commit()
    finally:
        conn.close()


def _log_event(conn, gp_id, kind, summary, user=None):
    conn.execute(
        "INSERT INTO events (gp_id, kind, summary, user, timestamp) "
        "VALUES (?, ?, ?, ?, ?)",
        (gp_id, kind, summary, user, now()),
    )


def set_status(gp_id, new_status, user=None):
    """Move a GP through the pipeline; logs the change to the timeline."""
    if new_status not in PIPELINE_STATUSES:
        return
    conn = connect()
    try:
        old = conn.execute("SELECT status FROM gps WHERE id = ?",
                           (gp_id,)).fetchone()
        old_status = old["status"] if old else "?"
        if old_status == new_status:
            return
        conn.execute("UPDATE gps SET status = ?, updated_at = ? WHERE id = ?",
                     (new_status, now(), gp_id))
        _log_event(conn, gp_id, "status",
                   f"Status: {old_status} → {new_status}", user)
        conn.commit()
    finally:
        conn.close()


def kill_gp(gp_id, category, reason_text="", user=None):
    """Kill a GP: permanently off the dashboard, into the Graveyard.

    Survives every refresh; can only come back via resurrect_gp.
    """
    conn = connect()
    try:
        conn.execute(
            """UPDATE gps SET killed = 1, kill_category = ?, kill_reason = ?,
               killed_at = ?, resurrected_at = NULL, updated_at = ?
               WHERE id = ?""",
            (category, reason_text, now(), now(), gp_id),
        )
        detail = f"Killed — {category}" + (f": {reason_text}" if reason_text else "")
        _log_event(conn, gp_id, "kill", detail, user)
        conn.commit()
    finally:
        conn.close()


def resurrect_gp(gp_id, user=None):
    conn = connect()
    try:
        conn.execute(
            """UPDATE gps SET killed = 0, resurrected_at = ?, updated_at = ?
               WHERE id = ?""",
            (now(), now(), gp_id),
        )
        _log_event(conn, gp_id, "resurrect", "Resurrected from Graveyard", user)
        conn.commit()
    finally:
        conn.close()


# ── NEW badge logic ──────────────────────────────────────────────────
# A GP is NEW when it has surfaced in the candidate pool but its detail
# page has never been opened. Killed GPs are never NEW.

def mark_surfaced(gp_ids):
    """Stamp first_surfaced_at for GPs newly appearing in the pool."""
    if not gp_ids:
        return
    conn = connect()
    try:
        for gid in gp_ids:
            conn.execute(
                "UPDATE gps SET first_surfaced_at = COALESCE(first_surfaced_at, ?) "
                "WHERE id = ? AND killed = 0",
                (now(), gid),
            )
        conn.commit()
    finally:
        conn.close()


def mark_seen(gp_id):
    """Opening the detail page clears the NEW badge."""
    conn = connect()
    try:
        conn.execute("UPDATE gps SET seen_at = COALESCE(seen_at, ?) WHERE id = ?",
                     (now(), gp_id))
        conn.commit()
    finally:
        conn.close()


def is_new(gp):
    return (not gp.get("killed")) and gp.get("first_surfaced_at") \
        and not gp.get("seen_at")
