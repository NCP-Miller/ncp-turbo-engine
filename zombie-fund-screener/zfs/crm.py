"""CRM layer: contacts, activities, tasks, and the cadence engine.

The cadence engine is the automation heart of daily work: when you log
an outbound touch that got no reply, it auto-creates the next follow-up
task on your schedule (+3 / +7 / +14 business days by default), and
after the max number of touches it suggests moving the GP to Nurture.
Suggestions are only suggestions — nothing changes status automatically.
"""

from datetime import date, timedelta

from zfs.db import connect, now
from zfs.settings import get_cadence

# Outcomes that mean the prospect actually replied/connected — these stop
# the no-reply cadence from scheduling another follow-up.
_REPLY_OUTCOMES = {"Connected", "Replied", "Meeting set"}


def add_business_days(start, days):
    """Skip weekends (simple version — holidays not modeled)."""
    d = start
    added = 0
    while added < days:
        d += timedelta(days=1)
        if d.weekday() < 5:
            added += 1
    return d


# ── Contacts ─────────────────────────────────────────────────────────

def add_contact(gp_id=None, company_id=None, name="", title=None, email=None,
                phone=None, linkedin_url=None, role_tag="Other",
                preferred=0, notes=""):
    conn = connect()
    try:
        cur = conn.execute(
            """INSERT INTO contacts (gp_id, company_id, name, title, email,
               phone, linkedin_url, role_tag, preferred, notes, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (gp_id, company_id, name, title, email, phone, linkedin_url,
             role_tag, preferred, notes, now()),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def list_contacts(gp_id=None, company_id=None):
    conn = connect()
    try:
        if gp_id is not None:
            rows = conn.execute(
                "SELECT * FROM contacts WHERE gp_id = ? "
                "ORDER BY preferred DESC, name", (gp_id,)).fetchall()
        elif company_id is not None:
            rows = conn.execute(
                "SELECT * FROM contacts WHERE company_id = ? ORDER BY name",
                (company_id,)).fetchall()
        else:
            rows = conn.execute("SELECT * FROM contacts ORDER BY name").fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def delete_contact(contact_id):
    conn = connect()
    try:
        conn.execute("DELETE FROM contacts WHERE id = ?", (contact_id,))
        conn.commit()
    finally:
        conn.close()


# ── Activities + cadence ─────────────────────────────────────────────

def outbound_touch_count(gp_id):
    """How many outbound touches have been logged for this GP."""
    conn = connect()
    try:
        r = conn.execute(
            "SELECT COUNT(*) AS n FROM events "
            "WHERE gp_id = ? AND kind = 'activity' AND direction = 'outbound'",
            (gp_id,)).fetchone()
        return r["n"] or 0
    finally:
        conn.close()


def log_activity(gp_id, type_, summary, direction="outbound", outcome="—",
                 contact_id=None, user=None):
    """Log an activity, run the cadence engine, return status suggestions.

    Returns a dict:
      {"suggestion": "Outreach Sent"|"In Dialogue"|"Nurture"|None,
       "auto_task": description or None}
    """
    conn = connect()
    try:
        conn.execute(
            """INSERT INTO events (gp_id, contact_id, kind, type, direction,
               summary, outcome, user, timestamp)
               VALUES (?, ?, 'activity', ?, ?, ?, ?, ?, ?)""",
            (gp_id, contact_id, type_, direction, summary, outcome, user, now()),
        )
        conn.commit()
        gp_status = conn.execute(
            "SELECT status FROM gps WHERE id = ?", (gp_id,)).fetchone()
        gp_name = conn.execute(
            "SELECT name FROM gps WHERE id = ?", (gp_id,)).fetchone()
    finally:
        conn.close()

    result = {"suggestion": None, "auto_task": None}
    cad = get_cadence()

    if direction == "inbound" or outcome in _REPLY_OUTCOMES:
        # They engaged → suggest moving the pipeline forward
        if gp_status and gp_status["status"] not in ("In Dialogue", "NDA"):
            result["suggestion"] = "In Dialogue"
        return result

    # Outbound with no reply → cadence
    touches = outbound_touch_count(gp_id)
    if touches == 1 and gp_status and gp_status["status"] == "New":
        result["suggestion"] = "Outreach Sent"

    if touches >= cad["max_touches"]:
        result["suggestion"] = "Nurture"
        return result

    intervals = cad["intervals"] or [3, 7, 14]
    # touch 1 → intervals[0], touch 2 → intervals[1], beyond → last interval
    days = intervals[min(touches - 1, len(intervals) - 1)]
    due = add_business_days(date.today(), days)
    desc = (f"Follow-up #{touches + 1} with "
            f"{gp_name['name'] if gp_name else 'GP'} "
            f"(no reply to touch {touches})")
    add_task(gp_id, desc, due.isoformat(), priority="Medium",
             assigned_to=user or "Trey", auto_generated=1,
             contact_id=contact_id)
    result["auto_task"] = f"{desc} — due {due.isoformat()}"
    return result


def timeline(gp_id, limit=100):
    """Unified reverse-chronological history: activities + status + kills."""
    conn = connect()
    try:
        rows = conn.execute(
            """SELECT e.*, c.name AS contact_name FROM events e
               LEFT JOIN contacts c ON c.id = e.contact_id
               WHERE e.gp_id = ? ORDER BY e.timestamp DESC LIMIT ?""",
            (gp_id, limit)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def last_activity_date(gp_id):
    conn = connect()
    try:
        r = conn.execute(
            "SELECT MAX(timestamp) AS t FROM events "
            "WHERE gp_id = ? AND kind = 'activity'", (gp_id,)).fetchone()
        return r["t"]
    finally:
        conn.close()


# ── Tasks ────────────────────────────────────────────────────────────

def add_task(gp_id, description, due_date, priority="Medium",
             assigned_to="Trey", auto_generated=0, contact_id=None):
    conn = connect()
    try:
        cur = conn.execute(
            """INSERT INTO tasks (gp_id, contact_id, description, due_date,
               priority, assigned_to, auto_generated, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (gp_id, contact_id, description, due_date, priority, assigned_to,
             auto_generated, now()),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def complete_task(task_id):
    conn = connect()
    try:
        conn.execute("UPDATE tasks SET done = 1, done_at = ? WHERE id = ?",
                     (now(), task_id))
        conn.commit()
    finally:
        conn.close()


def dismiss_task(task_id):
    conn = connect()
    try:
        conn.execute("UPDATE tasks SET dismissed = 1 WHERE id = ?", (task_id,))
        conn.commit()
    finally:
        conn.close()


def open_tasks(gp_id=None):
    conn = connect()
    try:
        q = ("SELECT t.*, g.name AS gp_name FROM tasks t "
             "JOIN gps g ON g.id = t.gp_id "
             "WHERE t.done = 0 AND t.dismissed = 0 AND g.killed = 0")
        params = []
        if gp_id is not None:
            q += " AND t.gp_id = ?"
            params.append(gp_id)
        q += " ORDER BY t.due_date"
        return [dict(r) for r in conn.execute(q, params).fetchall()]
    finally:
        conn.close()


def next_task_date(gp_id):
    conn = connect()
    try:
        r = conn.execute(
            "SELECT MIN(due_date) AS d FROM tasks "
            "WHERE gp_id = ? AND done = 0 AND dismissed = 0",
            (gp_id,)).fetchone()
        return r["d"]
    finally:
        conn.close()


# ── Today view queries ───────────────────────────────────────────────

def today_buckets():
    """Split open tasks into overdue / due today / due this week."""
    tasks = open_tasks()
    today = date.today().isoformat()
    week_end = (date.today() + timedelta(days=7)).isoformat()
    overdue = [t for t in tasks if t["due_date"] < today]
    due_today = [t for t in tasks if t["due_date"] == today]
    due_week = [t for t in tasks if today < t["due_date"] <= week_end]
    return overdue, due_today, due_week


def stale_relationships(stale_days):
    """GPs in Outreach Sent / In Dialogue with no activity in N days."""
    cutoff = (date.today() - timedelta(days=stale_days)).isoformat()
    conn = connect()
    try:
        rows = conn.execute(
            """SELECT g.*, MAX(e.timestamp) AS last_act FROM gps g
               LEFT JOIN events e ON e.gp_id = g.id AND e.kind = 'activity'
               WHERE g.killed = 0 AND g.status IN ('Outreach Sent', 'In Dialogue')
               GROUP BY g.id
               HAVING last_act IS NULL OR last_act < ?
               ORDER BY last_act""",
            (cutoff,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()
