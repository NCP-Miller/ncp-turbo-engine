"""Lightweight CRM for tracking target prospects.

Deals live in their own SQLite DB (pipeline_data/crm.db) so they persist
across searches and projects. Every deal carries a status, notes, an
optional next-follow-up date, Salesforce IDs, and a timestamped activity
log. Backed up to the GitHub 'data' branch alongside project state.
"""

import glob
import json
import os
import sqlite3
from datetime import datetime, timezone, timedelta

_CRM_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "pipeline_data")
_CRM_DB = os.path.join(_CRM_DIR, "crm.db")
_FEEDBACK_PATH = os.path.join(_CRM_DIR, "feedback_log.json")

# ---------------------------------------------------------------------------
# STATUS MODEL
# ---------------------------------------------------------------------------

STATUSES = [
    "New",
    "Outreach Active",
    "In Dialogue",
    "Meeting Scheduled",
    "Opportunity",
    "Revisit Later",
    "Closed – No Response",
    "Contacted – No Opportunity",
    "Not a Fit",
]

TERMINAL_STATUSES = {
    "Closed – No Response",
    "Contacted – No Opportunity",
    "Not a Fit",
}

# Days of inactivity before a deal shows up in "Needs Attention"
ATTENTION_RULES = {
    "New": 3,
    "Outreach Active": 5,
    "In Dialogue": 7,
    "Meeting Scheduled": 7,
    "Opportunity": 7,
    "Revisit Later": 60,  # only if no next_followup date is set
}

ACTIVITY_TYPES = ["Call", "Email", "LinkedIn", "Text", "Meeting", "Note"]


# ---------------------------------------------------------------------------
# DB SETUP
# ---------------------------------------------------------------------------

def _connect():
    os.makedirs(_CRM_DIR, exist_ok=True)
    conn = sqlite3.connect(_CRM_DB, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    conn = _connect()
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS deals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company TEXT NOT NULL,
                company_key TEXT NOT NULL UNIQUE,
                website TEXT, contact_name TEXT, title TEXT,
                email TEXT, phone TEXT, city TEXT, state TEXT,
                niche TEXT, project TEXT,
                status TEXT NOT NULL DEFAULT 'New',
                source TEXT, notes TEXT DEFAULT '',
                next_followup TEXT,
                sf_account_id TEXT, sf_contact_id TEXT,
                row_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )""")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS activities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                deal_id INTEGER NOT NULL,
                type TEXT NOT NULL,
                summary TEXT NOT NULL,
                detail TEXT DEFAULT '',
                timestamp TEXT NOT NULL,
                synced_to_sf INTEGER DEFAULT 0,
                FOREIGN KEY (deal_id) REFERENCES deals(id)
            )""")
        cols = {r[1] for r in conn.execute("PRAGMA table_info(deals)").fetchall()}
        if "memo" not in cols:
            conn.execute("ALTER TABLE deals ADD COLUMN memo TEXT")
        conn.commit()
    finally:
        conn.close()


def _now():
    return datetime.now(timezone.utc).isoformat()


def _key(company):
    return (company or "").strip().lower()


# ---------------------------------------------------------------------------
# DEAL CRUD
# ---------------------------------------------------------------------------

def upsert_deal(company, row=None, niche=None, project=None, source="pipeline",
                status=None, memo=None):
    """Create a deal if it doesn't exist; refresh contact fields if it does.

    Never downgrades an existing deal's status. Returns the deal id.
    """
    init_db()
    row = row or {}

    def _clean(v):
        return None if v in (None, "", "N/A") else v

    email = _clean(row.get("Email")) or _clean(row.get("Email Estimate"))
    fields = {
        "website": _clean(row.get("Website")),
        "contact_name": _clean(row.get("CEO/Owner Name")),
        "title": _clean(row.get("Title")),
        "email": email,
        "phone": _clean(row.get("Phone")),
        "city": _clean(row.get("City")),
        "state": _clean(row.get("State")),
    }

    conn = _connect()
    try:
        existing = conn.execute(
            "SELECT id FROM deals WHERE company_key = ?", (_key(company),)
        ).fetchone()
        if existing:
            updates = {k: v for k, v in fields.items() if v}
            if row:
                updates["row_json"] = json.dumps(row, default=str)
            if memo:
                updates["memo"] = memo
            if updates:
                sets = ", ".join(f"{k} = ?" for k in updates)
                conn.execute(
                    f"UPDATE deals SET {sets}, updated_at = ? WHERE id = ?",
                    (*updates.values(), _now(), existing["id"]),
                )
                conn.commit()
            return existing["id"]

        cur = conn.execute(
            """INSERT INTO deals
               (company, company_key, website, contact_name, title, email,
                phone, city, state, niche, project, status, source,
                row_json, memo, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (company, _key(company), fields["website"], fields["contact_name"],
             fields["title"], fields["email"], fields["phone"], fields["city"],
             fields["state"], niche, project, status or "New", source,
             json.dumps(row, default=str) if row else None, memo,
             _now(), _now()),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def get_deal(company):
    init_db()
    conn = _connect()
    try:
        r = conn.execute(
            "SELECT * FROM deals WHERE company_key = ?", (_key(company),)
        ).fetchone()
        return dict(r) if r else None
    finally:
        conn.close()


def list_deals(statuses=None, search=None):
    """Return deals (as dicts) with a last_activity timestamp attached."""
    init_db()
    conn = _connect()
    try:
        q = """SELECT d.*,
                      (SELECT MAX(timestamp) FROM activities a
                        WHERE a.deal_id = d.id) AS last_activity
               FROM deals d"""
        clauses, params = [], []
        if statuses:
            clauses.append(f"d.status IN ({','.join('?' * len(statuses))})")
            params.extend(statuses)
        if search:
            clauses.append("(d.company LIKE ? OR d.contact_name LIKE ? OR d.niche LIKE ?)")
            like = f"%{search}%"
            params.extend([like, like, like])
        if clauses:
            q += " WHERE " + " AND ".join(clauses)
        q += " ORDER BY COALESCE(last_activity, d.updated_at) DESC"
        return [dict(r) for r in conn.execute(q, params).fetchall()]
    finally:
        conn.close()


def update_deal(deal_id, **fields):
    allowed = {"status", "notes", "next_followup", "sf_account_id",
               "sf_contact_id", "contact_name", "email", "phone", "title"}
    fields = {k: v for k, v in fields.items() if k in allowed}
    if not fields:
        return
    conn = _connect()
    try:
        sets = ", ".join(f"{k} = ?" for k in fields)
        conn.execute(
            f"UPDATE deals SET {sets}, updated_at = ? WHERE id = ?",
            (*fields.values(), _now(), deal_id),
        )
        conn.commit()
    finally:
        conn.close()


def set_status(deal_id, new_status, old_status=None):
    """Change a deal's status and log it as an activity."""
    update_deal(deal_id, status=new_status)
    label = f"Status → {new_status}"
    if old_status and old_status != new_status:
        label = f"Status: {old_status} → {new_status}"
    log_activity(deal_id, "Note", label, activity_kind="status")


# ---------------------------------------------------------------------------
# ACTIVITY LOG
# ---------------------------------------------------------------------------

def log_activity(deal_id, type_, summary, detail="", synced_to_sf=0,
                 activity_kind=None):
    init_db()
    conn = _connect()
    try:
        conn.execute(
            """INSERT INTO activities (deal_id, type, summary, detail,
                                       timestamp, synced_to_sf)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (deal_id, type_, summary, detail, _now(), synced_to_sf),
        )
        conn.execute(
            "UPDATE deals SET updated_at = ? WHERE id = ?", (_now(), deal_id)
        )
        conn.commit()
    finally:
        conn.close()


def log_activity_for_company(company, type_, summary, detail="",
                             synced_to_sf=0, create_if_missing=False,
                             row=None, niche=None):
    """Convenience: log an activity by company name. Optionally create the deal."""
    deal = get_deal(company)
    if not deal:
        if not create_if_missing:
            return None
        deal_id = upsert_deal(company, row=row, niche=niche)
    else:
        deal_id = deal["id"]
    log_activity(deal_id, type_, summary, detail, synced_to_sf)
    return deal_id


def list_activities(deal_id, limit=50):
    init_db()
    conn = _connect()
    try:
        rows = conn.execute(
            """SELECT * FROM activities WHERE deal_id = ?
               ORDER BY timestamp DESC LIMIT ?""",
            (deal_id, limit),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def unsynced_activities(deal_id):
    init_db()
    conn = _connect()
    try:
        rows = conn.execute(
            """SELECT * FROM activities
               WHERE deal_id = ? AND synced_to_sf = 0
               ORDER BY timestamp ASC""",
            (deal_id,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def mark_activities_synced(activity_ids):
    if not activity_ids:
        return
    conn = _connect()
    try:
        conn.execute(
            f"UPDATE activities SET synced_to_sf = 1 "
            f"WHERE id IN ({','.join('?' * len(activity_ids))})",
            activity_ids,
        )
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# NEEDS ATTENTION
# ---------------------------------------------------------------------------

def deals_needing_attention():
    """Return deals that are due for a touch, with a reason string each."""
    init_db()
    now = datetime.now(timezone.utc)
    results = []
    for deal in list_deals():
        status = deal.get("status", "New")
        if status in TERMINAL_STATUSES:
            continue

        # Explicit follow-up date takes priority
        nf = deal.get("next_followup")
        if nf:
            try:
                due = datetime.fromisoformat(nf)
                if due.tzinfo is None:
                    due = due.replace(tzinfo=timezone.utc)
                if due <= now:
                    days_over = (now - due).days
                    results.append({
                        **deal,
                        "attention_reason": (
                            f"Follow-up date passed"
                            + (f" {days_over}d ago" if days_over > 0 else " today")
                        ),
                    })
                continue  # a future follow-up date suppresses inactivity nudges
            except (ValueError, TypeError):
                pass

        threshold = ATTENTION_RULES.get(status)
        if threshold is None:
            continue
        last = deal.get("last_activity") or deal.get("created_at")
        try:
            last_dt = datetime.fromisoformat(last)
            if last_dt.tzinfo is None:
                last_dt = last_dt.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            continue
        idle_days = (now - last_dt).days
        if idle_days >= threshold:
            results.append({
                **deal,
                "attention_reason": f"No activity in {idle_days} days ({status})",
            })
    return results


# ---------------------------------------------------------------------------
# BACKFILL FROM PAST SEARCHES
# ---------------------------------------------------------------------------

def _read_project_db(db_path):
    """Read completed_memos, memo_verdicts, and niche from a project DB."""
    try:
        conn = sqlite3.connect(db_path, timeout=10)
        rows = conn.execute("SELECT key, value FROM pipeline_state").fetchall()
        conn.close()
    except sqlite3.Error:
        return [], {}, None
    state = {}
    for key, value in rows:
        try:
            state[key] = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            pass
    memos = state.get("completed_memos") or []
    verdicts = state.get("memo_verdicts") or {}
    niche = (state.get("config") or {}).get("niche")
    return memos, verdicts, niche


def backfill_sources():
    """Preview what the backfill can see: local DBs, GitHub backups, feedback."""
    local_dbs = sorted(glob.glob(os.path.join(_CRM_DIR, "project_*.db")))
    if os.path.exists(os.path.join(_CRM_DIR, "state.db")):
        local_dbs.append(os.path.join(_CRM_DIR, "state.db"))

    github_projects = []
    try:
        from lib.github_backup import is_configured, read_projects_index
        if is_configured():
            github_projects = [
                p.get("slug") for p in read_projects_index() if p.get("slug")
            ]
    except Exception:
        pass

    feedback_count = 0
    try:
        with open(_FEEDBACK_PATH) as f:
            feedback_count = len(json.load(f))
    except (FileNotFoundError, json.JSONDecodeError):
        pass

    return {
        "local_dbs": [os.path.basename(p) for p in local_dbs],
        "github_projects": github_projects,
        "feedback_entries": feedback_count,
    }


def backfill_from_history():
    """Seed the CRM from past searches: every memo + every liked company.

    Reads local project DBs first, then the GitHub data-branch backups
    (the durable record on Streamlit Cloud, where local disk is ephemeral).
    Skips companies the user explicitly rejected. Returns a summary dict.
    """
    init_db()
    counters = {"created": 0, "skipped_rejected": 0, "already_tracked": 0}
    seen = set()

    def _ingest(memos, verdicts, niche, project):
        for memo in memos or []:
            company = memo.get("company")
            if not company or _key(company) in seen:
                continue
            seen.add(_key(company))
            verdict = (verdicts or {}).get(company)
            if verdict == "rejected":
                counters["skipped_rejected"] += 1
                continue
            if get_deal(company):
                counters["already_tracked"] += 1
                continue
            source = "backfill-liked" if verdict == "liked" else "backfill-memo"
            deal_id = upsert_deal(
                company, row=memo.get("row") or {}, niche=niche,
                project=project, source=source, memo=memo.get("memo"),
            )
            note = "Imported from past search"
            if verdict == "liked":
                note += " — you marked this one 👍 Interested"
            log_activity(deal_id, "Note", note)
            counters["created"] += 1

    # 1. Local project DBs + the active state DB
    db_paths = sorted(glob.glob(os.path.join(_CRM_DIR, "project_*.db")))
    state_db = os.path.join(_CRM_DIR, "state.db")
    if os.path.exists(state_db):
        db_paths.append(state_db)
    scanned_slugs = set()
    for db_path in db_paths:
        project = os.path.basename(db_path).replace("project_", "").replace(".db", "")
        if project == "state":
            project = "active search"
        else:
            scanned_slugs.add(project)
        memos, verdicts, niche = _read_project_db(db_path)
        _ingest(memos, verdicts, niche, project)

    # 2. GitHub data-branch backups for projects missing locally
    github_scanned = 0
    try:
        from lib.github_backup import (
            is_configured, read_projects_index, read_project_backup,
        )
        if is_configured():
            for proj in read_projects_index():
                slug = proj.get("slug")
                if not slug or slug in scanned_slugs:
                    continue
                data = read_project_backup(slug)
                if not data:
                    continue
                github_scanned += 1
                _ingest(
                    data.get("completed_memos"),
                    data.get("memo_verdicts"),
                    (data.get("config") or {}).get("niche"),
                    proj.get("name") or slug,
                )
    except Exception:
        pass

    # 3. Liked companies from the feedback log (local, else GitHub backup)
    try:
        with open(_FEEDBACK_PATH) as f:
            feedback = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        feedback = []
    if not feedback:
        try:
            from lib.github_backup import read_feedback_backup
            feedback = read_feedback_backup()
        except Exception:
            feedback = []
    for fb in feedback:
        if (fb.get("verdict") or "").lower() != "liked":
            continue
        company = fb.get("company")
        if not company or _key(company) in seen or get_deal(company):
            continue
        seen.add(_key(company))
        deal_id = upsert_deal(
            company, niche=fb.get("niche"), source="backfill-liked",
        )
        log_activity(deal_id, "Note",
                     "Imported from feedback log (👍 Interested)",
                     detail=fb.get("feedback", ""))
        counters["created"] += 1

    counters["local_dbs_scanned"] = len(db_paths)
    counters["github_projects_scanned"] = github_scanned
    counters["feedback_entries"] = len(feedback)
    return counters


# ---------------------------------------------------------------------------
# EXPORT / BACKUP
# ---------------------------------------------------------------------------

def export_crm_to_json():
    """Full CRM dump for GitHub backup."""
    init_db()
    conn = _connect()
    try:
        deals = [dict(r) for r in conn.execute("SELECT * FROM deals").fetchall()]
        acts = [dict(r) for r in conn.execute("SELECT * FROM activities").fetchall()]
        return {"deals": deals, "activities": acts}
    finally:
        conn.close()


def import_crm_from_json(data):
    """Restore CRM from a backup dump. Only runs into an empty DB."""
    init_db()
    conn = _connect()
    try:
        count = conn.execute("SELECT COUNT(*) FROM deals").fetchone()[0]
        if count > 0:
            return False
        for d in data.get("deals", []):
            cols = [k for k in d.keys() if k != "last_activity"]
            conn.execute(
                f"INSERT OR IGNORE INTO deals ({','.join(cols)}) "
                f"VALUES ({','.join('?' * len(cols))})",
                [d[c] for c in cols],
            )
        for a in data.get("activities", []):
            cols = list(a.keys())
            conn.execute(
                f"INSERT OR IGNORE INTO activities ({','.join(cols)}) "
                f"VALUES ({','.join('?' * len(cols))})",
                [a[c] for c in cols],
            )
        conn.commit()
        return True
    finally:
        conn.close()


_DEAL_COLUMNS = {
    "company", "company_key", "website", "contact_name", "title", "email",
    "phone", "city", "state", "niche", "project", "status", "source",
    "notes", "next_followup", "sf_account_id", "sf_contact_id", "row_json",
    "memo", "created_at", "updated_at",
}


def merge_crm_export(data, adopt_status=False):
    """Merge a CRM export dict into the local DB. Purely additive:

    - Deals missing locally are inserted with their full record.
    - Existing deals: NULL/empty fields (notes, follow-up, SF ids, memo)
      are filled from the export; non-empty local values always win.
    - Statuses on existing deals are adopted ONLY when adopt_status=True
      and the local deal is still 'New' (used by explicit history
      recovery, so a freshly-recreated deal gets its real status back).
    - Activities are inserted unless an identical (timestamp, summary)
      entry already exists on that deal.

    Never deletes or overwrites deliberate local work. Returns counts.
    """
    init_db()
    result = {"deals_added": 0, "deals_upgraded": 0, "activities_added": 0}
    deals = data.get("deals") or []
    acts_by_old_id = {}
    for a in data.get("activities") or []:
        acts_by_old_id.setdefault(a.get("deal_id"), []).append(a)

    conn = _connect()
    try:
        for d in deals:
            ck = d.get("company_key") or _key(d.get("company", ""))
            if not ck or not d.get("company"):
                continue
            local = conn.execute(
                "SELECT * FROM deals WHERE company_key = ?", (ck,)
            ).fetchone()
            if local is None:
                cols = [c for c in d.keys() if c in _DEAL_COLUMNS]
                if "company_key" not in cols:
                    cols.append("company_key")
                    d = {**d, "company_key": ck}
                conn.execute(
                    f"INSERT INTO deals ({','.join(cols)}) "
                    f"VALUES ({','.join('?' * len(cols))})",
                    [d.get(c) for c in cols],
                )
                new_id = conn.execute(
                    "SELECT id FROM deals WHERE company_key = ?", (ck,)
                ).fetchone()[0]
                result["deals_added"] += 1
            else:
                new_id = local["id"]
                updates = {}
                if (adopt_status
                        and (local["status"] or "New") == "New"
                        and (d.get("status") or "New") != "New"):
                    updates["status"] = d["status"]
                if not (local["notes"] or "").strip() and (d.get("notes") or "").strip():
                    updates["notes"] = d["notes"]
                for f in ("next_followup", "sf_account_id", "sf_contact_id", "memo"):
                    if not local[f] and d.get(f):
                        updates[f] = d[f]
                if updates:
                    sets = ", ".join(f"{k} = ?" for k in updates)
                    conn.execute(
                        f"UPDATE deals SET {sets}, updated_at = ? WHERE id = ?",
                        (*updates.values(), _now(), new_id),
                    )
                    result["deals_upgraded"] += 1

            for a in acts_by_old_id.get(d.get("id"), []):
                exists = conn.execute(
                    "SELECT 1 FROM activities WHERE deal_id = ? "
                    "AND timestamp = ? AND summary = ?",
                    (new_id, a.get("timestamp"), a.get("summary")),
                ).fetchone()
                if not exists:
                    conn.execute(
                        """INSERT INTO activities
                           (deal_id, type, summary, detail, timestamp, synced_to_sf)
                           VALUES (?, ?, ?, ?, ?, ?)""",
                        (new_id, a.get("type") or "Note",
                         a.get("summary") or "", a.get("detail") or "",
                         a.get("timestamp") or _now(),
                         a.get("synced_to_sf") or 0),
                    )
                    result["activities_added"] += 1
        conn.commit()
    finally:
        conn.close()
    return result


def sync_with_github_backup():
    """Page-load safety net: merge the current GitHub backup into the
    local DB (additive only). Handles fresh containers even when the
    pipeline already recreated a few deals before the tracker opened.
    """
    try:
        from lib.github_backup import restore_crm
        data = restore_crm()
        if data:
            return merge_crm_export(data, adopt_status=False)
    except Exception:
        pass
    return None


def recover_from_history(max_versions=30):
    """Deep recovery: merge every historical version of the GitHub CRM
    backup (newest first), adopting statuses for deals stuck on 'New'.
    Use when deals or statuses have gone missing after a redeploy.
    """
    try:
        from lib.github_backup import restore_crm, read_crm_history
    except Exception:
        return {"error": "GitHub backup is not available."}
    totals = {"deals_added": 0, "deals_upgraded": 0,
              "activities_added": 0, "versions_scanned": 0}
    versions = []
    try:
        current = restore_crm()
        if current:
            versions.append(current)
        versions.extend(read_crm_history(max_versions))
    except Exception:
        pass
    if not versions:
        return {"error": "No backup versions found on the GitHub data branch."}
    for v in versions:
        try:
            r = merge_crm_export(v, adopt_status=True)
        except Exception:
            continue
        totals["versions_scanned"] += 1
        for k in ("deals_added", "deals_upgraded", "activities_added"):
            totals[k] += r[k]
    return totals


def get_deal_by_id(deal_id):
    init_db()
    conn = _connect()
    try:
        r = conn.execute(
            "SELECT * FROM deals WHERE id = ?", (deal_id,)
        ).fetchone()
        return dict(r) if r else None
    finally:
        conn.close()


def auto_sync_deal(deal_id):
    """Push status, notes, follow-up, and pending activities to Salesforce.

    Best-effort and silent: no-ops when Salesforce isn't configured or the
    deal isn't linked yet (no sf_account_id — use the manual Sync button
    once to link/create the Account). Returns True when a sync happened.
    """
    try:
        deal = get_deal_by_id(deal_id)
        if not deal or not deal.get("sf_account_id"):
            return False
        from lib.api_clients import get_secret
        from lib.salesforce import sf_login, sync_deal_to_salesforce
        username = get_secret("SF_USERNAME", "")
        if not username:
            return False
        sf = sf_login(
            username,
            get_secret("SF_PASSWORD", ""),
            get_secret("SF_CONSUMER_KEY", ""),
            get_secret("SF_CONSUMER_SECRET", ""),
            get_secret("SF_SECURITY_TOKEN", ""),
        )
        acts = unsynced_activities(deal_id)
        acct, cont, synced_ids = sync_deal_to_salesforce(sf, deal, acts)
        mark_activities_synced(synced_ids)
        update_deal(deal_id, sf_account_id=acct, sf_contact_id=cont)
        return True
    except Exception:
        return False


def sync_all_to_salesforce(include_unlinked=False):
    """Catch-up sync: push every deal's status, notes, follow-up, and
    pending activities to Salesforce in one pass.

    Linked deals (sf_account_id set) always sync. Unlinked deals sync only
    when include_unlinked=True — that creates their Salesforce Accounts —
    and only if they have real user activity beyond the initial import
    note, so untouched backfilled imports never spam the org.

    Returns a summary dict.
    """
    init_db()
    from lib.api_clients import get_secret
    from lib.salesforce import sf_login, sync_deal_to_salesforce

    username = get_secret("SF_USERNAME", "")
    if not username:
        return {"error": "Salesforce is not configured (SF_USERNAME missing)."}
    sf = sf_login(
        username,
        get_secret("SF_PASSWORD", ""),
        get_secret("SF_CONSUMER_KEY", ""),
        get_secret("SF_CONSUMER_SECRET", ""),
        get_secret("SF_SECURITY_TOKEN", ""),
    )

    summary = {"deals_synced": 0, "activities_synced": 0,
               "newly_linked": 0, "skipped_unlinked": 0, "errors": []}

    for deal in list_deals():
        acts = unsynced_activities(deal["id"])
        is_linked = bool(deal.get("sf_account_id"))
        if not is_linked:
            real_acts = [
                a for a in acts
                if not (a.get("summary") or "").startswith("Imported from")
            ]
            if not include_unlinked or not real_acts:
                if acts:
                    summary["skipped_unlinked"] += 1
                continue
        try:
            acct, cont, synced_ids = sync_deal_to_salesforce(sf, deal, acts)
            mark_activities_synced(synced_ids)
            update_deal(deal["id"], sf_account_id=acct, sf_contact_id=cont)
            summary["deals_synced"] += 1
            summary["activities_synced"] += len(synced_ids)
            if not is_linked:
                summary["newly_linked"] += 1
        except Exception as e:
            summary["errors"].append(f"{deal['company']}: {e}")

    return summary


def backup_to_github():
    """Push the full CRM to the GitHub data branch. Best-effort.

    Clobber-proof: merges the remote backup into the local DB first
    (additive only), so a freshly-wiped container can never overwrite
    the backup with fewer deals than it holds.
    """
    try:
        from lib.github_backup import backup_crm, restore_crm
        try:
            remote = restore_crm()
            if remote:
                local_count = len(export_crm_to_json().get("deals", []))
                remote_count = len(remote.get("deals", []))
                if remote_count > local_count:
                    merge_crm_export(remote, adopt_status=False)
        except Exception:
            pass
        return backup_crm(export_crm_to_json())
    except Exception:
        return False


def restore_from_github_if_empty():
    """On a fresh container, pull the CRM back from the data branch."""
    init_db()
    conn = _connect()
    try:
        count = conn.execute("SELECT COUNT(*) FROM deals").fetchone()[0]
    finally:
        conn.close()
    if count > 0:
        return False
    try:
        from lib.github_backup import restore_crm
        data = restore_crm()
        if data:
            return import_crm_from_json(data)
    except Exception:
        pass
    return False
