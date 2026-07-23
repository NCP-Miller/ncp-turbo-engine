"""Database layer for the Zombie Fund Screener.

Design rule (from the spec): three separated layers so automated refreshes
can NEVER touch manual work.
  - EVIDENCE tables  -> owned by refreshes  (edgar_filings, adv_snapshots,
                        wayback_checks, pension_rows, provider_changes)
  - MANUAL tables    -> owned by the user   (gps' manual fields, funds,
                        portfolio_companies, contacts, tasks, events)
  - LIFECYCLE fields -> owned by the user   (status, kills, NEW badges)

Everything lives in one SQLite file: zombie_screener.db in the project
folder. Connections are short-lived; WAL mode allows the UI and any
background refresh to coexist.
"""

import json
import os
import sqlite3
from datetime import datetime, timezone

# The DB sits in the project folder (one level above zlib/)
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(_ROOT, "zombie_screener.db")

PIPELINE_STATUSES = ["New", "Researching", "Outreach Sent", "In Dialogue",
                     "NDA", "Nurture"]
KILL_REASONS = ["Too big", "Too small", "Wrong vertical", "Wrong geography",
                "Asset quality", "GP unresponsive", "Other"]
VERTICALS = ["Financial Services/IT", "Healthcare Services/IT",
             "Business Services/IT", "Other"]
ACTIVITY_TYPES = ["Call", "Email", "Meeting", "LinkedIn message", "Letter",
                  "Internal note"]
ACTIVITY_OUTCOMES = ["Connected", "Left voicemail", "No answer", "Replied",
                     "Bounced", "Meeting set", "—"]

# States east of Denver for the geography screen (everything not in this set
# gets the "geographic exception" badge, not excluded)
EAST_OF_DENVER = {
    "AL", "AR", "CT", "DC", "DE", "FL", "GA", "IA", "IL", "IN", "KS", "KY",
    "LA", "MA", "MD", "ME", "MI", "MN", "MO", "MS", "NC", "ND", "NE", "NH",
    "NJ", "NY", "OH", "OK", "PA", "RI", "SC", "SD", "TN", "TX", "VA", "VT",
    "WI", "WV", "CO",
}

# Direct links to state UCC search pages (Signal 10 guided workflow)
UCC_SEARCH_URLS = {
    "DE": "https://icis.corp.delaware.gov/ecorp/ucc/uccsearch.aspx",
    "GA": "https://gsccca.org/search/ucc",
    "AL": "https://www.sos.alabama.gov/government-records/ucc-records",
    "TN": "https://tnbear.tn.gov/UCC/Ecommerce/UCCSearch.aspx",
    "FL": "https://www.floridaucc.com/uccweb/search.aspx",
    "TX": "https://webservices.sos.state.tx.us/ucc/ucc-search.aspx",
    "NC": "https://www.sosnc.gov/online_services/search/by_title/_UCC",
    "VA": "https://cis.scc.virginia.gov/Ucc/Search",
    "OH": "https://bsportal.ohiosos.gov/UCC/Search",
    "IL": "https://apps.ilsos.gov/uccsearch/",
    "NY": "https://appext20.dos.ny.gov/pls/ucc_public/web_search.main_frame",
    "PA": "https://www.corporations.pa.gov/search/uccsearch",
}


def now():
    """UTC timestamp string used everywhere in the DB."""
    return datetime.now(timezone.utc).isoformat()


def connect():
    conn = sqlite3.connect(DB_PATH, timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    """Create every table if missing. Safe to call on every page load."""
    conn = connect()
    c = conn.cursor()

    # ── MANUAL + LIFECYCLE: the GP itself ────────────────────────────
    c.execute("""CREATE TABLE IF NOT EXISTS gps (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        name_key TEXT NOT NULL UNIQUE,        -- lowercase, for dedup
        website TEXT, linkedin_url TEXT, crd_number TEXT,
        city TEXT, state TEXT, notes TEXT DEFAULT '',
        -- lifecycle (user-owned, survives every refresh)
        status TEXT NOT NULL DEFAULT 'New',
        killed INTEGER NOT NULL DEFAULT 0,
        kill_category TEXT, kill_reason TEXT,
        killed_at TEXT, resurrected_at TEXT,
        first_surfaced_at TEXT,               -- when it first hit the pool
        seen_at TEXT,                          -- detail page opened => clears NEW
        -- Signal 5 manual checklist (LinkedIn — never scraped)
        li_current_headcount INTEGER, li_peak_headcount INTEGER,
        li_junior_hire_recent INTEGER,         -- 1 yes / 0 no / NULL unknown
        li_notes TEXT DEFAULT '',
        -- Signal 7 manual exit record
        last_exit_date TEXT, exit_last_checked TEXT,
        created_at TEXT NOT NULL, updated_at TEXT NOT NULL
    )""")

    # ── MANUAL: funds (seeded by EDGAR refresh OR entered by hand) ───
    c.execute("""CREATE TABLE IF NOT EXISTS funds (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        gp_id INTEGER NOT NULL REFERENCES gps(id),
        name TEXT NOT NULL,
        filing_date TEXT,                      -- Form D date or vintage year
        offering_amount TEXT, state TEXT,
        sec_file_number TEXT, edgar_url TEXT,
        source TEXT DEFAULT 'manual',          -- manual / edgar / pension
        -- Signal 9 manual extension record
        term_extension_note TEXT, term_extension_source TEXT,
        created_at TEXT NOT NULL
    )""")

    # ── MANUAL: portfolio companies (Signals 4, 8, 10 live here) ─────
    c.execute("""CREATE TABLE IF NOT EXISTS portfolio_companies (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        gp_id INTEGER NOT NULL REFERENCES gps(id),
        name TEXT NOT NULL,
        website TEXT, hq_state TEXT,
        vertical TEXT DEFAULT 'Other',
        ebitda_estimate TEXT,                  -- free text like "2.5M" or blank
        acquisition_date TEXT,                 -- Signal 4
        -- Signal 8 manual decay checklist
        decay_exec_departures INTEGER DEFAULT 0,
        decay_job_postings INTEGER,            -- 1 yes / 0 no / NULL unknown
        decay_notes TEXT DEFAULT '',
        -- Signal 10 manual UCC findings
        ucc_active_liens INTEGER,
        ucc_secured_parties TEXT,
        ucc_last_filing_date TEXT,
        ucc_lender_changed INTEGER DEFAULT 0,
        ucc_amendment_count INTEGER,
        ucc_notes TEXT DEFAULT '',
        notes TEXT DEFAULT '',
        created_at TEXT NOT NULL
    )""")

    # ── MANUAL: contacts (attachable to a GP or a portfolio company) ─
    c.execute("""CREATE TABLE IF NOT EXISTS contacts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        gp_id INTEGER REFERENCES gps(id),
        company_id INTEGER REFERENCES portfolio_companies(id),
        name TEXT NOT NULL, title TEXT, email TEXT, phone TEXT,
        linkedin_url TEXT,
        role_tag TEXT DEFAULT 'Other',
        preferred INTEGER DEFAULT 0,
        notes TEXT DEFAULT '',
        created_at TEXT NOT NULL
    )""")

    # ── MANUAL: unified timeline (activities + status/kill events) ───
    c.execute("""CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        gp_id INTEGER NOT NULL REFERENCES gps(id),
        contact_id INTEGER REFERENCES contacts(id),
        kind TEXT NOT NULL,                    -- activity / status / kill / resurrect
        type TEXT,                             -- Call/Email/... for activities
        direction TEXT,                        -- outbound / inbound
        summary TEXT NOT NULL,
        outcome TEXT,
        user TEXT,
        timestamp TEXT NOT NULL
    )""")

    # ── MANUAL: tasks (with cadence automation) ──────────────────────
    c.execute("""CREATE TABLE IF NOT EXISTS tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        gp_id INTEGER NOT NULL REFERENCES gps(id),
        contact_id INTEGER REFERENCES contacts(id),
        description TEXT NOT NULL,
        due_date TEXT NOT NULL,
        priority TEXT DEFAULT 'Medium',
        assigned_to TEXT DEFAULT 'Trey',
        done INTEGER DEFAULT 0, done_at TEXT,
        auto_generated INTEGER DEFAULT 0,      -- created by the cadence engine
        dismissed INTEGER DEFAULT 0,           -- user dismissed an auto-task
        created_at TEXT NOT NULL
    )""")

    # ── MANUAL: outreach email templates ─────────────────────────────
    c.execute("""CREATE TABLE IF NOT EXISTS templates (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        subject TEXT DEFAULT '',
        body TEXT NOT NULL,
        created_at TEXT NOT NULL, updated_at TEXT NOT NULL
    )""")

    # ── SETTINGS: signal config, presets, cadence (JSON blobs) ───────
    c.execute("""CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )""")

    # ── EVIDENCE tables (refresh-owned; Phase 3 importers fill them) ─
    c.execute("""CREATE TABLE IF NOT EXISTS edgar_filings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        gp_id INTEGER REFERENCES gps(id),
        sponsor_name TEXT, fund_name TEXT, filing_date TEXT,
        offering_amount TEXT, state TEXT, sec_file_number TEXT,
        edgar_url TEXT, fetched_at TEXT
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS adv_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        gp_id INTEGER REFERENCES gps(id),
        crd_number TEXT, snapshot_date TEXT,
        raum REAL, employees INTEGER,
        funds_json TEXT,                       -- Schedule D 7.B.1 rows
        fetched_at TEXT
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS wayback_checks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        gp_id INTEGER REFERENCES gps(id),
        company_id INTEGER REFERENCES portfolio_companies(id),
        url TEXT, last_change_date TEXT,
        snapshot_url TEXT, checked_at TEXT
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS pension_rows (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        gp_id INTEGER REFERENCES gps(id),
        fund_id INTEGER REFERENCES funds(id),
        source TEXT,                           -- CalPERS / upload filename
        fund_name TEXT, vintage_year INTEGER,
        committed REAL, nav REAL, dpi REAL, irr REAL,
        confirmed INTEGER DEFAULT 0,           -- user confirmed the fuzzy match
        imported_at TEXT
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS provider_changes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        gp_id INTEGER REFERENCES gps(id),
        fund_name TEXT, provider_role TEXT,
        old_provider TEXT, new_provider TEXT,
        change_date TEXT, detected_at TEXT
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS refresh_log (
        source TEXT PRIMARY KEY,
        last_run TEXT, detail TEXT
    )""")

    conn.commit()
    conn.close()


# ── tiny helpers used across the app ─────────────────────────────────

def name_key(name):
    return (name or "").strip().lower()


def get_setting(key, default=None):
    conn = connect()
    try:
        r = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
        return json.loads(r["value"]) if r else default
    finally:
        conn.close()


def set_setting(key, value):
    conn = connect()
    try:
        conn.execute(
            "INSERT INTO settings (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, json.dumps(value)),
        )
        conn.commit()
    finally:
        conn.close()


def load_config():
    """Read config.json (SEC contact email, user names)."""
    path = os.path.join(_ROOT, "config.json")
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"sec_contact_email": "", "users": ["Trey", "Intern"]}
