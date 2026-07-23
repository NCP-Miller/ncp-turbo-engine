"""Signals 3 & 8 importer — website staleness via the Wayback Machine.

Uses the free CDX API with collapse=digest: the archive returns one row
per CONTENT CHANGE, so the last row's timestamp is the last time the
page meaningfully changed. No key needed; we throttle politely.

Evidence lands in wayback_checks:
  - GP rows (company_id NULL) drive Signal 3 (stale GP website)
  - portfolio-company rows drive Signal 8 (company decay)
"""

import time

import requests

from zfs.db import connect, now, load_config

_CDX_URL = "https://web.archive.org/cdx/search/cdx"
_THROTTLE_SECONDS = 0.7


def _headers():
    email = load_config().get("sec_contact_email", "")
    return {"User-Agent": f"NCP Zombie Fund Screener {email}".strip()}


def _normalize(url):
    """Bare domain works best with the CDX API."""
    u = (url or "").strip()
    u = u.replace("https://", "").replace("http://", "")
    return u.rstrip("/")


def check_url(url, timeout=25):
    """Return {'last_change_date': 'YYYY-MM-DD', 'snapshot_url': ...}
    or None when the archive has nothing for this URL."""
    target = _normalize(url)
    if not target:
        return None
    params = {
        "url": target,
        "output": "json",
        "fl": "timestamp,digest",
        "collapse": "digest",      # one row per content change
        "filter": "statuscode:200",
        "limit": "500",
    }
    r = requests.get(_CDX_URL, params=params, headers=_headers(),
                     timeout=timeout)
    r.raise_for_status()
    rows = r.json()
    if not rows or len(rows) < 2:      # first row is the header
        return None
    last_ts = rows[-1][0]              # e.g. "20230115093011"
    date_str = f"{last_ts[:4]}-{last_ts[4:6]}-{last_ts[6:8]}"
    return {
        "last_change_date": date_str,
        "snapshot_url": f"https://web.archive.org/web/{last_ts}/{target}",
    }


def run_all(progress_cb=None):
    """Check every GP website and portfolio-company website.
    Returns {'checked': n, 'no_archive': n, 'errors': n}."""
    conn = connect()
    try:
        gp_targets = [dict(r) for r in conn.execute(
            "SELECT id, website FROM gps WHERE killed = 0 AND "
            "website IS NOT NULL AND website != ''").fetchall()]
        co_targets = [dict(r) for r in conn.execute(
            """SELECT pc.id, pc.gp_id, pc.website FROM portfolio_companies pc
               JOIN gps g ON g.id = pc.gp_id
               WHERE g.killed = 0 AND pc.website IS NOT NULL
               AND pc.website != ''""").fetchall()]
    finally:
        conn.close()

    jobs = [("gp", t) for t in gp_targets] + [("co", t) for t in co_targets]
    checked = no_archive = errors = 0
    total = len(jobs)

    for i, (kind, t) in enumerate(jobs):
        if progress_cb:
            progress_cb(i, total, t["website"])
        try:
            result = check_url(t["website"])
        except requests.RequestException:
            errors += 1
            time.sleep(2)
            continue
        conn = connect()
        try:
            if kind == "gp":
                gp_id, company_id = t["id"], None
            else:
                gp_id, company_id = t["gp_id"], t["id"]
            # Keep one row per target: replace the previous check
            conn.execute(
                "DELETE FROM wayback_checks WHERE gp_id = ? AND "
                + ("company_id IS NULL" if company_id is None
                   else "company_id = ?"),
                (gp_id,) if company_id is None else (gp_id, company_id))
            if result:
                conn.execute(
                    """INSERT INTO wayback_checks (gp_id, company_id, url,
                       last_change_date, snapshot_url, checked_at)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (gp_id, company_id, t["website"],
                     result["last_change_date"], result["snapshot_url"],
                     now()))
                checked += 1
            else:
                no_archive += 1
            conn.commit()
        finally:
            conn.close()
        time.sleep(_THROTTLE_SECONDS)

    conn = connect()
    try:
        conn.execute(
            "INSERT INTO refresh_log (source, last_run, detail) VALUES "
            "('wayback', ?, ?) ON CONFLICT(source) DO UPDATE SET "
            "last_run = excluded.last_run, detail = excluded.detail",
            (now(), f"{checked} checked, {no_archive} not archived, "
                    f"{errors} errors"))
        conn.commit()
    finally:
        conn.close()
    return {"checked": checked, "no_archive": no_archive, "errors": errors}
