"""Signal 1 importer — SEC EDGAR Form D filings (free, no API key).

Two modes:
  1. refresh_all(): for every GP already in the database, search EDGAR's
     full-text search for that firm's Form D filings, store them as
     evidence, and upsert each distinct fund entity into the funds table
     (source='edgar'). Signal 1 then fires from stored data.
  2. discover(query, ...): search OLD Form D filings by keyword so you
     can surface sponsors you don't know yet, then add the interesting
     ones as GPs with one click.

Politeness (SEC fair-use rules):
  - Every request carries a User-Agent with your contact email
    (config.json -> sec_contact_email).
  - Throttled to ~2 requests/second — far below SEC's 10/sec limit.
  - Per-GP results are cached: a GP refreshed in the last 20 hours is
    skipped unless you force it.
"""

import time
from datetime import datetime, timedelta, timezone

import requests

from zfs.db import connect, now, load_config

_FTS_URL = "https://efts.sec.gov/LATEST/search-index"
_THROTTLE_SECONDS = 0.5
_CACHE_HOURS = 20


def _headers():
    email = load_config().get("sec_contact_email", "")
    return {"User-Agent": f"NCP Zombie Fund Screener {email}".strip(),
            "Accept-Encoding": "gzip, deflate"}


def _search(query, forms="D", startdt=None, enddt=None, timeout=30):
    """One EDGAR full-text search call. Returns the raw hit list."""
    params = {"q": f'"{query}"', "forms": forms}
    if startdt and enddt:
        params["dateRange"] = "custom"
        params["startdt"] = startdt
        params["enddt"] = enddt
    r = requests.get(_FTS_URL, params=params, headers=_headers(),
                     timeout=timeout)
    r.raise_for_status()
    data = r.json()
    return (data.get("hits") or {}).get("hits") or []


def _hit_fields(hit):
    """Pull the fields we care about out of one FTS hit, defensively."""
    src = hit.get("_source") or {}
    adsh = src.get("adsh") or (hit.get("_id") or "").split(":")[0]
    ciks = src.get("ciks") or []
    cik = ciks[0].lstrip("0") if ciks else ""
    names = src.get("display_names") or []
    name = names[0].split("  (CIK")[0].strip() if names else ""
    file_date = src.get("file_date") or ""
    url = ""
    if cik and adsh:
        url = (f"https://www.sec.gov/Archives/edgar/data/{cik}/"
               f"{adsh.replace('-', '')}/{adsh}-index.htm")
    return {"adsh": adsh, "cik": cik, "entity": name,
            "file_date": file_date, "url": url,
            "file_type": src.get("file_type") or "D"}


def _recently_refreshed(conn, gp_id):
    r = conn.execute(
        "SELECT MAX(fetched_at) AS t FROM edgar_filings WHERE gp_id = ?",
        (gp_id,)).fetchone()
    if not r or not r["t"]:
        return False
    try:
        t = datetime.fromisoformat(r["t"])
        if t.tzinfo is None:
            t = t.replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc) - t < timedelta(hours=_CACHE_HOURS)
    except ValueError:
        return False


def refresh_gp(gp, force=False):
    """Fetch Form D filings for one GP. Returns (filings_added, funds_added)
    or raises requests exceptions for the caller to explain."""
    conn = connect()
    try:
        if not force and _recently_refreshed(conn, gp["id"]):
            return 0, 0
        hits = _search(gp["name"], forms="D")
        time.sleep(_THROTTLE_SECONDS)

        filings_added = funds_added = 0
        # Group filings by fund entity; earliest filing = the fund's launch
        by_entity = {}
        for h in hits:
            f = _hit_fields(h)
            if not f["entity"] or not f["file_date"]:
                continue
            # Evidence row (skip exact duplicates from previous runs)
            dup = conn.execute(
                "SELECT 1 FROM edgar_filings WHERE gp_id = ? AND "
                "fund_name = ? AND filing_date = ?",
                (gp["id"], f["entity"], f["file_date"])).fetchone()
            if not dup:
                conn.execute(
                    """INSERT INTO edgar_filings (gp_id, sponsor_name,
                       fund_name, filing_date, sec_file_number, edgar_url,
                       fetched_at) VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (gp["id"], gp["name"], f["entity"], f["file_date"],
                     f["adsh"], f["url"], now()))
                filings_added += 1
            cur = by_entity.get(f["entity"])
            if not cur or f["file_date"] < cur["file_date"]:
                by_entity[f["entity"]] = f

        # Upsert each distinct fund entity into the funds table
        for entity, f in by_entity.items():
            existing = conn.execute(
                "SELECT id, filing_date FROM funds WHERE gp_id = ? AND "
                "LOWER(name) = LOWER(?)", (gp["id"], entity)).fetchone()
            if existing is None:
                conn.execute(
                    """INSERT INTO funds (gp_id, name, filing_date,
                       sec_file_number, edgar_url, source, created_at)
                       VALUES (?, ?, ?, ?, ?, 'edgar', ?)""",
                    (gp["id"], entity, f["file_date"], f["adsh"],
                     f["url"], now()))
                funds_added += 1
            elif not existing["filing_date"]:
                conn.execute(
                    "UPDATE funds SET filing_date = ?, edgar_url = ? "
                    "WHERE id = ?", (f["file_date"], f["url"], existing["id"]))
        conn.commit()
        return filings_added, funds_added
    finally:
        conn.close()


def refresh_all(gps, progress_cb=None, force=False):
    """Refresh every GP with throttling. progress_cb(i, total, gp_name).
    Returns a plain-English summary dict."""
    total = len(gps)
    filings = funds = errors = skipped = 0
    for i, gp in enumerate(gps):
        if progress_cb:
            progress_cb(i, total, gp["name"])
        try:
            fa, fu = refresh_gp(gp, force=force)
            if fa == 0 and fu == 0 and not force:
                skipped += 1
            filings += fa
            funds += fu
        except requests.RequestException:
            errors += 1
            time.sleep(2)          # back off politely on any error
    conn = connect()
    try:
        conn.execute(
            "INSERT INTO refresh_log (source, last_run, detail) VALUES "
            "('edgar', ?, ?) ON CONFLICT(source) DO UPDATE SET "
            "last_run = excluded.last_run, detail = excluded.detail",
            (now(), f"{filings} filings, {funds} funds, {errors} errors"))
        conn.commit()
    finally:
        conn.close()
    return {"filings": filings, "funds": funds, "errors": errors,
            "skipped": skipped}


def discover(query, start_year, end_year, max_results=60):
    """Search OLD Form D filings by keyword to surface unknown sponsors.

    Returns a de-duplicated list of {entity, file_date, url} you can
    review and add as GPs. Old filing + interesting name = a lead worth
    a per-GP refresh (which then checks for successor funds).
    """
    hits = _search(query, forms="D",
                   startdt=f"{start_year}-01-01", enddt=f"{end_year}-12-31")
    seen, out = set(), []
    for h in hits:
        f = _hit_fields(h)
        key = f["entity"].lower()
        if not f["entity"] or key in seen:
            continue
        seen.add(key)
        out.append(f)
        if len(out) >= max_results:
            break
    return out
