"""Scoring engine: evaluate the ten signals from STORED data only.

Rules from the spec:
  - Toggled-off signals are excluded from numerator AND denominator, so
    scores stay comparable across configurations.
  - Composite = sum(fired x weight) / sum(enabled weights), scaled 0-100.
  - Thresholds apply instantly to already-stored data — no re-fetching.
  - Signals with no data simply don't fire (evidence arrives over time
    from the Data Manager importers or manual entry).

Each evaluator returns a dict:
  {"fired": bool, "evidence": "human-readable why", "link": url or None}
"""

import json
from datetime import date, datetime

from zfs.db import connect


def _years_ago(date_str):
    """Years between a stored date string (YYYY-MM-DD...) and today."""
    if not date_str:
        return None
    try:
        d = datetime.fromisoformat(str(date_str)[:10]).date()
    except ValueError:
        # allow bare years like "2014"
        try:
            d = date(int(str(date_str)[:4]), 1, 1)
        except ValueError:
            return None
    return (date.today() - d).days / 365.25


def _gp_bundle(conn, gp_id):
    """Load everything scoring needs for one GP in one place."""
    return {
        "funds": [dict(r) for r in conn.execute(
            "SELECT * FROM funds WHERE gp_id = ?", (gp_id,)).fetchall()],
        "companies": [dict(r) for r in conn.execute(
            "SELECT * FROM portfolio_companies WHERE gp_id = ?",
            (gp_id,)).fetchall()],
        "adv": [dict(r) for r in conn.execute(
            "SELECT * FROM adv_snapshots WHERE gp_id = ? ORDER BY snapshot_date",
            (gp_id,)).fetchall()],
        "wayback_gp": [dict(r) for r in conn.execute(
            "SELECT * FROM wayback_checks WHERE gp_id = ? AND company_id IS NULL "
            "ORDER BY checked_at DESC", (gp_id,)).fetchall()],
        "wayback_co": [dict(r) for r in conn.execute(
            "SELECT * FROM wayback_checks WHERE gp_id = ? AND company_id IS NOT NULL",
            (gp_id,)).fetchall()],
        "pension": [dict(r) for r in conn.execute(
            "SELECT * FROM pension_rows WHERE gp_id = ? AND confirmed = 1",
            (gp_id,)).fetchall()],
        "providers": [dict(r) for r in conn.execute(
            "SELECT * FROM provider_changes WHERE gp_id = ?",
            (gp_id,)).fetchall()],
    }


def evaluate_gp(gp, bundle, settings):
    """Run all enabled signals for one GP. Returns {signal_id: result}."""
    th = {k: v["thresholds"] for k, v in settings.items()}
    out = {}
    funds = bundle["funds"]
    companies = bundle["companies"]

    # ── Signal 1: newest fund older than N years, no successor ───────
    if settings["s1"]["enabled"]:
        dates = [(_years_ago(f.get("filing_date")), f) for f in funds
                 if f.get("filing_date")]
        dates = [(a, f) for a, f in dates if a is not None]
        if dates:
            newest_age, newest = min(dates, key=lambda x: x[0])
            fired = newest_age >= th["s1"]["age_years"]
            out["s1"] = {
                "fired": fired,
                "evidence": (f"Newest fund '{newest['name']}' is "
                             f"{newest_age:.1f} yrs old (threshold "
                             f"{th['s1']['age_years']}); no younger filing."),
                "link": newest.get("edgar_url"),
            }
        else:
            out["s1"] = {"fired": False, "evidence": "No fund dates on file.",
                         "link": None}

    # ── Signal 2: ADV decline pattern ────────────────────────────────
    if settings["s2"]["enabled"]:
        adv = bundle["adv"]
        fired, why = False, []
        if len(adv) >= 2:
            first, last = adv[0], adv[-1]
            yrs = _years_ago(first["snapshot_date"]) or 0
            if (th["s2"].get("use_raum") and first.get("raum") and
                    last.get("raum") and yrs and
                    yrs <= th["s2"]["raum_years"] + 1):
                drop = (first["raum"] - last["raum"]) / first["raum"] * 100
                if drop >= th["s2"]["raum_pct"]:
                    fired = True
                    why.append(f"RAUM down {drop:.0f}%")
            if (th["s2"].get("use_emp") and first.get("employees") and
                    last.get("employees")):
                drop = ((first["employees"] - last["employees"])
                        / first["employees"] * 100)
                if drop >= th["s2"]["emp_pct"]:
                    fired = True
                    why.append(f"Headcount down {drop:.0f}%")
        if adv and th["s2"].get("use_age"):
            try:
                fund_rows = json.loads(adv[-1].get("funds_json") or "[]")
            except (ValueError, TypeError):
                fund_rows = []
            ages = [_years_ago(f.get("inception")) for f in fund_rows]
            ages = [a for a in ages if a is not None]
            if ages and min(ages) >= th["s2"]["fund_age"]:
                fired = True
                why.append(f"Oldest ADV fund {max(ages):.0f} yrs, "
                           f"no younger fund")
        out["s2"] = {"fired": fired,
                     "evidence": "; ".join(why) if why else
                     ("No ADV data imported yet." if not adv else
                      "No decline pattern detected."),
                     "link": None}

    # ── Signal 3: stale GP website (Wayback evidence) ────────────────
    if settings["s3"]["enabled"]:
        wb = bundle["wayback_gp"]
        if wb:
            latest = wb[0]
            age = _years_ago(latest.get("last_change_date"))
            fired = age is not None and age >= th["s3"]["stale_years"]
            out["s3"] = {
                "fired": fired,
                "evidence": (f"Site last meaningfully changed "
                             f"{latest.get('last_change_date', '?')[:10]} "
                             f"({age:.1f} yrs ago)." if age is not None
                             else "Wayback check inconclusive."),
                "link": latest.get("snapshot_url"),
            }
        else:
            out["s3"] = {"fired": False,
                         "evidence": "No Wayback check run yet.", "link": None}

    # ── Signal 4: companies held too long ────────────────────────────
    if settings["s4"]["enabled"]:
        over = []
        for co in companies:
            age = _years_ago(co.get("acquisition_date"))
            if age is not None and age >= th["s4"]["hold_years"]:
                over.append((co["name"], age))
        out["s4"] = {
            "fired": bool(over),
            "evidence": (", ".join(f"{n} ({a:.0f} yrs)" for n, a in over)
                         if over else
                         f"No company held ≥ {th['s4']['hold_years']} yrs "
                         f"({len(companies)} companies tracked)."),
            "link": None,
        }

    # ── Signal 5: team decay (manual LinkedIn checklist) ─────────────
    if settings["s5"]["enabled"]:
        cur, peak = gp.get("li_current_headcount"), gp.get("li_peak_headcount")
        junior = gp.get("li_junior_hire_recent")
        fired, why = False, []
        if cur and peak and peak > 0:
            drop = (peak - cur) / peak * 100
            if drop >= th["s5"]["decline_pct"]:
                fired = True
                why.append(f"Headcount down {drop:.0f}% from peak")
        if junior == 0:
            fired = True
            why.append(f"No junior hire in {th['s5']['hire_years']} yrs")
        out["s5"] = {"fired": fired,
                     "evidence": "; ".join(why) if why else
                     "No decay recorded in the checklist.",
                     "link": gp.get("linkedin_url")}

    # ── Signal 6: pension performance (verified) ─────────────────────
    if settings["s6"]["enabled"]:
        hits = []
        for p in bundle["pension"]:
            if (p.get("vintage_year") and
                    p["vintage_year"] <= th["s6"]["vintage_max"] and
                    p.get("dpi") is not None and
                    p["dpi"] < th["s6"]["dpi_max"] and
                    p.get("nav") is not None and
                    p["nav"] >= th["s6"]["nav_floor_m"] * 1_000_000):
                hits.append(p)
        out["s6"] = {
            "fired": bool(hits),
            "evidence": ("; ".join(
                f"{p['fund_name']} v{p['vintage_year']} DPI {p['dpi']:.2f} "
                f"NAV ${p['nav']/1e6:.0f}M ({p['source']})" for p in hits)
                if hits else "No confirmed pension rows match."),
            "link": None,
            "verified": bool(hits),
        }

    # ── Signal 7: no exits in N years ────────────────────────────────
    if settings["s7"]["enabled"]:
        last_exit = gp.get("last_exit_date")
        age = _years_ago(last_exit)
        if age is not None:
            fired = age >= th["s7"]["exit_years"]
            ev = f"Last confirmed exit {last_exit[:10]} ({age:.1f} yrs ago)."
        else:
            # never recorded an exit AND the fund is old → fires
            fund_ages = [_years_ago(f.get("filing_date")) for f in funds]
            fund_ages = [a for a in fund_ages if a is not None]
            old_fund = bool(fund_ages) and min(fund_ages) >= \
                settings["s1"]["thresholds"]["age_years"]
            fired = old_fund
            ev = ("No exit ever recorded and youngest fund is past the "
                  "age threshold." if old_fund
                  else "No exit recorded; fund age unknown or young.")
        out["s7"] = {"fired": fired, "evidence": ev, "link": None}

    # ── Signal 8: portfolio company decay ────────────────────────────
    if settings["s8"]["enabled"]:
        wb_by_co = {}
        for w in bundle["wayback_co"]:
            wb_by_co.setdefault(w["company_id"], w)
        decaying = []
        for co in companies:
            w = wb_by_co.get(co["id"])
            stale = False
            if w:
                a = _years_ago(w.get("last_change_date"))
                stale = a is not None and a >= th["s8"]["stale_years"]
            manual = bool(co.get("decay_exec_departures")) or \
                co.get("decay_job_postings") == 0
            if th["s8"].get("logic", "AND") == "AND":
                hit = stale and manual
            else:
                hit = stale or manual
            if hit:
                decaying.append(co["name"])
        out["s8"] = {
            "fired": bool(decaying),
            "evidence": (f"Decaying: {', '.join(decaying)}" if decaying
                         else "No companies meet the decay test."),
            "link": None,
        }

    # ── Signal 9: provider changes / term extensions ─────────────────
    if settings["s9"]["enabled"]:
        recent = [p for p in bundle["providers"]
                  if (_years_ago(p.get("change_date")) or 99)
                  <= th["s9"]["window_years"]]
        extensions = [f for f in funds if (f.get("term_extension_note") or "").strip()]
        fired = bool(recent) or bool(extensions)
        bits = []
        if recent:
            bits.append("; ".join(
                f"{p['provider_role']}: {p['old_provider']} → {p['new_provider']}"
                for p in recent[:3]))
        if extensions:
            bits.append("; ".join(
                f"{f['name']}: extension recorded" for f in extensions))
        out["s9"] = {"fired": fired,
                     "evidence": " | ".join(bits) if bits else
                     "No provider changes or extensions on file.",
                     "link": None}

    # ── Signal 10: UCC lien activity ─────────────────────────────────
    if settings["s10"]["enabled"]:
        hits = []
        for co in companies:
            recent_filing = (_years_ago(co.get("ucc_last_filing_date")) or 99) \
                <= th["s10"]["window_years"]
            if (co.get("ucc_lender_changed") and recent_filing) or \
                    (co.get("ucc_active_liens") and recent_filing) or \
                    ((co.get("ucc_amendment_count") or 0)
                     > th["s10"]["amendment_max"]):
                hits.append(co["name"])
        out["s10"] = {
            "fired": bool(hits),
            "evidence": (f"UCC activity: {', '.join(hits)}" if hits
                         else "No qualifying UCC findings recorded."),
            "link": None,
        }

    return out


def composite_score(results, settings):
    """0-100 = sum(fired x weight) / sum(enabled weights)."""
    total_weight = sum(v["weight"] for k, v in settings.items()
                       if v["enabled"])
    if not total_weight:
        return 0
    fired_weight = sum(settings[k]["weight"] for k, r in results.items()
                       if r.get("fired"))
    return round(fired_weight / total_weight * 100)


def score_all(settings, min_signals=1):
    """Score every live GP. Returns rows for the dashboard, ranked."""
    conn = connect()
    try:
        gps = [dict(r) for r in conn.execute(
            "SELECT * FROM gps WHERE killed = 0").fetchall()]
        rows = []
        for gp in gps:
            bundle = _gp_bundle(conn, gp["id"])
            results = evaluate_gp(gp, bundle, settings)
            fired = [k for k, r in results.items() if r.get("fired")]
            score = composite_score(results, settings)
            verified = any(r.get("verified") for r in results.values())
            rows.append({"gp": gp, "results": results, "score": score,
                         "fired": fired, "verified": verified})
        # candidate pool = fired at least min_signals enabled signals
        pool = [r for r in rows if len(r["fired"]) >= min_signals]
        pool.sort(key=lambda r: r["score"], reverse=True)
        return pool, rows
    finally:
        conn.close()
