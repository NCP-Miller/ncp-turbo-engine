"""Outreach email templates with merge fields.

Merge fields available: {first_name}, {firm_name}, {portfolio_company},
{fund_vintage} — filled from the database for whichever GP you pick.
Rendered output is shown in a copy-ready block; you send from Outlook.
"""

from zfs.db import connect, now


def list_templates():
    conn = connect()
    try:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM templates ORDER BY name").fetchall()]
    finally:
        conn.close()


def save_template(name, subject, body, template_id=None):
    conn = connect()
    try:
        if template_id:
            conn.execute(
                "UPDATE templates SET name = ?, subject = ?, body = ?, "
                "updated_at = ? WHERE id = ?",
                (name, subject, body, now(), template_id))
        else:
            conn.execute(
                "INSERT INTO templates (name, subject, body, created_at, "
                "updated_at) VALUES (?, ?, ?, ?, ?)",
                (name, subject, body, now(), now()))
        conn.commit()
    finally:
        conn.close()


def delete_template(template_id):
    conn = connect()
    try:
        conn.execute("DELETE FROM templates WHERE id = ?", (template_id,))
        conn.commit()
    finally:
        conn.close()


def merge_fields_for_gp(gp_id):
    """Build the merge-field values for one GP from the database."""
    conn = connect()
    try:
        gp = conn.execute("SELECT * FROM gps WHERE id = ?", (gp_id,)).fetchone()
        contact = conn.execute(
            "SELECT * FROM contacts WHERE gp_id = ? "
            "ORDER BY preferred DESC, id LIMIT 1", (gp_id,)).fetchone()
        company = conn.execute(
            "SELECT * FROM portfolio_companies WHERE gp_id = ? "
            "ORDER BY id LIMIT 1", (gp_id,)).fetchone()
        fund = conn.execute(
            "SELECT * FROM funds WHERE gp_id = ? "
            "ORDER BY filing_date LIMIT 1", (gp_id,)).fetchone()
    finally:
        conn.close()
    first_name = ""
    if contact and contact["name"]:
        first_name = contact["name"].split()[0]
    vintage = ""
    if fund and fund["filing_date"]:
        vintage = str(fund["filing_date"])[:4]
    return {
        "first_name": first_name or "[first_name]",
        "firm_name": gp["name"] if gp else "[firm_name]",
        "portfolio_company": (company["name"] if company
                              else "[portfolio_company]"),
        "fund_vintage": vintage or "[fund_vintage]",
    }


def render(template_body, fields):
    out = template_body
    for k, v in fields.items():
        out = out.replace("{" + k + "}", str(v))
    return out
