"""Salesforce integration — create Accounts + Contacts from sourcing results.

Authentication uses Connected App OAuth (username-password flow) with
consumer key/secret, which works with MFA-enabled orgs when the Connected
App has 'Relax IP restrictions' set.
"""

from datetime import date, timedelta
from simple_salesforce import Salesforce, SalesforceAuthenticationFailed


def sf_login(username, password, consumer_key, consumer_secret,
             security_token="", domain="login"):
    """Authenticate to Salesforce via Connected App OAuth.

    Returns a Salesforce client on success, raises on failure.
    """
    return Salesforce(
        username=username,
        password=password,
        security_token=security_token,
        consumer_key=consumer_key,
        consumer_secret=consumer_secret,
        domain=domain,
    )


def create_account(sf, row):
    """Create a Salesforce Account from a sourcing results row dict.

    Returns the new Account Id.
    """
    payload = {
        "Name": row.get("Company", "Unknown"),
        "Website": row.get("Website") or None,
        "Description": row.get("Description") or None,
        "BillingCity": row.get("City") or None,
        "BillingState": row.get("State") or None,
        "NumberOfEmployees": row.get("Employees") if row.get("Employees") else None,
        "Industry": "Other",
    }
    payload = {k: v for k, v in payload.items() if v is not None}
    result = sf.Account.create(payload)
    return result["id"]


def create_contact(sf, account_id, row):
    """Create a Salesforce Contact linked to an Account.

    Splits CEO/Owner Name into first/last. Returns the new Contact Id.
    """
    full_name = row.get("CEO/Owner Name", "").strip()
    parts = full_name.split(None, 1) if full_name and full_name != "N/A" else []
    first = parts[0] if len(parts) >= 1 else "Unknown"
    last = parts[1] if len(parts) >= 2 else (first if first != "Unknown" else "Contact")
    if len(parts) == 1:
        first = parts[0]
        last = parts[0]

    email = row.get("Email")
    if email == "N/A":
        email = row.get("Email Estimate") or None
    phone = row.get("Phone")
    if phone == "N/A":
        phone = None

    payload = {
        "AccountId": account_id,
        "FirstName": first,
        "LastName": last,
        "Title": row.get("Title") if row.get("Title") != "N/A" else None,
        "Email": email,
        "Phone": phone,
    }
    payload = {k: v for k, v in payload.items() if v is not None}
    result = sf.Contact.create(payload)
    return result["id"]


def push_to_salesforce(sf, row):
    """Create Account + Contact for a sourcing result. Returns (account_id, contact_id)."""
    account_id = create_account(sf, row)
    contact_id = create_contact(sf, account_id, row)
    return account_id, contact_id


def find_existing_account(sf, company_name):
    """Check if an Account with this name already exists. Returns Account Id or None."""
    safe_name = company_name.replace("'", "\\'")
    result = sf.query(f"SELECT Id FROM Account WHERE Name = '{safe_name}' LIMIT 1")
    if result["totalSize"] > 0:
        return result["records"][0]["Id"]
    return None


def find_contact_for_account(sf, account_id):
    """Find the first Contact linked to an Account. Returns Contact Id or None."""
    result = sf.query(
        f"SELECT Id FROM Contact WHERE AccountId = '{account_id}' LIMIT 1"
    )
    if result["totalSize"] > 0:
        return result["records"][0]["Id"]
    return None


def log_outreach_activity(sf, account_id, contact_id, subject, body):
    """Create a completed Task on the Account/Contact to log outreach.

    Returns the new Task Id.
    """
    payload = {
        "WhatId": account_id,
        "WhoId": contact_id,
        "Subject": f"Email: {subject}" if subject else "Outreach Email Sent",
        "Description": body or "",
        "Status": "Completed",
        "Priority": "Normal",
        "Type": "Email",
        "ActivityDate": None,
    }
    payload = {k: v for k, v in payload.items() if v is not None}
    result = sf.Task.create(payload)
    return result["id"]


_SF_TASK_TYPES = {
    "Call": "Call", "Email": "Email", "Meeting": "Meeting",
    "LinkedIn": "Other", "Text": "Other", "Note": "Other",
}


def sync_deal_to_salesforce(sf, deal, activities):
    """Mirror a Deal Tracker record into Salesforce.

    Ensures the Account (and Contact when we have a name) exists, then
    pushes each un-synced activity as a completed Task and logs the
    current tracker status/notes as one summary Task.

    Args:
        sf: authenticated Salesforce client.
        deal: deal dict from lib.crm (company, status, notes, sf ids, row_json...).
        activities: list of un-synced activity dicts from lib.crm.

    Returns (account_id, contact_id, synced_activity_ids).
    """
    import json as _json

    company = deal.get("company", "Unknown")
    row = {}
    if deal.get("row_json"):
        try:
            row = _json.loads(deal["row_json"])
        except (ValueError, TypeError):
            row = {}

    account_id = deal.get("sf_account_id") or find_existing_account(sf, company)
    if not account_id:
        account_id = create_account(sf, row or {
            "Company": company,
            "Website": deal.get("website"),
            "City": deal.get("city"),
            "State": deal.get("state"),
        })

    contact_id = deal.get("sf_contact_id") or find_contact_for_account(sf, account_id)
    if not contact_id and (deal.get("contact_name") or row.get("CEO/Owner Name")):
        try:
            contact_id = create_contact(sf, account_id, row or {
                "CEO/Owner Name": deal.get("contact_name", ""),
                "Title": deal.get("title"),
                "Email": deal.get("email"),
                "Phone": deal.get("phone"),
            })
        except Exception:
            contact_id = None

    synced_ids = []
    for act in activities:
        ts = (act.get("timestamp") or "")[:10]
        _desc = act.get("detail") or act.get("summary", "")
        if act.get("user"):
            _desc = f"{_desc}\n(Logged by {act['user']})"
        payload = {
            "WhatId": account_id,
            "WhoId": contact_id,
            "Subject": f"NCP Tracker — {act.get('type', 'Note')}: {act.get('summary', '')[:200]}",
            "Description": _desc,
            "Status": "Completed",
            "Priority": "Normal",
            "Type": _SF_TASK_TYPES.get(act.get("type"), "Other"),
            "ActivityDate": ts or None,
        }
        payload = {k: v for k, v in payload.items() if v is not None}
        sf.Task.create(payload)
        synced_ids.append(act["id"])

    # Status + notes — ONE summary Task per Account, updated in place so
    # frequent auto-syncs don't pile up duplicate records.
    status_subject = f"NCP Tracker - Status: {deal.get('status', 'New')}"
    status_desc = (
        f"Deal Tracker sync on {date.today().isoformat()}.\n"
        f"Status: {deal.get('status', 'New')}\n"
        f"Notes: {deal.get('notes') or '(none)'}"
    )
    try:
        existing_status = sf.query(
            f"SELECT Id FROM Task WHERE WhatId = '{account_id}' "
            f"AND Subject LIKE 'NCP Tracker - Status:%' LIMIT 1"
        )
        if existing_status["totalSize"] > 0:
            sf.Task.update(existing_status["records"][0]["Id"], {
                "Subject": status_subject,
                "Description": status_desc,
            })
        else:
            payload = {
                "WhatId": account_id,
                "WhoId": contact_id,
                "Subject": status_subject,
                "Description": status_desc,
                "Status": "Completed",
                "Priority": "Normal",
                "Type": "Other",
            }
            sf.Task.create({k: v for k, v in payload.items() if v is not None})
    except Exception:
        pass

    # Follow-up date — ONE open Task per Account with the due date, updated
    # in place whenever the tracker's next-follow-up changes.
    nf = (deal.get("next_followup") or "")[:10]
    if nf:
        fu_desc = (
            f"Follow up with {company} (from NCP Deal Tracker).\n"
            f"Status: {deal.get('status', 'New')}\n"
            f"Notes: {deal.get('notes') or '(none)'}"
        )
        try:
            existing_fu = sf.query(
                f"SELECT Id FROM Task WHERE WhatId = '{account_id}' "
                f"AND Subject LIKE 'NCP Tracker - Follow up%' "
                f"AND Status = 'Not Started' LIMIT 1"
            )
            if existing_fu["totalSize"] > 0:
                sf.Task.update(existing_fu["records"][0]["Id"], {
                    "ActivityDate": nf,
                    "Description": fu_desc,
                })
            else:
                payload = {
                    "WhatId": account_id,
                    "WhoId": contact_id,
                    "Subject": f"NCP Tracker - Follow up: {company}",
                    "Description": fu_desc,
                    "Status": "Not Started",
                    "Priority": "High",
                    "Type": "Call",
                    "ActivityDate": nf,
                }
                sf.Task.create({k: v for k, v in payload.items() if v is not None})
        except Exception:
            pass

    return account_id, contact_id, synced_ids


def recent_activity_accounts(sf, days=14):
    """Find Accounts with Task activity logged in the last N days.

    Returns a list of dicts with account details, the primary contact,
    and the most recent activity's subject/date — used by the Deal
    Tracker's import to pull in companies being actively worked in
    Salesforce.
    """
    tasks = sf.query_all(
        f"SELECT WhatId, Subject, LastModifiedDate FROM Task "
        f"WHERE WhatId != null AND LastModifiedDate = LAST_N_DAYS:{int(days)} "
        f"ORDER BY LastModifiedDate DESC"
    )
    latest_by_account = {}
    for t in tasks.get("records", []):
        wid = t.get("WhatId") or ""
        if not wid.startswith("001"):        # Accounts only
            continue
        if wid not in latest_by_account:     # records arrive newest-first
            latest_by_account[wid] = {
                "subject": t.get("Subject") or "Activity",
                "when": (t.get("LastModifiedDate") or "")[:10],
            }
    if not latest_by_account:
        return []

    results = []
    ids = list(latest_by_account)
    for i in range(0, len(ids), 100):        # chunk the IN clauses
        chunk = ids[i:i + 100]
        id_list = ",".join(f"'{a}'" for a in chunk)
        accounts = sf.query_all(
            f"SELECT Id, Name, Website, BillingCity, BillingState, "
            f"NumberOfEmployees, Description FROM Account "
            f"WHERE Id IN ({id_list})"
        )
        contacts_by_account = {}
        contacts = sf.query_all(
            f"SELECT Id, FirstName, LastName, Title, Email, Phone, "
            f"AccountId FROM Contact WHERE AccountId IN ({id_list})"
        )
        for c in contacts.get("records", []):
            contacts_by_account.setdefault(c.get("AccountId"), c)
        for a in accounts.get("records", []):
            act = latest_by_account.get(a["Id"], {})
            c = contacts_by_account.get(a["Id"]) or {}
            contact_name = " ".join(
                x for x in [c.get("FirstName"), c.get("LastName")] if x)
            results.append({
                "account_id": a["Id"],
                "name": a.get("Name") or "",
                "website": a.get("Website"),
                "city": a.get("BillingCity"),
                "state": a.get("BillingState"),
                "employees": a.get("NumberOfEmployees"),
                "description": a.get("Description"),
                "contact_id": c.get("Id"),
                "contact_name": contact_name,
                "contact_title": c.get("Title"),
                "contact_email": c.get("Email"),
                "contact_phone": c.get("Phone"),
                "last_activity_subject": act.get("subject", ""),
                "last_activity_date": act.get("when", ""),
            })
    return results


def create_followup_tasks(sf, account_id, contact_id, company_name):
    """Create two open follow-up Tasks after initial outreach:
      1. Phone call — due 1 day from now
      2. Follow-up email — due 3 days from now

    Returns (call_task_id, email_task_id).
    """
    today = date.today()

    call_payload = {
        "WhatId": account_id,
        "WhoId": contact_id,
        "Subject": f"Follow-up call: {company_name}",
        "Description": (
            f"Call the contact at {company_name} to follow up on the "
            f"outreach email sent on {today.isoformat()}."
        ),
        "Status": "Not Started",
        "Priority": "High",
        "Type": "Call",
        "ActivityDate": (today + timedelta(days=1)).isoformat(),
    }
    call_payload = {k: v for k, v in call_payload.items() if v is not None}
    call_result = sf.Task.create(call_payload)

    email_payload = {
        "WhatId": account_id,
        "WhoId": contact_id,
        "Subject": f"Follow-up email: {company_name}",
        "Description": (
            f"Send a follow-up email to the contact at {company_name}. "
            f"Initial outreach was sent on {today.isoformat()}, "
            f"follow-up call was scheduled for {(today + timedelta(days=1)).isoformat()}."
        ),
        "Status": "Not Started",
        "Priority": "Normal",
        "Type": "Email",
        "ActivityDate": (today + timedelta(days=3)).isoformat(),
    }
    email_payload = {k: v for k, v in email_payload.items() if v is not None}
    email_result = sf.Task.create(email_payload)

    return call_result["id"], email_result["id"]
