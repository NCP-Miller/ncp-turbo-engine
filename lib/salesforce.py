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
