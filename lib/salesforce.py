"""Salesforce integration — create Accounts + Contacts from sourcing results.

Authentication uses Connected App OAuth (username-password flow) with
consumer key/secret, which works with MFA-enabled orgs when the Connected
App has 'Relax IP restrictions' set.
"""

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
