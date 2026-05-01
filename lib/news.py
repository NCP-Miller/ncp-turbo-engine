"""News fetching helpers.

Fetches the latest news headline + link for a company from Google News RSS.
Pure functions — no Streamlit, no global state. Pass user_agent in.
"""

import requests
import xml.etree.ElementTree as ET
from urllib.parse import quote_plus

from lib.constants import DEFAULT_HTTP_USER_AGENT


def get_latest_news_link(company_name, city=None, user_agent=None):
    """Return (title, link) for the latest Google News result for a company.

    Returns (None, None) on any error or empty result.
    """
    ua = user_agent or DEFAULT_HTTP_USER_AGENT
    q = f"{company_name} {city}" if city else company_name
    rss = (
        f"https://news.google.com/rss/search?q={quote_plus(q)}"
        f"&hl=en-US&gl=US&ceid=US:en"
    )
    try:
        r = requests.get(rss, timeout=10, headers={"User-Agent": ua})
        if r.status_code != 200:
            return None, None
        root = ET.fromstring(r.content)
        items = root.findall("./channel/item")
        if not items:
            return None, None
        title = (items[0].findtext("title") or "").strip()
        link = (items[0].findtext("link") or "").strip()
        return title, link
    except Exception:
        return None, None