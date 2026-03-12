import requests
import xml.etree.ElementTree as ET
from urllib.parse import quote_plus
from config import HTTP_USER_AGENT


def get_latest_news_link(company_name, city=None):
    q   = f"{company_name} {city}" if city else company_name
    rss = f"https://news.google.com/rss/search?q={quote_plus(q)}&hl=en-US&gl=US&ceid=US:en"
    try:
        r = requests.get(rss, timeout=10, headers={"User-Agent": HTTP_USER_AGENT})
        if r.status_code != 200: return None, None
        root  = ET.fromstring(r.content)
        items = root.findall("./channel/item")
        if not items: return None, None
        return (items[0].findtext("title") or "").strip(), \
               (items[0].findtext("link")  or "").strip()
    except:
        return None, None
