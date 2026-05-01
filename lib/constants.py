"""Constants and configuration shared across the NCP sourcing pipeline.

This module holds pure data (no Streamlit, no API clients). Anything that
needs an API key or a live client should go in lib/api_clients.py instead.
"""

# ---------------------------------------------------------------------------
# MODEL
# ---------------------------------------------------------------------------
OPENAI_MODEL = "gpt-4o"

# ---------------------------------------------------------------------------
# APOLLO INDUSTRIES
# ---------------------------------------------------------------------------
APOLLO_INDUSTRIES = [
    "Accounting", "Airlines/Aviation", "Alternative Dispute Resolution", "Alternative Medicine",
    "Animation", "Apparel & Fashion", "Architecture & Planning", "Arts and Crafts", "Automotive",
    "Aviation & Aerospace", "Banking", "Biotechnology", "Broadcast Media", "Building Materials",
    "Business Supplies and Equipment", "Capital Markets", "Chemicals", "Civic & Social Organization",
    "Civil Engineering", "Commercial Real Estate", "Computer & Network Security", "Computer Games",
    "Computer Hardware", "Computer Networking", "Computer Software", "Construction",
    "Consumer Electronics", "Consumer Goods", "Consumer Services", "Cosmetics", "Dairy",
    "Defense & Space", "Design", "Education Management", "E-Learning",
    "Electrical/Electronic Manufacturing", "Entertainment", "Environmental Services",
    "Events Services", "Executive Office", "Facilities Services", "Farming", "Financial Services",
    "Fine Art", "Food & Beverages", "Food Production", "Fund-Raising", "Furniture",
    "Gambling & Casinos", "Glass, Ceramics & Concrete", "Government Administration",
    "Government Relations", "Graphic Design", "Health, Wellness and Fitness", "Higher Education",
    "Hospital & Health Care", "Hospitality", "Human Resources", "Import and Export",
    "Individual & Family Services", "Industrial Automation", "Information Services",
    "Information Technology and Services", "Insurance", "International Affairs",
    "International Trade and Development", "Internet", "Investment Banking", "Investment Management",
    "Judiciary", "Law Enforcement", "Law Practice", "Legal Services", "Legislative Office",
    "Leisure, Travel & Tourism", "Libraries", "Logistics and Supply Chain",
    "Luxury Goods & Jewelry", "Machinery", "Management Consulting", "Maritime", "Market Research",
    "Marketing and Advertising", "Mechanical or Industrial Engineering", "Media Production",
    "Medical Devices", "Medical Practice", "Mental Health Care", "Military", "Mining & Metals",
    "Motion Pictures and Film", "Museums and Institutions", "Music", "Nanotechnology", "Newspapers",
    "Non-Profit Organization Management", "Oil & Energy", "Online Media", "Outsourcing/Offshoring",
    "Package/Freight Delivery", "Packaging and Containers", "Paper & Forest Products",
    "Performing Arts", "Pharmaceuticals", "Philanthropy", "Photography", "Plastics",
    "Political Organization", "Primary/Secondary Education", "Printing",
    "Professional Training & Coaching", "Program Development", "Public Policy",
    "Public Relations and Communications", "Public Safety", "Publishing", "Railroad Manufacture",
    "Ranching", "Real Estate", "Recreational Facilities and Services", "Religious Institutions",
    "Renewables & Environment", "Research", "Restaurants", "Retail", "Security and Investigations",
    "Semiconductors", "Shipbuilding", "Sporting Goods", "Sports", "Staffing and Recruiting",
    "Supermarkets", "Telecommunications", "Textiles", "Think Tanks", "Tobacco",
    "Translation and Localization", "Transportation/Trucking/Railroad", "Utilities",
    "Venture Capital & Private Equity", "Veterinary", "Warehousing", "Wholesale",
    "Wine and Spirits", "Wireless", "Writing and Editing",
]

# ---------------------------------------------------------------------------
# TITLE SCORES (used to rank discovered contacts by seniority)
# ---------------------------------------------------------------------------
_TITLE_SCORES = {
    "owner": 100, "founder": 95, "co-founder": 90,
    "chief executive": 95, " ceo": 95,
    "president": 90, "managing partner": 88, "managing member": 88,
    "managing director": 85, "principal": 85,
    "executive director": 82, "medical director": 80,
    "chief operating": 75, " coo": 75,
    "chief financial": 70, " cfo": 70,
    "chief medical": 80, "chief clinical": 78, "chief nursing": 75,
    "administrator": 70, "director of": 60,
    "vice president": 50, " vp ": 50,
    "manager": 30,
}

# ---------------------------------------------------------------------------
# CONTACT PATHS (URL paths commonly used for team/leadership pages)
# ---------------------------------------------------------------------------
_CONTACT_PATHS = [
    "/about", "/about-us", "/team", "/our-team", "/leadership",
    "/staff", "/management", "/who-we-are", "/meet-the-team",
    "/people", "/executives", "/administration", "/about/team",
    "/about/leadership", "/contact", "/contact-us",
    "/our-staff", "/board", "/board-of-directors", "/leadership-team",
    "/meet-our-team", "/staff-directory", "/our-leadership", "/directors",
    "/about/staff", "/about/leadership-team", "/about/administration",
]

# ---------------------------------------------------------------------------
# DEFAULT HTTP USER AGENT (used as a fallback if not provided via secrets)
# ---------------------------------------------------------------------------
DEFAULT_HTTP_USER_AGENT = (
    "Mozilla/5.0 (compatible; NCPSourcingBot/1.0; +https://newcapitalpartners.com)"
)