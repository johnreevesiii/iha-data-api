"""
Grant Discovery & Eligibility Matching Engine

Data sources:
  - grants.gov API (federal opportunities)
  - HRSA NOFO curated database (health-specific)
  - Tribal/IHS-specific grant catalog (built-in)

Matching logic:
  HPSA status + MUA status + tribal status + rural classification
  → scored eligibility against each grant's requirements.
"""

import logging
import requests
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

log = logging.getLogger("iha.grants")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# BUILT-IN TRIBAL HEALTH GRANT CATALOG
# These are well-known recurring federal grants relevant
# to tribal health organizations. Updated manually.
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

TRIBAL_GRANT_CATALOG = [
    {
        "id": "HRSA-330",
        "title": "HRSA Health Center Program (Section 330)",
        "agency": "HRSA",
        "category": "Primary Care",
        "description": "Funding for community health centers to provide primary care services to underserved populations. Tribal and Urban Indian health centers are eligible.",
        "typical_award": "$500,000 - $5,000,000",
        "cycle": "Annual",
        "url": "https://bphc.hrsa.gov/funding/funding-opportunities",
        "eligibility_tags": ["tribal", "fqhc", "underserved", "hpsa"],
        "requires_hpsa": False,
        "requires_tribal": False,
        "boost_if_hpsa": True,
        "boost_if_tribal": True,
        "boost_if_rural": True,
        "boost_if_mua": True,
    },
    {
        "id": "IHS-IHCIF",
        "title": "IHS Indian Health Care Improvement Fund",
        "agency": "IHS",
        "category": "Tribal Health",
        "description": "Supplements IHS appropriations to address disparities in healthcare resources among IHS and tribal health programs.",
        "typical_award": "$100,000 - $2,000,000",
        "cycle": "Annual",
        "url": "https://www.ihs.gov/dgm/funding/",
        "eligibility_tags": ["tribal", "638", "ihs"],
        "requires_hpsa": False,
        "requires_tribal": True,
        "boost_if_hpsa": True,
        "boost_if_tribal": True,
        "boost_if_rural": False,
        "boost_if_mua": True,
    },
    {
        "id": "HRSA-NHSC",
        "title": "National Health Service Corps (NHSC)",
        "agency": "HRSA",
        "category": "Workforce",
        "description": "Loan repayment and scholarships for health professionals who serve in HPSAs. IHS and tribal sites are eligible.",
        "typical_award": "$50,000 - $75,000 per provider",
        "cycle": "Rolling",
        "url": "https://nhsc.hrsa.gov/",
        "eligibility_tags": ["hpsa", "workforce", "tribal"],
        "requires_hpsa": True,
        "requires_tribal": False,
        "boost_if_hpsa": True,
        "boost_if_tribal": True,
        "boost_if_rural": True,
        "boost_if_mua": False,
    },
    {
        "id": "SAMHSA-TBHPP",
        "title": "Tribal Behavioral Health Grants",
        "agency": "SAMHSA",
        "category": "Behavioral Health",
        "description": "Grants for federally recognized tribes and tribal organizations to address substance abuse and mental health.",
        "typical_award": "$200,000 - $1,000,000",
        "cycle": "Annual",
        "url": "https://www.samhsa.gov/grants/grant-announcements-by-organization",
        "eligibility_tags": ["tribal", "behavioral_health", "substance_abuse"],
        "requires_hpsa": False,
        "requires_tribal": True,
        "boost_if_hpsa": True,
        "boost_if_tribal": True,
        "boost_if_rural": False,
        "boost_if_mua": False,
    },
    {
        "id": "USDA-DLT",
        "title": "USDA Distance Learning & Telemedicine Grants",
        "agency": "USDA",
        "category": "Telehealth",
        "description": "Funds equipment and infrastructure for telehealth services in rural communities. Tribal entities are eligible.",
        "typical_award": "$50,000 - $1,000,000",
        "cycle": "Annual",
        "url": "https://www.usda.gov/topics/rural/telehealth",
        "eligibility_tags": ["rural", "telehealth", "tribal"],
        "requires_hpsa": False,
        "requires_tribal": False,
        "boost_if_hpsa": False,
        "boost_if_tribal": True,
        "boost_if_rural": True,
        "boost_if_mua": False,
    },
    {
        "id": "FCC-RHC",
        "title": "FCC Rural Health Care Program",
        "agency": "FCC",
        "category": "Telehealth",
        "description": "Subsidized broadband for rural healthcare providers. Covers up to 65% of eligible costs for internet connectivity.",
        "typical_award": "Up to 65% subsidy",
        "cycle": "Rolling",
        "url": "https://www.fcc.gov/general/rural-health-care-program",
        "eligibility_tags": ["rural", "telehealth", "broadband"],
        "requires_hpsa": False,
        "requires_tribal": False,
        "boost_if_hpsa": False,
        "boost_if_tribal": True,
        "boost_if_rural": True,
        "boost_if_mua": False,
    },
    {
        "id": "HRSA-RCORP",
        "title": "Rural Communities Opioid Response Program (RCORP)",
        "agency": "HRSA",
        "category": "Behavioral Health",
        "description": "Supports prevention, treatment, and recovery for substance use disorders in rural communities.",
        "typical_award": "$200,000 - $1,000,000",
        "cycle": "Annual",
        "url": "https://www.hrsa.gov/rural-health/rcorp",
        "eligibility_tags": ["rural", "behavioral_health", "substance_abuse"],
        "requires_hpsa": False,
        "requires_tribal": False,
        "boost_if_hpsa": True,
        "boost_if_tribal": True,
        "boost_if_rural": True,
        "boost_if_mua": True,
    },
    {
        "id": "ACF-SDPI",
        "title": "Special Diabetes Program for Indians (SDPI)",
        "agency": "IHS",
        "category": "Chronic Disease",
        "description": "Grants for diabetes prevention and treatment programs in AI/AN communities. One of the most successful tribal health programs.",
        "typical_award": "$100,000 - $500,000",
        "cycle": "Multi-year",
        "url": "https://www.ihs.gov/sdpi/",
        "eligibility_tags": ["tribal", "diabetes", "chronic_disease"],
        "requires_hpsa": False,
        "requires_tribal": True,
        "boost_if_hpsa": False,
        "boost_if_tribal": True,
        "boost_if_rural": False,
        "boost_if_mua": False,
    },
    {
        "id": "CDC-GHWIC",
        "title": "CDC Good Health and Wellness in Indian Country",
        "agency": "CDC",
        "category": "Prevention",
        "description": "Supports chronic disease prevention and health promotion in tribal communities. Focus on commercial tobacco use, physical activity, nutrition.",
        "typical_award": "$200,000 - $800,000",
        "cycle": "Multi-year (5-year)",
        "url": "https://www.cdc.gov/tribal/tribes-and-organizations/good-health-wellness.html",
        "eligibility_tags": ["tribal", "prevention", "chronic_disease"],
        "requires_hpsa": False,
        "requires_tribal": True,
        "boost_if_hpsa": False,
        "boost_if_tribal": True,
        "boost_if_rural": False,
        "boost_if_mua": False,
    },
    {
        "id": "HRSA-MCHB",
        "title": "Maternal & Child Health Block Grant (Title V)",
        "agency": "HRSA",
        "category": "Maternal Health",
        "description": "Funding for maternal and child health services. States allocate funds to underserved areas; tribal organizations can receive direct funding.",
        "typical_award": "Varies by state allocation",
        "cycle": "Annual",
        "url": "https://mchb.hrsa.gov/programs-impact/title-v-maternal-child-health-mch-block-grant",
        "eligibility_tags": ["maternal", "child_health", "tribal"],
        "requires_hpsa": False,
        "requires_tribal": False,
        "boost_if_hpsa": True,
        "boost_if_tribal": True,
        "boost_if_rural": True,
        "boost_if_mua": True,
    },
    {
        "id": "HRSA-SUD",
        "title": "HRSA Rural Maternal & Obstetrics Management Strategies",
        "agency": "HRSA",
        "category": "Maternal Health",
        "description": "Improving access to maternal health services in rural communities. Focus on reducing maternal mortality and morbidity.",
        "typical_award": "$200,000 - $500,000",
        "cycle": "Annual",
        "url": "https://www.hrsa.gov/rural-health/rmoms",
        "eligibility_tags": ["rural", "maternal", "access"],
        "requires_hpsa": False,
        "requires_tribal": False,
        "boost_if_hpsa": True,
        "boost_if_tribal": True,
        "boost_if_rural": True,
        "boost_if_mua": True,
    },
    {
        "id": "GRANTS-GOV-TRIBAL",
        "title": "Grants.gov Tribal Set-Aside Opportunities",
        "agency": "Multiple",
        "category": "Various",
        "description": "Federal grants with tribal set-asides or tribal-specific eligibility. Search grants.gov with 'tribal' keyword for current opportunities.",
        "typical_award": "Varies",
        "cycle": "Ongoing",
        "url": "https://www.grants.gov/search-grants?keywords=tribal%20health",
        "eligibility_tags": ["tribal"],
        "requires_hpsa": False,
        "requires_tribal": True,
        "boost_if_hpsa": False,
        "boost_if_tribal": True,
        "boost_if_rural": False,
        "boost_if_mua": False,
    },
]


def search_grants_gov(keywords: str = "tribal health", limit: int = 25) -> List[Dict[str, Any]]:
    """Search grants.gov API for open opportunities.

    Returns a list of grant opportunities matching the keywords.
    Falls back to an empty list on failure (API can be flaky).
    """
    try:
        resp = requests.post(
            "https://apply07.grants.gov/grantsws/rest/opportunities/search/",
            json={
                "keyword": keywords,
                "oppStatuses": "forecasted|posted",
                "rows": limit,
                "sortBy": "openDate|desc",
            },
            headers={"Content-Type": "application/json"},
            timeout=15,
        )
        if resp.status_code != 200:
            log.warning("grants.gov API returned %d", resp.status_code)
            return []

        data = resp.json()
        opportunities = data.get("oppHits", [])

        results = []
        for opp in opportunities:
            results.append({
                "id": opp.get("id", ""),
                "opportunity_number": opp.get("number", ""),
                "title": opp.get("title", ""),
                "agency": opp.get("agency", ""),
                "open_date": opp.get("openDate", ""),
                "close_date": opp.get("closeDate", ""),
                "award_ceiling": opp.get("awardCeiling", 0),
                "award_floor": opp.get("awardFloor", 0),
                "url": f"https://www.grants.gov/search-results-detail/{opp.get('id', '')}",
            })
        return results

    except Exception as e:
        log.warning("grants.gov search failed: %s", e)
        return []


def score_eligibility(
    grant: Dict[str, Any],
    is_tribal: bool = False,
    has_hpsa: bool = False,
    has_mua: bool = False,
    is_rural: bool = False,
) -> Dict[str, Any]:
    """Score a grant's eligibility for a given community profile.

    Returns:
        {
            "score": 0-100,
            "eligible": bool,
            "match_reasons": [...],
            "boost_factors": [...],
        }
    """
    score = 50  # base score — the grant exists and is open
    reasons = []
    boosts = []

    # Hard requirements
    if grant.get("requires_tribal") and not is_tribal:
        return {"score": 0, "eligible": False, "match_reasons": ["Requires tribal eligibility"], "boost_factors": []}

    if grant.get("requires_hpsa") and not has_hpsa:
        return {"score": 0, "eligible": False, "match_reasons": ["Requires HPSA designation"], "boost_factors": []}

    # Eligibility boosts
    if grant.get("boost_if_tribal") and is_tribal:
        score += 20
        boosts.append("Tribal organization (strong preference)")
    if grant.get("boost_if_hpsa") and has_hpsa:
        score += 15
        boosts.append("HPSA designation (priority scoring)")
    if grant.get("boost_if_mua") and has_mua:
        score += 10
        boosts.append("MUA/MUP designation")
    if grant.get("boost_if_rural") and is_rural:
        score += 10
        boosts.append("Rural classification (RUCA)")

    # Tag-based matching
    tags = grant.get("eligibility_tags", [])
    if is_tribal and "tribal" in tags:
        reasons.append("Tribal-eligible program")
    if has_hpsa and "hpsa" in tags:
        reasons.append("Serves health professional shortage area")
    if is_rural and "rural" in tags:
        reasons.append("Rural community eligible")

    if not reasons:
        reasons.append("General eligibility")

    return {
        "score": min(100, score),
        "eligible": True,
        "match_reasons": reasons,
        "boost_factors": boosts,
    }


def get_eligible_grants(
    is_tribal: bool = True,
    has_hpsa: bool = False,
    has_mua: bool = False,
    is_rural: bool = False,
    category: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Get grants from the catalog scored by eligibility.

    Returns grants sorted by eligibility score (highest first).
    """
    results = []

    catalog = TRIBAL_GRANT_CATALOG
    if category:
        cat_lower = category.lower()
        catalog = [g for g in catalog if cat_lower in g.get("category", "").lower()]

    for grant in catalog:
        eligibility = score_eligibility(
            grant,
            is_tribal=is_tribal,
            has_hpsa=has_hpsa,
            has_mua=has_mua,
            is_rural=is_rural,
        )

        if eligibility["eligible"]:
            results.append({
                **{k: v for k, v in grant.items() if k not in ("eligibility_tags", "requires_hpsa", "requires_tribal", "boost_if_hpsa", "boost_if_tribal", "boost_if_rural", "boost_if_mua")},
                "eligibility": eligibility,
            })

    # Sort by score descending
    results.sort(key=lambda g: g["eligibility"]["score"], reverse=True)
    return results
