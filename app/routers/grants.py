"""Grants endpoint — Discovery, eligibility matching, and live search."""

import logging
from fastapi import APIRouter, Depends, Query
from typing import Optional

from app.auth.dependencies import get_current_user, enforce_fips_access
from app.auth.jwt_validator import TokenClaims

router = APIRouter(prefix="/v1/grants", tags=["grants"])
log = logging.getLogger("iha.api.grants")

# State FIPS → abbreviation
_STATE_FIPS = {
    "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA",
    "08": "CO", "09": "CT", "10": "DE", "11": "DC", "12": "FL",
    "13": "GA", "15": "HI", "16": "ID", "17": "IL", "18": "IN",
    "19": "IA", "20": "KS", "21": "KY", "22": "LA", "23": "ME",
    "24": "MD", "25": "MA", "26": "MI", "27": "MN", "28": "MS",
    "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH",
    "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND",
    "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI",
    "45": "SC", "46": "SD", "47": "TN", "48": "TX", "49": "UT",
    "50": "VT", "51": "VA", "53": "WA", "54": "WV", "55": "WI",
    "56": "WY",
}


@router.get("/eligible")
async def get_eligible_grants(
    fips: str = Query(..., description="5-digit county FIPS"),
    category: Optional[str] = Query(None, description="Filter by category"),
    user: TokenClaims = Depends(get_current_user),
):
    """Get grants scored by eligibility based on community profile.

    Uses HPSA, MUA, rural, and tribal status to score each grant.
    Free tier: assigned FIPS only. Premium: any FIPS.
    """
    enforce_fips_access(user, fips)

    state_fips = fips[:2]
    county_fips = fips[2:]
    state_abbr = _STATE_FIPS.get(state_fips, "")

    # Build community profile from HPSA data
    has_hpsa = False
    has_mua = False
    is_rural = False
    underserved_score = 0

    try:
        from fetchers.hpsa_data import get_shortage_area_summary
        hpsa = get_shortage_area_summary(state_abbr, "", fips)
        has_hpsa = any("HPSA" in f for f in hpsa.get("underserved_factors", []))
        has_mua = any("MUA" in f or "MUP" in f for f in hpsa.get("underserved_factors", []))
        is_rural = any("Rural" in f for f in hpsa.get("underserved_factors", []))
        underserved_score = hpsa.get("underserved_score", 0)
    except Exception as e:
        log.warning("HPSA lookup for grant eligibility failed: %s", e)

    # Score grants
    from fetchers.grants import get_eligible_grants as _get_eligible
    grants = _get_eligible(
        is_tribal=True,  # marketplace users are tribal orgs
        has_hpsa=has_hpsa,
        has_mua=has_mua,
        is_rural=is_rural,
        category=category,
    )

    return {
        "fips": fips,
        "state": state_abbr,
        "community_profile": {
            "has_hpsa": has_hpsa,
            "has_mua": has_mua,
            "is_rural": is_rural,
            "underserved_score": underserved_score,
        },
        "grants_count": len(grants),
        "grants": grants,
        "categories": sorted(set(g["category"] for g in grants)),
    }


@router.get("/search")
async def search_grants(
    keywords: str = Query("tribal health", description="Search keywords"),
    limit: int = Query(25, ge=1, le=100),
    user: TokenClaims = Depends(get_current_user),
):
    """Search grants.gov for open federal opportunities."""
    from fetchers.grants import search_grants_gov
    results = search_grants_gov(keywords=keywords, limit=limit)

    return {
        "keywords": keywords,
        "count": len(results),
        "opportunities": results,
        "source": "grants.gov",
    }


@router.get("/categories")
async def get_grant_categories(
    user: TokenClaims = Depends(get_current_user),
):
    """List available grant categories."""
    from fetchers.grants import TRIBAL_GRANT_CATALOG
    categories = {}
    for g in TRIBAL_GRANT_CATALOG:
        cat = g["category"]
        if cat not in categories:
            categories[cat] = {"name": cat, "count": 0}
        categories[cat]["count"] += 1

    return {
        "categories": sorted(categories.values(), key=lambda c: c["count"], reverse=True),
    }
