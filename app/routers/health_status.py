"""Health Status endpoint — CDC PLACES disease prevalence (Premium)."""

import logging
from fastapi import APIRouter, Depends, Query
from typing import Optional

from app.auth.dependencies import require_tier
from app.auth.jwt_validator import TokenClaims

router = APIRouter(prefix="/v1/health", tags=["health"])
log = logging.getLogger("iha.api.health_status")


@router.get("/{state}/{county}")
async def get_health_status(
    state: str,
    county: str,
    county_fips: Optional[str] = Query(None),
    user: TokenClaims = Depends(require_tier("premium", "internal")),
):
    """CDC PLACES health prevalence data for a county."""
    profile = {}

    try:
        from fetchers.cdc_places import get_health_profile
        profile = get_health_profile(
            state_abbr=state,
            county_name=county,
            county_fips=county_fips,
        )
    except Exception as e:
        log.warning("CDC PLACES fetch failed for %s, %s: %s", county, state, e)

    return {
        "state": state,
        "county": county,
        "county_fips": county_fips,
        "profile": profile,
        "sources": [{"name": "CDC PLACES Local Health Data", "url": "https://www.cdc.gov/places"}],
    }
