"""Environment endpoint — EPA Air Quality Index (Premium)."""

import logging
from fastapi import APIRouter, Depends, Query

from app.auth.dependencies import require_tier
from app.auth.jwt_validator import TokenClaims

router = APIRouter(prefix="/v1/environment", tags=["environment"])
log = logging.getLogger("iha.api.environment")


@router.get("/{state_fips}/{county_fips}")
async def get_environment(
    state_fips: str,
    county_fips: str,
    year: str = Query("2023", description="Year for AQI data"),
    user: TokenClaims = Depends(require_tier("premium", "internal")),
):
    """EPA Air Quality Index summary for a county."""
    summary = {}

    try:
        from fetchers.epa_aqi import summarize_air_quality
        summary = summarize_air_quality(state_fips, county_fips, year=year)
    except Exception as e:
        log.warning("EPA AQI fetch failed for %s%s: %s", state_fips, county_fips, e)

    return {
        "fips": f"{state_fips}{county_fips}",
        "state_fips": state_fips,
        "county_fips": county_fips,
        "year": year,
        "air_quality": summary,
        "sources": [{"name": "EPA Air Quality System", "url": "https://aqs.epa.gov/aqsweb/airdata"}],
    }
