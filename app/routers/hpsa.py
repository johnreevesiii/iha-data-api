"""HPSA / Shortage Areas endpoint — HRSA HPSA + MUA + RUCA."""

import logging
from fastapi import APIRouter, Depends

from app.auth.dependencies import get_current_user, enforce_fips_access
from app.auth.jwt_validator import TokenClaims
from app.schemas.responses import (
    HPSAResponse, HPSASummary, HPSADesignation,
    MUASummary, RUCASummary, DataSource,
)

router = APIRouter(prefix="/v1/hpsa", tags=["hpsa"])
log = logging.getLogger("iha.api.hpsa")


@router.get("/{state_abbr}/{county_name}", response_model=HPSAResponse)
async def get_hpsa(
    state_abbr: str,
    county_name: str,
    county_fips: str = "",
    user: TokenClaims = Depends(get_current_user),
):
    """HPSA designations, MUA status, and RUCA rural classification for a county."""
    if county_fips:
        enforce_fips_access(user, county_fips)

    result = {
        "hpsa": {},
        "mua": {},
        "ruca": {},
        "underserved_score": 0,
        "underserved_factors": [],
        "is_underserved": False,
    }

    try:
        from fetchers.hpsa_data import get_shortage_area_summary
        result = get_shortage_area_summary(state_abbr, county_name, county_fips)
    except Exception as e:
        log.warning("HPSA fetch failed: %s", e)

    # Parse HPSA sub-summary
    hpsa_raw = result.get("hpsa", {})
    hpsa_summary = HPSASummary(
        primary_care=_parse_designations(hpsa_raw.get("primary_care", [])),
        mental_health=_parse_designations(hpsa_raw.get("mental_health", [])),
        dental=_parse_designations(hpsa_raw.get("dental", [])),
    )

    # Parse MUA
    mua_raw = result.get("mua", {})
    mua_summary = MUASummary(
        is_mua=mua_raw.get("is_mua", False),
        is_mup=mua_raw.get("is_mup", False),
        designations=mua_raw.get("designations", []),
    )

    # Parse RUCA
    ruca_raw = result.get("ruca", {})
    ruca_summary = RUCASummary(
        primary_code=ruca_raw.get("primary_code"),
        classification=ruca_raw.get("classification", ""),
        is_rural=ruca_raw.get("is_rural", False),
    )

    return HPSAResponse(
        state=state_abbr,
        county=county_name,
        county_fips=county_fips,
        hpsa=hpsa_summary,
        mua=mua_summary,
        ruca=ruca_summary,
        underserved_score=result.get("underserved_score", 0),
        underserved_factors=result.get("underserved_factors", []),
        is_underserved=result.get("is_underserved", False),
        sources=[
            DataSource(name="HRSA Health Professional Shortage Areas", url="https://data.hrsa.gov"),
            DataSource(name="HRSA Medically Underserved Areas", url="https://data.hrsa.gov"),
            DataSource(name="USDA RUCA Codes", url="https://www.ers.usda.gov"),
        ],
    )


def _parse_designations(raw_list) -> list[HPSADesignation]:
    if not isinstance(raw_list, list):
        return []
    designations = []
    for item in raw_list:
        if isinstance(item, dict):
            designations.append(HPSADesignation(
                designation_type=item.get("designation_type", ""),
                score=item.get("score"),
                status=item.get("status", ""),
                name=item.get("name", ""),
            ))
    return designations
