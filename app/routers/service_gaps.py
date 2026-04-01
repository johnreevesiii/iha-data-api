"""Service Gaps endpoint — Composite gap analysis with desert scores (Premium)."""

import logging
from fastapi import APIRouter, Depends, Query
from typing import Optional

from app.auth.dependencies import require_tier, enforce_fips_access
from app.auth.jwt_validator import TokenClaims

router = APIRouter(prefix="/v1/service-gaps", tags=["service-gaps"])
log = logging.getLogger("iha.api.service_gaps")

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


@router.get("")
async def get_service_gaps(
    fips: str = Query(..., description="5-digit county FIPS code"),
    lat: Optional[float] = Query(None),
    lon: Optional[float] = Query(None),
    user: TokenClaims = Depends(require_tier("premium", "internal")),
):
    """Composite service gap analysis — HPSA + provider supply + hospital access."""
    state_fips = fips[:2]
    county_fips = fips[2:]
    state_abbr = _STATE_FIPS.get(state_fips, "")

    gaps = {
        "fips": fips,
        "state": state_abbr,
        "desert_scores": {},
        "shortages": [],
        "recommendations": [],
    }

    # 1. HPSA shortages
    try:
        from fetchers.hpsa_data import get_shortage_area_summary
        hpsa = get_shortage_area_summary(state_abbr, "", fips)
        gaps["hpsa"] = {
            "underserved_score": hpsa.get("underserved_score", 0),
            "is_underserved": hpsa.get("is_underserved", False),
            "factors": hpsa.get("underserved_factors", []),
        }
        for factor in hpsa.get("underserved_factors", []):
            gaps["shortages"].append({"type": "designation", "detail": factor})
    except Exception as e:
        log.warning("HPSA gap analysis failed: %s", e)

    # 2. Hospital desert score
    try:
        from fetchers.fetchers import load_hospitals
        df = load_hospitals(lat=lat, lon=lon, radius_miles=30, state=state_abbr)
        hospital_count = len(df) if df is not None and not df.empty else 0
        # Desert: <2 hospitals within 30 miles
        desert_score = max(0, 100 - (hospital_count * 20))
        gaps["desert_scores"]["hospital"] = {
            "score": desert_score,
            "nearby_count": hospital_count,
            "is_desert": hospital_count < 2,
        }
        if hospital_count < 2:
            gaps["shortages"].append({"type": "access", "detail": f"Hospital desert — only {hospital_count} within 30 miles"})
            gaps["recommendations"].append("Apply for HRSA funding to expand hospital access.")
    except Exception as e:
        log.warning("Hospital gap analysis failed: %s", e)

    # 3. IHS facility coverage
    try:
        from fetchers.ihs_data import find_ihs_facilities_near
        if lat and lon:
            df = find_ihs_facilities_near(lat, lon, radius_miles=50)
            ihs_count = len(df) if df is not None and not df.empty else 0
            gaps["desert_scores"]["ihs"] = {
                "nearby_count": ihs_count,
                "is_underserved": ihs_count < 2,
            }
            if ihs_count == 0:
                gaps["shortages"].append({"type": "tribal", "detail": "No IHS/Tribal facilities within 50 miles"})
                gaps["recommendations"].append("Explore Urban Indian Health Program eligibility.")
    except Exception as e:
        log.warning("IHS gap analysis failed: %s", e)

    # 4. Broadband
    try:
        from fetchers.broadband import broadband_county
        df = broadband_county(local_path=None, county_fips=fips)
        if df is not None and not df.empty:
            gaps["desert_scores"]["broadband"] = {"available": True}
        else:
            gaps["desert_scores"]["broadband"] = {"available": False}
            gaps["shortages"].append({"type": "telehealth", "detail": "Limited broadband — telehealth barriers"})
            gaps["recommendations"].append("FCC Rural Health Care Program may provide broadband subsidies.")
    except Exception as e:
        log.warning("Broadband gap analysis failed: %s", e)

    # Overall gap score
    total_shortages = len(gaps["shortages"])
    gaps["overall_gap_score"] = min(100, total_shortages * 15 + gaps.get("hpsa", {}).get("underserved_score", 0))

    gaps["sources"] = [
        {"name": "HRSA HPSA/MUA", "url": "https://data.hrsa.gov"},
        {"name": "CMS Hospital Data", "url": "https://data.cms.gov"},
        {"name": "Indian Health Service", "url": "https://www.ihs.gov"},
        {"name": "FCC Broadband Data", "url": "https://broadbandmap.fcc.gov"},
    ]

    return gaps
