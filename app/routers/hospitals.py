"""Hospital directory endpoint — CMS hospital data + IHS facilities."""

import logging
from fastapi import APIRouter, Depends, Query

from app.auth.dependencies import get_current_user
from app.auth.jwt_validator import TokenClaims
from app.schemas.responses import (
    HospitalResponse, Hospital, IHSFacility, DataSource,
)

router = APIRouter(prefix="/v1/hospitals", tags=["hospitals"])
log = logging.getLogger("iha.api.hospitals")


@router.get("", response_model=HospitalResponse)
async def get_hospitals(
    lat: float = Query(..., description="Latitude of search center"),
    lon: float = Query(..., description="Longitude of search center"),
    radius: float = Query(50, description="Search radius in miles", ge=1, le=200),
    user: TokenClaims = Depends(get_current_user),
):
    """Nearby hospitals with name, type, rating, and distance."""
    hospitals = []
    try:
        from fetchers.fetchers import load_hospitals
        df = load_hospitals(lat=lat, lon=lon, radius_miles=radius)
        if df is not None and not df.empty:
            for _, row in df.iterrows():
                hospitals.append(Hospital(
                    facility_id=str(row.get("facility_id", "")),
                    facility_name=str(row.get("facility_name", "")),
                    city=str(row.get("city", "")),
                    state=str(row.get("state", "")),
                    zip_code=str(row.get("zip_code", "")),
                    county_name=str(row.get("county_name", "")),
                    hospital_type=str(row.get("hospital_type", "")),
                    hospital_ownership=str(row.get("hospital_ownership", "")),
                    overall_rating=_str_or_none(row.get("overall_rating")),
                    emergency_services=_str_or_none(row.get("emergency_services")),
                    latitude=_float_or_none(row.get("latitude")),
                    longitude=_float_or_none(row.get("longitude")),
                    distance_miles=_float_or_none(row.get("distance_miles")),
                ))
    except Exception as e:
        log.warning("Hospital fetch failed: %s", e)

    return HospitalResponse(
        count=len(hospitals),
        hospitals=hospitals,
        search_lat=lat,
        search_lon=lon,
        radius_miles=radius,
        sources=[
            DataSource(
                name="CMS Hospital General Information",
                url="https://data.cms.gov/provider-data/dataset/xubh-q36u",
            ),
        ],
    )


def _str_or_none(val):
    if val is None or str(val).lower() in ("nan", "none", ""):
        return None
    return str(val)


def _float_or_none(val):
    try:
        if val is None:
            return None
        f = float(val)
        if f != f:  # NaN check
            return None
        return round(f, 2)
    except (ValueError, TypeError):
        return None
