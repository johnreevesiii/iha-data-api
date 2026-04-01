"""Quality endpoint — Star ratings, outcomes, complications (Premium)."""

import logging
from fastapi import APIRouter, Depends, Query
from typing import Optional

from app.auth.dependencies import get_current_user, require_tier
from app.auth.jwt_validator import TokenClaims

router = APIRouter(prefix="/v1/quality", tags=["quality"])
log = logging.getLogger("iha.api.quality")


@router.get("/{state}")
async def get_quality(
    state: str,
    user: TokenClaims = Depends(require_tier("premium", "internal")),
):
    """Hospital quality outcomes and star ratings."""
    quality_data = []
    complications = []

    try:
        from fetchers.fetchers import load_quality_outcomes
        df = load_quality_outcomes(state=state)
        if df is not None and not df.empty:
            for _, row in df.head(50).iterrows():
                quality_data.append({k: _safe(row.get(k)) for k in [
                    "facility_id", "facility_name", "state",
                    "measure_id", "measure_name", "score",
                    "compared_to_national", "denominator",
                ]})
    except Exception as e:
        log.warning("Quality outcomes fetch failed: %s", e)

    try:
        from fetchers.fetchers import load_complications
        df = load_complications(state_abbr=state)
        if df is not None and not df.empty:
            for _, row in df.head(50).iterrows():
                complications.append({k: _safe(row.get(k)) for k in [
                    "facility_id", "facility_name", "state",
                    "measure_id", "measure_name", "score",
                    "compared_to_national",
                ]})
    except Exception as e:
        log.warning("Complications fetch failed: %s", e)

    return {
        "state": state,
        "quality_outcomes": quality_data,
        "complications": complications,
        "sources": [
            {"name": "CMS Complications & Deaths", "url": "https://data.cms.gov"},
            {"name": "CMS Quality Outcomes", "url": "https://data.cms.gov"},
        ],
    }


def _safe(val):
    if val is None:
        return None
    s = str(val)
    if s.lower() in ("nan", "none", ""):
        return None
    return s
