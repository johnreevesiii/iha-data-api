"""Readmissions endpoint — HRRP penalties and readmission rates (Premium)."""

import logging
from fastapi import APIRouter, Depends

from app.auth.dependencies import require_tier
from app.auth.jwt_validator import TokenClaims

router = APIRouter(prefix="/v1/readmissions", tags=["readmissions"])
log = logging.getLogger("iha.api.readmissions")


@router.get("/{state}")
async def get_readmissions(
    state: str,
    user: TokenClaims = Depends(require_tier("premium", "internal")),
):
    """Hospital readmission rates and complications data."""
    records = []

    try:
        from fetchers.fetchers import load_complications
        df = load_complications(state_abbr=state)
        if df is not None and not df.empty:
            for _, row in df.head(100).iterrows():
                records.append({
                    "facility_id": _s(row.get("facility_id")),
                    "facility_name": _s(row.get("facility_name")),
                    "state": _s(row.get("state")),
                    "measure_id": _s(row.get("measure_id")),
                    "measure_name": _s(row.get("measure_name")),
                    "score": _s(row.get("score")),
                    "compared_to_national": _s(row.get("compared_to_national")),
                })
    except Exception as e:
        log.warning("Readmissions fetch failed: %s", e)

    return {
        "state": state,
        "count": len(records),
        "records": records,
        "sources": [{"name": "CMS Hospital Readmissions Reduction Program", "url": "https://data.cms.gov"}],
    }


def _s(val):
    if val is None:
        return None
    s = str(val)
    return None if s.lower() in ("nan", "none", "") else s
