"""Competition endpoint — FQHCs and competing providers (Premium)."""

import logging
from fastapi import APIRouter, Depends, Query
from typing import Optional

from app.auth.dependencies import require_tier
from app.auth.jwt_validator import TokenClaims

router = APIRouter(prefix="/v1/competition", tags=["competition"])
log = logging.getLogger("iha.api.competition")


@router.get("/{state}")
async def get_competition(
    state: str,
    zip_code: Optional[str] = Query(None),
    user: TokenClaims = Depends(require_tier("premium", "internal")),
):
    """HRSA health centers (FQHCs) and competing providers."""
    facilities = []

    try:
        from fetchers.fetchers import load_competition
        df = load_competition(state=state, zip_code=zip_code)
        if df is not None and not df.empty:
            cols = df.columns.tolist()
            for _, row in df.head(100).iterrows():
                f = {}
                for c in cols:
                    val = row.get(c)
                    if val is not None and str(val).lower() not in ("nan", "none"):
                        f[c] = str(val) if not isinstance(val, (int, float)) else val
                facilities.append(f)
    except Exception as e:
        log.warning("Competition fetch failed: %s", e)

    return {
        "state": state,
        "zip_code": zip_code,
        "count": len(facilities),
        "facilities": facilities,
        "sources": [{"name": "HRSA Health Center Program", "url": "https://data.hrsa.gov"}],
    }
