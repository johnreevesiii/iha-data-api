"""HCAHPS endpoint — Patient experience scores (Premium)."""

import logging
from fastapi import APIRouter, Depends

from app.auth.dependencies import require_tier
from app.auth.jwt_validator import TokenClaims

router = APIRouter(prefix="/v1/hcahps", tags=["hcahps"])
log = logging.getLogger("iha.api.hcahps")


@router.get("/{state}")
async def get_hcahps(
    state: str,
    user: TokenClaims = Depends(require_tier("premium", "internal")),
):
    """HCAHPS patient satisfaction survey scores."""
    hospitals = []

    try:
        from fetchers.fetchers import load_hcahps
        df = load_hcahps(state=state)
        if df is not None and not df.empty:
            cols = df.columns.tolist()
            for _, row in df.head(100).iterrows():
                h = {}
                for c in cols:
                    val = row.get(c)
                    if val is not None and str(val).lower() not in ("nan", "none"):
                        h[c] = str(val) if not isinstance(val, (int, float)) else val
                hospitals.append(h)
    except Exception as e:
        log.warning("HCAHPS fetch failed: %s", e)

    return {
        "state": state,
        "count": len(hospitals),
        "hospitals": hospitals,
        "sources": [{"name": "CMS HCAHPS Patient Satisfaction", "url": "https://data.cms.gov/provider-data/dataset/dgck-syfz"}],
    }
