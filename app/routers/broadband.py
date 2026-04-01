"""Broadband endpoint — FCC broadband access data (Premium)."""

import logging
from fastapi import APIRouter, Depends

from app.auth.dependencies import require_tier, enforce_fips_access
from app.auth.jwt_validator import TokenClaims

router = APIRouter(prefix="/v1/broadband", tags=["broadband"])
log = logging.getLogger("iha.api.broadband")


@router.get("/{fips}")
async def get_broadband(
    fips: str,
    user: TokenClaims = Depends(require_tier("premium", "internal")),
):
    """FCC broadband availability for a county."""
    data = {}

    try:
        from fetchers.broadband import broadband_county
        df = broadband_county(local_path=None, county_fips=fips)
        if df is not None and not df.empty:
            row = df.iloc[0]
            for c in df.columns:
                val = row.get(c)
                if val is not None and str(val).lower() not in ("nan", "none"):
                    data[c] = str(val) if not isinstance(val, (int, float)) else val
    except Exception as e:
        log.warning("Broadband fetch failed for %s: %s", fips, e)

    return {
        "fips": fips,
        "data": data,
        "sources": [{"name": "FCC Broadband Data", "url": "https://broadbandmap.fcc.gov"}],
    }
