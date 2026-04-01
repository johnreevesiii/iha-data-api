"""County Health Rankings endpoint (Premium)."""

import logging
from fastapi import APIRouter, Depends

from app.auth.dependencies import require_tier, enforce_fips_access
from app.auth.jwt_validator import TokenClaims

router = APIRouter(prefix="/v1/chr", tags=["chr"])
log = logging.getLogger("iha.api.chr")


@router.get("/{state_fips}/{county_fips}")
async def get_chr(
    state_fips: str,
    county_fips: str,
    user: TokenClaims = Depends(require_tier("premium", "internal")),
):
    """County Health Rankings data — mortality, morbidity, clinical care, health behaviors."""
    fips = f"{state_fips}{county_fips}"
    records = []

    try:
        from fetchers.fetchers import load_chr
        df = load_chr(state_fips=state_fips, county_fips=county_fips)
        if df is not None and not df.empty:
            cols = df.columns.tolist()
            for _, row in df.iterrows():
                r = {}
                for c in cols:
                    val = row.get(c)
                    if val is not None and str(val).lower() not in ("nan", "none"):
                        r[c] = str(val) if not isinstance(val, (int, float)) else val
                records.append(r)
    except Exception as e:
        log.warning("CHR fetch failed for %s: %s", fips, e)

    return {
        "fips": fips,
        "state_fips": state_fips,
        "county_fips": county_fips,
        "count": len(records),
        "records": records,
        "sources": [{"name": "County Health Rankings & Roadmaps", "url": "https://www.countyhealthrankings.org"}],
    }
