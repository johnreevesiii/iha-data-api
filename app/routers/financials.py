"""Financials endpoint — Medicare/Medicaid county data (Premium)."""

import logging
from fastapi import APIRouter, Depends, Query
from typing import Optional

from app.auth.dependencies import require_tier
from app.auth.jwt_validator import TokenClaims

router = APIRouter(prefix="/v1/financials", tags=["financials"])
log = logging.getLogger("iha.api.financials")


@router.get("/{state}")
async def get_financials(
    state: str,
    county_fips: Optional[str] = Query(None),
    user: TokenClaims = Depends(require_tier("premium", "internal")),
):
    """Medicare and Medicaid financial data for a state/county."""
    medicare = []
    medicaid = []

    try:
        from fetchers.fetchers import load_medicare_county
        df = load_medicare_county(state=state)
        if df is not None and not df.empty:
            if county_fips:
                fips_cols = [c for c in df.columns if "fips" in c.lower()]
                for fc in fips_cols:
                    filtered = df[df[fc].astype(str).str.contains(county_fips, na=False)]
                    if not filtered.empty:
                        df = filtered
                        break

            for _, row in df.head(50).iterrows():
                r = {}
                for c in df.columns:
                    val = row.get(c)
                    if val is not None and str(val).lower() not in ("nan", "none"):
                        r[c] = val if isinstance(val, (int, float)) else str(val)
                medicare.append(r)
    except Exception as e:
        log.warning("Medicare fetch failed for %s: %s", state, e)

    try:
        from fetchers.fetchers import load_medicaid_state
        df = load_medicaid_state(state=state)
        if df is not None and not df.empty:
            for _, row in df.head(20).iterrows():
                r = {}
                for c in df.columns:
                    val = row.get(c)
                    if val is not None and str(val).lower() not in ("nan", "none"):
                        r[c] = val if isinstance(val, (int, float)) else str(val)
                medicaid.append(r)
    except Exception as e:
        log.warning("Medicaid fetch failed for %s: %s", state, e)

    return {
        "state": state,
        "county_fips": county_fips,
        "medicare": {"count": len(medicare), "records": medicare},
        "medicaid": {"count": len(medicaid), "records": medicaid},
        "sources": [
            {"name": "CMS Medicare Geographic Variation", "url": "https://data.cms.gov"},
            {"name": "CMS Medicaid Enrollment", "url": "https://data.cms.gov"},
        ],
    }
