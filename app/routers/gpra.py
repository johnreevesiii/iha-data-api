"""GPRA endpoint — IHS Government Performance and Results Act benchmarks (Premium)."""

import logging
from fastapi import APIRouter, Depends, Query
from typing import Optional

from app.auth.dependencies import require_tier
from app.auth.jwt_validator import TokenClaims

router = APIRouter(prefix="/v1/gpra", tags=["gpra"])
log = logging.getLogger("iha.api.gpra")


@router.get("/{state}")
async def get_gpra(
    state: str,
    category: Optional[str] = Query(None, description="Filter by category (e.g., Diabetes, Immunizations)"),
    user: TokenClaims = Depends(require_tier("premium", "internal")),
):
    """IHS GPRA benchmark data for a state/area."""
    summary = {}

    try:
        from fetchers.ihs_gpra import get_gpra_summary
        summary = get_gpra_summary(state_abbr=state)
    except Exception as e:
        log.warning("GPRA fetch failed for %s: %s", state, e)

    benchmarks = []
    try:
        from fetchers.ihs_gpra import get_gpra_benchmarks
        benchmarks = get_gpra_benchmarks(category=category)
    except Exception as e:
        log.warning("GPRA benchmarks fetch failed: %s", e)

    return {
        "state": state,
        "category": category,
        "summary": summary,
        "benchmarks": benchmarks,
        "sources": [{"name": "IHS GPRA Performance Data", "url": "https://www.ihs.gov/crs/gprareporting"}],
    }
