"""Workforce endpoint — Provider counts by service line (Premium)."""

import logging
from fastapi import APIRouter, Depends, Query
from typing import Optional

from app.auth.dependencies import get_current_user, require_tier
from app.auth.jwt_validator import TokenClaims

router = APIRouter(prefix="/v1/workforce", tags=["workforce"])
log = logging.getLogger("iha.api.workforce")

SERVICE_LINES = [
    "Family Medicine", "Internal Medicine", "Pediatrics",
    "Obstetrics & Gynecology", "Psychiatry", "General Surgery",
    "Emergency Medicine", "Dentistry", "Optometry", "Pharmacy",
]


@router.get("/{state}")
async def get_workforce(
    state: str,
    city: Optional[str] = Query(None),
    service_line: Optional[str] = Query(None, description="Specific service line, or omit for all"),
    user: TokenClaims = Depends(require_tier("premium", "internal")),
):
    """Provider counts by service line from NPI Registry."""
    results = []
    lines = [service_line] if service_line else SERVICE_LINES

    try:
        from fetchers.fetchers import load_npi_service_line
        for sl in lines:
            try:
                df = load_npi_service_line(sl, city=city or "", state=state)
                results.append({
                    "service_line": sl,
                    "provider_count": len(df) if df is not None and not df.empty else 0,
                })
            except Exception as e:
                log.warning("NPI fetch failed for %s: %s", sl, e)
                results.append({"service_line": sl, "provider_count": 0})
    except Exception as e:
        log.warning("Workforce fetch failed: %s", e)

    return {
        "state": state,
        "city": city,
        "service_lines": results,
        "total_providers": sum(r["provider_count"] for r in results),
        "sources": [{"name": "CMS NPI Registry", "url": "https://npiregistry.cms.hhs.gov"}],
    }
