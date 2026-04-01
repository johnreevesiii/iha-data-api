"""FastAPI dependencies for auth and tier enforcement."""

from fastapi import Depends, HTTPException, Header
from typing import Optional

from app.auth.jwt_validator import TokenClaims, decode_token


async def get_current_user(
    authorization: Optional[str] = Header(None),
) -> TokenClaims:
    """Extract and validate JWT from Authorization header.

    Raises 401 if token is missing or invalid.
    """
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid Authorization header")

    token = authorization.removeprefix("Bearer ").strip()
    claims = decode_token(token)
    if claims is None:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    return claims


def require_tier(*allowed_tiers: str):
    """Dependency factory: require user's tier to be in allowed_tiers.

    Usage:
        @router.get("/premium-endpoint", dependencies=[Depends(require_tier("premium", "internal"))])
    """
    async def _check(user: TokenClaims = Depends(get_current_user)):
        if user.tier not in allowed_tiers:
            raise HTTPException(
                status_code=403,
                detail=f"This endpoint requires one of: {', '.join(allowed_tiers)}",
            )
        return user
    return _check


def enforce_fips_access(user: TokenClaims, requested_fips: str) -> None:
    """Raise 403 if free-tier user requests a FIPS outside their assignment.

    Premium and internal users can access any FIPS.
    """
    if user.tier in ("premium", "internal"):
        return

    if not user.assigned_fips:
        raise HTTPException(
            status_code=403,
            detail="No service area assigned. Contact your IHA administrator.",
        )

    if requested_fips != user.assigned_fips:
        raise HTTPException(
            status_code=403,
            detail="Free tier: access limited to your assigned service area. "
                   "Contact IHA to upgrade.",
        )
