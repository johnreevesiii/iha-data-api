"""JWT validation — verifies tokens issued by the marketplace."""

import jwt
from datetime import datetime, timezone
from typing import Optional
from pydantic import BaseModel

from app.config import get_settings


class TokenClaims(BaseModel):
    """Claims extracted from a valid marketplace JWT."""
    user_id: str
    tier: str = "free"          # free | premium | internal
    assigned_fips: str = ""     # Primary service area FIPS
    assigned_state: str = ""    # Primary service area state abbr
    exp: int = 0


def decode_token(token: str) -> Optional[TokenClaims]:
    """Decode and validate a marketplace JWT.

    Returns TokenClaims on success, None on failure.
    """
    settings = get_settings()
    try:
        payload = jwt.decode(
            token,
            settings.jwt_secret,
            algorithms=[settings.jwt_algorithm],
        )
    except jwt.ExpiredSignatureError:
        return None
    except jwt.InvalidTokenError:
        return None

    # Map marketplace field names to our claims
    return TokenClaims(
        user_id=payload.get("userId", ""),
        tier=payload.get("tier", "free"),
        assigned_fips=payload.get("assignedFips", ""),
        assigned_state=payload.get("assignedState", ""),
        exp=payload.get("exp", 0),
    )
