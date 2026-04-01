"""Sliding-window rate limiter middleware.

Tier limits per hour:
  - free:     60 requests
  - premium:  300 requests
  - internal: unlimited
"""

import time
import logging
from collections import defaultdict
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from app.auth.jwt_validator import decode_token

log = logging.getLogger("iha.api.ratelimit")

TIER_LIMITS = {
    "free": 60,
    "premium": 300,
    "internal": 0,  # 0 = unlimited
}

WINDOW_SECONDS = 3600  # 1 hour

# In-memory store: {user_id: [(timestamp, ...), ...]}
_requests: dict[str, list[float]] = defaultdict(list)


class RateLimitMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Skip non-API routes
        path = request.url.path
        if not path.startswith("/v1/"):
            return await call_next(request)

        # Extract user from token
        auth = request.headers.get("authorization", "")
        if not auth.startswith("Bearer "):
            return await call_next(request)

        token = auth.removeprefix("Bearer ").strip()
        claims = decode_token(token)
        if claims is None:
            return await call_next(request)

        tier = claims.tier or "free"
        limit = TIER_LIMITS.get(tier, 60)

        # Unlimited for internal
        if limit == 0:
            return await call_next(request)

        # Sliding window
        user_id = claims.user_id
        now = time.time()
        window_start = now - WINDOW_SECONDS

        # Prune old entries
        _requests[user_id] = [t for t in _requests[user_id] if t > window_start]

        if len(_requests[user_id]) >= limit:
            retry_after = int(_requests[user_id][0] - window_start) + 1
            log.warning("Rate limit hit for user %s (tier=%s, count=%d)", user_id, tier, len(_requests[user_id]))
            return JSONResponse(
                status_code=429,
                content={
                    "detail": f"Rate limit exceeded ({limit} requests/hour for {tier} tier)",
                    "retry_after": retry_after,
                },
                headers={"Retry-After": str(retry_after)},
            )

        _requests[user_id].append(now)
        response = await call_next(request)
        response.headers["X-RateLimit-Limit"] = str(limit)
        response.headers["X-RateLimit-Remaining"] = str(max(0, limit - len(_requests[user_id])))
        return response
