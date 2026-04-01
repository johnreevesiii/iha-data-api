"""In-memory response cache for Data API endpoints.

Caches GET responses by URL for a configurable TTL.
Dramatically reduces latency for repeated requests
(community snapshot goes from 10s to <100ms on cache hit).
"""

import time
import json
import logging
import hashlib
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response, JSONResponse

log = logging.getLogger("iha.api.cache")

# TTL per endpoint prefix (seconds)
CACHE_TTLS = {
    "/v1/community/": 3600,       # 1 hour — composite, expensive
    "/v1/demographics/": 3600,    # 1 hour
    "/v1/hospitals": 3600,        # 1 hour
    "/v1/hpsa/": 3600,            # 1 hour
    "/v1/workforce/": 1800,       # 30 min
    "/v1/quality/": 1800,         # 30 min
    "/v1/hcahps/": 1800,          # 30 min
    "/v1/chr/": 3600,             # 1 hour
    "/v1/broadband/": 86400,      # 24 hours — static data
    "/v1/grants/eligible": 3600,  # 1 hour
    "/v1/grants/categories": 86400, # 24 hours
}

# In-memory store: {cache_key: (response_body, content_type, timestamp)}
_cache: dict[str, tuple[bytes, str, float]] = {}
MAX_CACHE_ENTRIES = 500


class ResponseCacheMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Only cache GET requests to /v1/
        if request.method != "GET" or not request.url.path.startswith("/v1/"):
            return await call_next(request)

        # Don't cache export endpoints
        if "/export/" in request.url.path:
            return await call_next(request)

        # Find TTL for this path
        ttl = 0
        for prefix, t in CACHE_TTLS.items():
            if request.url.path.startswith(prefix):
                ttl = t
                break

        if ttl == 0:
            return await call_next(request)

        # Build cache key from path + query params (excludes auth header)
        cache_key = hashlib.md5(str(request.url).encode()).hexdigest()

        # Check cache
        if cache_key in _cache:
            body, content_type, cached_at = _cache[cache_key]
            age = time.time() - cached_at
            if age < ttl:
                log.debug("Cache HIT for %s (age=%ds)", request.url.path, int(age))
                return Response(
                    content=body,
                    media_type=content_type,
                    headers={
                        "X-Cache": "HIT",
                        "X-Cache-Age": str(int(age)),
                        "Cache-Control": f"public, max-age={int(ttl - age)}",
                    },
                )

        # Cache miss — call the actual endpoint
        response = await call_next(request)

        # Only cache 200 responses
        if response.status_code == 200:
            body = b""
            async for chunk in response.body_iterator:
                body += chunk

            content_type = response.headers.get("content-type", "application/json")

            # Evict oldest if at capacity
            if len(_cache) >= MAX_CACHE_ENTRIES:
                oldest_key = min(_cache, key=lambda k: _cache[k][2])
                del _cache[oldest_key]

            _cache[cache_key] = (body, content_type, time.time())
            log.debug("Cache STORE for %s (ttl=%ds)", request.url.path, ttl)

            return Response(
                content=body,
                status_code=200,
                media_type=content_type,
                headers={
                    "X-Cache": "MISS",
                    "Cache-Control": f"public, max-age={ttl}",
                },
            )

        return response
