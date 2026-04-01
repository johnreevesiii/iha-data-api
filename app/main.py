"""IHA Data API — FastAPI service exposing HCA data fetchers as REST endpoints."""

import os
import logging
from contextlib import asynccontextmanager

import sentry_sdk
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Sentry error monitoring
sentry_dsn = os.environ.get("SENTRY_DSN", "")
if sentry_dsn:
    sentry_sdk.init(
        dsn=sentry_dsn,
        traces_sample_rate=0.2,
        environment=os.environ.get("RAILWAY_ENVIRONMENT", "development"),
    )

from app.config import get_settings
from app.routers import (
    community_snapshot, hospitals, hpsa, demographics,
    # Phase 2 — premium endpoints
    workforce, quality, hcahps, readmissions,
    competition, chr, broadband, environment,
    gpra, health_status, service_gaps, financials, export, grants,
)
from app.middleware.rate_limit import RateLimitMiddleware
from app.middleware.response_cache import ResponseCacheMiddleware


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup / shutdown hooks."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    log = logging.getLogger("iha.api")
    log.info("IHA Data API v2.0 starting up — 16 endpoints active")
    yield
    log.info("IHA Data API shutting down")


settings = get_settings()

app = FastAPI(
    title=settings.app_name,
    version="2.0.0",
    description="REST API exposing tribal health community data — population, HPSA, hospitals, IHS, insurance, quality, workforce, and more.",
    lifespan=lifespan,
)

# Response caching (outermost = checked first)
app.add_middleware(ResponseCacheMiddleware)

# Rate limiting
app.add_middleware(RateLimitMiddleware)

# CORS
origins = [o.strip() for o in settings.allowed_origins.split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type"],
)

# Phase 1 — free tier
app.include_router(community_snapshot.router)
app.include_router(hospitals.router)
app.include_router(hpsa.router)
app.include_router(demographics.router)

# Phase 2 — premium tier
app.include_router(workforce.router)
app.include_router(quality.router)
app.include_router(hcahps.router)
app.include_router(readmissions.router)
app.include_router(competition.router)
app.include_router(chr.router)
app.include_router(broadband.router)
app.include_router(environment.router)
app.include_router(gpra.router)
app.include_router(health_status.router)
app.include_router(service_gaps.router)
app.include_router(financials.router)
app.include_router(export.router)

# Phase 3 — Capital Finder
app.include_router(grants.router)


@app.get("/health", tags=["meta"])
async def health_check():
    return {"status": "ok", "service": "iha-data-api", "version": "2.0.0"}
