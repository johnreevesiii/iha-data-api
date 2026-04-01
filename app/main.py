"""IHA Data API — FastAPI service exposing HCA data fetchers as REST endpoints."""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import get_settings
from app.routers import community_snapshot, hospitals, hpsa, demographics


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup / shutdown hooks."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    log = logging.getLogger("iha.api")
    log.info("IHA Data API starting up")
    yield
    log.info("IHA Data API shutting down")


settings = get_settings()

app = FastAPI(
    title=settings.app_name,
    version="1.0.0",
    description="REST API exposing tribal health community data — population, HPSA, hospitals, IHS, insurance.",
    lifespan=lifespan,
)

# CORS
origins = [o.strip() for o in settings.allowed_origins.split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type"],
)

# Routers
app.include_router(community_snapshot.router)
app.include_router(hospitals.router)
app.include_router(hpsa.router)
app.include_router(demographics.router)


@app.get("/health", tags=["meta"])
async def health_check():
    return {"status": "ok", "service": "iha-data-api", "version": "1.0.0"}
