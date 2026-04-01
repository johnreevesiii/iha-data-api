"""Application configuration loaded from environment variables."""

from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    # API
    app_name: str = "IHA Data API"
    debug: bool = False

    # Auth — shared secret with marketplace
    jwt_secret: str = "change-me-in-production"
    jwt_algorithm: str = "HS256"

    # CORS
    allowed_origins: str = "http://localhost:5173,https://iha-consultant-marketplace.vercel.app"

    # Data API keys (optional — fetchers degrade gracefully)
    census_api_key: str = ""
    socrata_app_token: str = ""

    class Config:
        env_file = ".env"
        extra = "ignore"


@lru_cache()
def get_settings() -> Settings:
    return Settings()
