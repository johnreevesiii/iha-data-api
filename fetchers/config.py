
from pathlib import Path
from dataclasses import dataclass
import os, json

DEFAULT_DATA_DIR = Path(os.getenv("HCA_DATA_DIR", Path.cwd() / "data"))
DEFAULT_CACHE_DIR = DEFAULT_DATA_DIR / "cache"

@dataclass
class Settings:
    data_dir: Path = DEFAULT_DATA_DIR
    cache_dir: Path = DEFAULT_CACHE_DIR
    cache_ttl_hours: int = int(os.getenv("HCA_CACHE_TTL_HOURS", "24"))
    socrata_app_token: str | None = os.getenv("SOCRATA_APP_TOKEN")

    def to_json(self) -> str:
        return json.dumps({
            "data_dir": str(self.data_dir),
            "cache_dir": str(self.cache_dir),
            "cache_ttl_hours": self.cache_ttl_hours,
            "socrata_app_token": bool(self.socrata_app_token),
        }, indent=2)

settings = Settings()
settings.cache_dir.mkdir(parents=True, exist_ok=True)
settings.data_dir.mkdir(parents=True, exist_ok=True)

# --- user settings for legacy connectors ---
_SETTINGS_FILE = settings.data_dir / "user_settings.json"
def load_settings() -> dict:
    try:
        return json.loads(_SETTINGS_FILE.read_text())
    except Exception:
        return {}
def save_user_settings(updates: dict) -> None:
    s = load_settings(); s.update(updates or {})
    _SETTINGS_FILE.parent.mkdir(parents=True, exist_ok=True)
    _SETTINGS_FILE.write_text(json.dumps(s, indent=2))

# --- credentials store ---
API_CREDENTIALS_FILE = settings.data_dir / "api_credentials.json"
def load_credentials() -> dict:
    try:
        creds = json.loads(API_CREDENTIALS_FILE.read_text())
    except Exception:
        creds = {}
    # Fill gaps from environment variables
    env_bridges = {
        "anthropic_api_key": "ANTHROPIC_API_KEY",
        "openai_api_key": "OPENAI_API_KEY",
        "openai_model": "OPENAI_MODEL",
        "openrouteservice_api_key": "OPENROUTESERVICE_API_KEY",
        "mapbox_token": "MAPBOX_TOKEN",
        "google_maps_api_key": "GOOGLE_MAPS_API_KEY",
        "definitive_healthcare_key": "DEFINITIVE_HEALTHCARE_KEY",
        "fair_health_key": "FAIR_HEALTH_KEY",
        "socrata_app_token": "SOCRATA_APP_TOKEN",
    }
    for cred_key, env_key in env_bridges.items():
        if not creds.get(cred_key):
            val = os.environ.get(env_key, "")
            if val:
                creds[cred_key] = val
    return creds
def save_credentials(updates: dict) -> None:
    creds = load_credentials()
    creds.update({k: v for k, v in (updates or {}).items() if v is not None})
    API_CREDENTIALS_FILE.parent.mkdir(parents=True, exist_ok=True)
    API_CREDENTIALS_FILE.write_text(json.dumps(creds, indent=2))
def apply_credentials_env(force: bool = False) -> None:
    creds = load_credentials()
    bridges = {
        "google_maps_api_key": "GOOGLE_MAPS_API_KEY",
        "openrouteservice_api_key": "OPENROUTESERVICE_API_KEY",
        "mapbox_token": "MAPBOX_TOKEN",
        "here_api_key": "HERE_API_KEY",
        "tomtom_api_key": "TOMTOM_API_KEY",
        "esri_token": "ESRI_TOKEN",
        "definitive_healthcare_key": "DEFINITIVE_HEALTHCARE_KEY",
        "plainsight_claims_key": "PLAINSIGHT_CLAIMS_KEY",
        "fair_health_key": "FAIR_HEALTH_KEY",
        "socrata_app_token": "SOCRATA_APP_TOKEN",
        "noaa_token": "NOAA_TOKEN",
        "anthropic_api_key": "ANTHROPIC_API_KEY",
        "openai_api_key": "OPENAI_API_KEY",
    }
    for k, envk in bridges.items():
        if creds.get(k) and (force or not os.environ.get(envk)):
            os.environ[envk] = creds[k]

    # Also set OpenAI model preference
    if creds.get("openai_model"):
        os.environ["OPENAI_MODEL"] = creds["openai_model"]


def get_openai_config() -> dict:
    """Get OpenAI configuration from credentials."""
    creds = load_credentials()
    return {
        "api_key": creds.get("openai_api_key") or os.environ.get("OPENAI_API_KEY"),
        "model": creds.get("openai_model", "o1"),
    }

# apply at import
apply_credentials_env()

# ensure external connectors see cache/token
os.environ.setdefault("HCA_CACHE_DIR", str(settings.cache_dir))
if settings.socrata_app_token:
    os.environ.setdefault("CMS_APP_TOKEN", settings.socrata_app_token)
    os.environ.setdefault("HEALTHDATA_APP_TOKEN", settings.socrata_app_token)
