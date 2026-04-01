"""
EPA Air Quality System (AQS) — County-level air quality data.

Provides annual AQI summaries (PM2.5, Ozone) for county-level SDOH analysis.

API docs: https://aqs.epa.gov/aqsweb/documents/data_api.html
No API key required for testing (test@aqs.api / test).
Register at: https://aqs.epa.gov/data/api/signup?email=YOUR_EMAIL
"""

import pandas as pd
import requests
from typing import Optional, Dict, Any

from hca_core.cache import cache_key, is_fresh, read_cache_df, write_cache_df
from hca_core.utils.data_freshness import record_data_fetch
from hca_core.utils.http import resilient_get
from hca_core.config import settings
from hca_core.utils.logging import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_BASE_URL = "https://aqs.epa.gov/data/api"

# Criteria pollutant codes
POLLUTANT_CODES = {
    "pm25": "88101",
    "ozone": "44201",
    "pm10": "81102",
    "no2": "42602",
    "so2": "42401",
    "co": "42101",
}

# AQI categories
AQI_CATEGORIES = {
    (0, 50): ("Good", "#00e400"),
    (51, 100): ("Moderate", "#ffff00"),
    (101, 150): ("Unhealthy for Sensitive Groups", "#ff7e00"),
    (151, 200): ("Unhealthy", "#ff0000"),
    (201, 300): ("Very Unhealthy", "#8f3f97"),
    (301, 500): ("Hazardous", "#7e0023"),
}


def _get_credentials() -> tuple:
    """Get EPA AQS API credentials from config or use test credentials."""
    email = getattr(settings, "epa_email", None) or "test@aqs.api"
    key = getattr(settings, "epa_api_key", None) or "test"
    return email, key


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def load_aqi_annual(
    state_fips: str,
    county_fips: str,
    year: str = "2023",
    pollutants: Optional[list] = None,
    refresh: bool = False,
) -> pd.DataFrame:
    """
    Fetch annual AQI summary from EPA AQS for a county.

    Args:
        state_fips: 2-digit state FIPS (e.g. ``26`` for Michigan).
        county_fips: 3-digit county FIPS (e.g. ``005`` for Allegan).
        year: Calendar year.
        pollutants: List of pollutant keys (default: pm25, ozone).
        refresh: Bypass cache.

    Returns:
        DataFrame with annual summary statistics per pollutant/monitor.
    """
    if pollutants is None:
        pollutants = ["pm25", "ozone"]

    param_codes = ",".join(POLLUTANT_CODES[p] for p in pollutants if p in POLLUTANT_CODES)

    ckey = cache_key(
        "epa_aqi_annual",
        state=state_fips, county=county_fips,
        year=year, params=param_codes,
    )
    if not refresh and is_fresh(ckey, tier="annual"):
        return read_cache_df(ckey)

    logger.info("Fetching EPA AQS annual data", extra={"state": state_fips, "county": county_fips, "dataset": "epa_aqi_annual"})

    email, key = _get_credentials()

    url = f"{_BASE_URL}/annualData/byCounty"
    params = {
        "email": email,
        "key": key,
        "param": param_codes,
        "bdate": f"{year}0101",
        "edate": f"{year}1231",
        "state": state_fips,
        "county": county_fips,
    }

    try:
        resp = resilient_get(url, params=params, timeout=60)
        if resp.status_code != 200:
            logger.warning("EPA AQS HTTP error", extra={"status_code": resp.status_code})
            return write_cache_df(ckey, pd.DataFrame(), tier="daily")

        body = resp.json()
        data = body.get("Data", [])
        if not data:
            logger.warning("No EPA AQS data", extra={"state": state_fips, "county": county_fips})
            return write_cache_df(ckey, pd.DataFrame(), tier="daily")

        df = pd.DataFrame(data)

        # Coerce numeric columns
        numeric_cols = [
            "arithmetic_mean", "arithmetic_standard_dev",
            "first_max_value", "second_max_value",
            "ninety_ninth_percentile", "ninety_eighth_percentile",
            "ninety_fifth_percentile", "ninetieth_percentile",
            "seventy_fifth_percentile", "fiftieth_percentile",
            "tenth_percentile", "observation_count",
            "valid_day_count", "primary_exceedance_count",
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        logger.info("EPA AQS annual loaded", extra={"count": len(df), "dataset": "epa_aqi_annual"})
        record_data_fetch("epa_aqi", row_count=len(df), source="api", source_version=f"EPA AQS {year}", data_vintage_year=int(year))
        return write_cache_df(ckey, df, tier="annual")

    except (requests.RequestException, KeyError, ValueError) as exc:
        logger.warning("EPA AQS error: %s", exc)
        return write_cache_df(ckey, pd.DataFrame(), tier="daily")


def load_aqi_daily(
    state_fips: str,
    county_fips: str,
    year: str = "2023",
    month: str = "01",
    pollutants: Optional[list] = None,
    refresh: bool = False,
) -> pd.DataFrame:
    """
    Fetch daily AQI data from EPA AQS for a county and month.

    Args:
        state_fips: 2-digit state FIPS.
        county_fips: 3-digit county FIPS.
        year: Calendar year.
        month: 2-digit month (01-12).
        pollutants: List of pollutant keys.
        refresh: Bypass cache.

    Returns:
        DataFrame with daily AQI values.
    """
    if pollutants is None:
        pollutants = ["pm25", "ozone"]

    param_codes = ",".join(POLLUTANT_CODES[p] for p in pollutants if p in POLLUTANT_CODES)
    last_day = "28" if month == "02" else "30" if month in ("04", "06", "09", "11") else "31"

    ckey = cache_key(
        "epa_aqi_daily", state=state_fips, county=county_fips,
        year=year, month=month,
    )
    if not refresh and is_fresh(ckey, tier="annual"):
        return read_cache_df(ckey)

    logger.info("Fetching EPA AQS daily data", extra={"state": state_fips, "county": county_fips, "dataset": "epa_aqi_daily"})

    email, key = _get_credentials()

    url = f"{_BASE_URL}/dailyData/byCounty"
    params = {
        "email": email,
        "key": key,
        "param": param_codes,
        "bdate": f"{year}{month}01",
        "edate": f"{year}{month}{last_day}",
        "state": state_fips,
        "county": county_fips,
    }

    try:
        resp = resilient_get(url, params=params, timeout=60)
        if resp.status_code != 200:
            return write_cache_df(ckey, pd.DataFrame(), tier="daily")

        body = resp.json()
        data = body.get("Data", [])
        if not data:
            return write_cache_df(ckey, pd.DataFrame(), tier="daily")

        df = pd.DataFrame(data)
        if "aqi" in df.columns:
            df["aqi"] = pd.to_numeric(df["aqi"], errors="coerce")
        if "arithmetic_mean" in df.columns:
            df["arithmetic_mean"] = pd.to_numeric(df["arithmetic_mean"], errors="coerce")

        logger.info("EPA AQS daily loaded", extra={"count": len(df), "dataset": "epa_aqi_daily"})
        return write_cache_df(ckey, df, tier="annual")

    except (requests.RequestException, KeyError, ValueError) as exc:
        logger.warning("EPA AQS daily error: %s", exc)
        return write_cache_df(ckey, pd.DataFrame(), tier="daily")


# ---------------------------------------------------------------------------
# Summary helpers
# ---------------------------------------------------------------------------

def get_aqi_category(aqi_value: float) -> tuple:
    """Return (category_name, color) for an AQI value."""
    for (low, high), (name, color) in AQI_CATEGORIES.items():
        if low <= aqi_value <= high:
            return name, color
    return "Unknown", "#999999"


def summarize_air_quality(
    state_fips: str,
    county_fips: str,
    year: str = "2023",
) -> Dict[str, Any]:
    """
    Build an air quality summary for a county.

    Returns:
        Dict with ``pm25_mean``, ``ozone_mean``, ``aqi_category``,
        ``exceedance_days``, etc.
    """
    df = load_aqi_annual(state_fips, county_fips, year)
    if df.empty:
        return {}

    summary: Dict[str, Any] = {"year": year, "monitors": len(df)}

    # PM2.5 summary
    pm25 = df[df["parameter_code"] == "88101"] if "parameter_code" in df.columns else pd.DataFrame()
    if not pm25.empty:
        summary["pm25_mean"] = round(float(pm25["arithmetic_mean"].mean()), 1)
        summary["pm25_max"] = round(float(pm25["first_max_value"].max()), 1)
        summary["pm25_95th"] = round(float(pm25["ninety_fifth_percentile"].mean()), 1)
        exc = pm25.get("primary_exceedance_count")
        if exc is not None:
            summary["pm25_exceedance_days"] = int(exc.sum())

    # Ozone summary
    o3 = df[df["parameter_code"] == "44201"] if "parameter_code" in df.columns else pd.DataFrame()
    if not o3.empty:
        summary["ozone_mean"] = round(float(o3["arithmetic_mean"].mean()), 4)
        summary["ozone_max"] = round(float(o3["first_max_value"].max()), 4)

    return summary
