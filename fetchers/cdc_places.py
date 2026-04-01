"""
CDC PLACES — Local Data for Better Health.

County-level and census-tract-level disease prevalence, health behaviors,
prevention measures, disability rates, and health-related social needs.

Socrata API (no key required for small queries):
  County: https://data.cdc.gov/resource/swc5-untb.json
  Tract:  https://data.cdc.gov/resource/373s-ayzu.json
"""

import logging
import requests
from hca_core.utils.http import resilient_get
import pandas as pd
from typing import Optional, Dict, Any, List

logger = logging.getLogger(__name__)

from hca_core.cache import cache_key, is_fresh, read_cache_df, write_cache_df

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_COUNTY_ENDPOINT = "https://data.cdc.gov/resource/swc5-untb.json"
_TRACT_ENDPOINT = "https://data.cdc.gov/resource/373s-ayzu.json"

# Measures most relevant for tribal health needs assessments & grant narratives
TRIBAL_PRIORITY_MEASURES = [
    "Diagnosed diabetes among adults",
    "Obesity among adults",
    "Depression among adults",
    "Chronic obstructive pulmonary disease among adults",
    "Coronary heart disease among adults",
    "Current asthma among adults",
    "High blood pressure among adults",
    "Stroke among adults",
    "Binge drinking among adults",
    "Current cigarette smoking among adults",
    "No leisure-time physical activity among adults",
    "Current lack of health insurance among adults aged 18-64 years",
    "Visits to doctor for routine checkup within the past year among adults",
    "Visited dentist or dental clinic in the past year among adults",
    "Frequent mental distress among adults",
    "Food insecurity in the past 12 months among adults",
    "Lack of reliable transportation in the past 12 months among adults",
    "Any disability among adults",
]

# Short label mapping for display
MEASURE_SHORT_LABELS: Dict[str, str] = {
    "Diagnosed diabetes among adults": "Diabetes",
    "Obesity among adults": "Obesity",
    "Depression among adults": "Depression",
    "Chronic obstructive pulmonary disease among adults": "COPD",
    "Coronary heart disease among adults": "Heart Disease",
    "Current asthma among adults": "Asthma",
    "High blood pressure among adults": "High Blood Pressure",
    "Stroke among adults": "Stroke",
    "Binge drinking among adults": "Binge Drinking",
    "Current cigarette smoking among adults": "Smoking",
    "No leisure-time physical activity among adults": "Physical Inactivity",
    "Current lack of health insurance among adults aged 18-64 years": "Uninsured (18-64)",
    "Visits to doctor for routine checkup within the past year among adults": "Annual Checkup",
    "Visited dentist or dental clinic in the past year among adults": "Dental Visit",
    "Frequent mental distress among adults": "Mental Distress",
    "Food insecurity in the past 12 months among adults": "Food Insecurity",
    "Lack of reliable transportation in the past 12 months among adults": "Transportation Barriers",
    "Any disability among adults": "Any Disability",
    "Arthritis among adults": "Arthritis",
    "Cancer (non-skin) or melanoma among adults": "Cancer",
    "High cholesterol among adults who have ever been screened": "High Cholesterol",
    "Short sleep duration among adults": "Short Sleep",
    "Colorectal cancer screening among adults aged 45\u201375 years": "Colorectal Screening",
    "Mammography use among women aged 50-74 years": "Mammography",
    "Fair or poor self-rated health status among adults": "Poor/Fair Health",
    "Frequent physical distress among adults": "Physical Distress",
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def load_places_county(
    state_abbr: str,
    county_name: Optional[str] = None,
    county_fips: Optional[str] = None,
    refresh: bool = False,
) -> pd.DataFrame:
    """
    Fetch CDC PLACES county-level health data.

    Args:
        state_abbr: Two-letter state code (e.g. ``MI``).
        county_name: County name to filter on (optional).
        county_fips: 5-digit county FIPS to filter on (optional).
        refresh: Bypass cache.

    Returns:
        DataFrame with columns including ``measure``, ``data_value``,
        ``category``, ``short_question_text``, etc.
    """
    ckey = cache_key(
        "cdc_places_county",
        state=state_abbr,
        county=county_name or county_fips or "all",
    )
    if not refresh and is_fresh(ckey, tier="annual"):
        return read_cache_df(ckey)

    logger.info("Fetching CDC PLACES county data for %s", state_abbr)

    where_clauses = [f"stateabbr='{state_abbr.upper()}'"]
    if county_name:
        where_clauses.append(f"locationname='{county_name}'")
    if county_fips:
        where_clauses.append(f"locationid='{county_fips}'")

    params = {
        "$where": " AND ".join(where_clauses),
        "$limit": 5000,
        "$offset": 0,
    }

    try:
        resp = resilient_get(_COUNTY_ENDPOINT, params=params, timeout=60)
        if resp.status_code == 200:
            data = resp.json()
            if data:
                df = pd.DataFrame(data)
                # Coerce data_value to numeric
                if "data_value" in df.columns:
                    df["data_value"] = pd.to_numeric(
                        df["data_value"], errors="coerce"
                    )
                logger.info("CDC PLACES county: %d records", len(df))
                return write_cache_df(ckey, df, tier="annual")
        else:
            logger.warning("CDC PLACES returned HTTP %s", resp.status_code)
    except (requests.RequestException, KeyError, ValueError) as exc:
        logger.warning("CDC PLACES county fetch error: %s", exc)

    # Use "daily" tier for empty results so a failed fetch is retried tomorrow
    return write_cache_df(ckey, pd.DataFrame(), tier="daily")


def load_places_tract(
    state_abbr: str,
    county_fips: Optional[str] = None,
    refresh: bool = False,
) -> pd.DataFrame:
    """
    Fetch CDC PLACES census-tract-level health data.

    Args:
        state_abbr: Two-letter state code.
        county_fips: 5-digit FIPS to limit tracts to one county.
        refresh: Bypass cache.

    Returns:
        DataFrame with tract-level prevalence estimates.
    """
    ckey = cache_key(
        "cdc_places_tract",
        state=state_abbr,
        county=county_fips or "all",
    )
    if not refresh and is_fresh(ckey, tier="annual"):
        return read_cache_df(ckey)

    logger.info("Fetching CDC PLACES tract data for %s", state_abbr)

    where_clauses = [f"stateabbr='{state_abbr.upper()}'"]
    if county_fips:
        where_clauses.append(f"countyfips='{county_fips}'")

    all_rows: list = []
    offset = 0
    page_size = 5000

    while True:
        params = {
            "$where": " AND ".join(where_clauses),
            "$limit": page_size,
            "$offset": offset,
        }
        try:
            resp = resilient_get(_TRACT_ENDPOINT, params=params, timeout=90)
            if resp.status_code != 200:
                break
            page = resp.json()
            if not page:
                break
            all_rows.extend(page)
            if len(page) < page_size:
                break
            offset += page_size
            if offset > 50000:
                break
        except (requests.RequestException, KeyError, ValueError) as exc:
            logger.warning("CDC PLACES tract page error: %s", exc)
            break

    if all_rows:
        df = pd.DataFrame(all_rows)
        if "data_value" in df.columns:
            df["data_value"] = pd.to_numeric(df["data_value"], errors="coerce")
        logger.info("CDC PLACES tract: %d records", len(df))
        return write_cache_df(ckey, df, tier="annual")

    # Use "daily" tier for empty results so a failed fetch is retried tomorrow
    return write_cache_df(ckey, pd.DataFrame(), tier="daily")


# ---------------------------------------------------------------------------
# Summary helpers
# ---------------------------------------------------------------------------

def summarize_county_health(
    df: pd.DataFrame,
    priority_only: bool = True,
) -> Dict[str, Any]:
    """
    Summarize county PLACES data into a structured dict.

    Returns a dict keyed by short label with prevalence value, category,
    and national comparison flag.
    """
    if df.empty:
        return {}

    if priority_only:
        df = df[df["measure"].isin(TRIBAL_PRIORITY_MEASURES)]

    summary: Dict[str, Any] = {}
    for _, row in df.iterrows():
        measure = row.get("measure", "")
        label = MEASURE_SHORT_LABELS.get(measure, measure)
        val = row.get("data_value")
        summary[label] = {
            "value": val,
            "unit": row.get("data_value_unit", "%"),
            "category": row.get("category", ""),
            "measure_full": measure,
        }

    return summary


def get_health_profile(
    state_abbr: str,
    county_name: Optional[str] = None,
    county_fips: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Build a complete health profile for a county from PLACES data.

    Returns:
        Dict with keys: ``measures`` (list of measure dicts),
        ``categories`` (grouped summary), ``top_concerns`` (highest values).
    """
    df = load_places_county(state_abbr, county_name, county_fips)
    if df.empty:
        return {"measures": [], "categories": {}, "top_concerns": []}

    # Filter to most recent data_value_type = "Crude prevalence"
    crude = df[df.get("data_value_type", pd.Series(dtype=str)).str.contains(
        "Crude", case=False, na=False
    )] if "data_value_type" in df.columns else df

    if crude.empty:
        crude = df

    measures = []
    for _, row in crude.iterrows():
        label = MEASURE_SHORT_LABELS.get(row.get("measure", ""), row.get("measure", ""))
        measures.append({
            "label": label,
            "value": row.get("data_value"),
            "category": row.get("category", ""),
            "measure": row.get("measure", ""),
        })

    # Group by category
    categories: Dict[str, list] = {}
    for m in measures:
        cat = m["category"]
        categories.setdefault(cat, []).append(m)

    # Top concerns: highest prevalence health outcomes / risk behaviors
    concern_cats = {"Health Outcomes", "Health Risk Behaviors", "Health Status"}
    concerns = sorted(
        [m for m in measures if m["category"] in concern_cats and m["value"] is not None],
        key=lambda x: x["value"] or 0,
        reverse=True,
    )

    return {
        "measures": measures,
        "categories": categories,
        "top_concerns": concerns[:10],
    }
