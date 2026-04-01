"""
Data Freshness Tracking Utility
Tracks last updated timestamps for data sources and checks staleness.
"""
from __future__ import annotations
import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

# Default data directory
_DATA_DIR = Path(__file__).parent.parent.parent / "data"

# CMS data release schedule (approximate)
DATA_REFRESH_SCHEDULES = {
    "hospital_info": {
        "name": "CMS Hospital General Information",
        "refresh_days": 30,  # Monthly updates
        "source_url": "https://data.cms.gov/provider-data/dataset/xubh-q36u",
    },
    "hcahps": {
        "name": "HCAHPS Patient Satisfaction",
        "refresh_days": 90,  # Quarterly updates
        "source_url": "https://data.cms.gov/provider-data/dataset/dgck-syfz",
    },
    "quality_outcomes": {
        "name": "CMS Complications & Deaths",
        "refresh_days": 90,  # Quarterly updates
        "source_url": "https://data.cms.gov/provider-data/dataset/ynj2-r877",
    },
    "hhs_utilization": {
        "name": "HHS Hospital Utilization",
        "refresh_days": 7,  # Weekly updates
        "source_url": "https://healthdata.gov/Hospital/COVID-19-Reported-Patient-Impact-and-Hospital-Capa/g62h-syeh",
    },
    "census_population": {
        "name": "Census ACS 5-Year Estimates",
        "refresh_days": 365,  # Annual updates
        "source_url": "https://data.census.gov/",
    },
    "hpsa": {
        "name": "HRSA HPSA Designations",
        "refresh_days": 30,  # Monthly updates
        "source_url": "https://data.hrsa.gov/data/download",
    },
    "mua": {
        "name": "HRSA MUA Designations",
        "refresh_days": 30,  # Monthly updates
        "source_url": "https://data.hrsa.gov/data/download",
    },
    "ihs_facilities": {
        "name": "IHS Facility Data",
        "refresh_days": 30,  # Monthly updates
        "source_url": "https://www.ihs.gov/findfacility/",
    },
    "census_cbp": {
        "name": "Census County Business Patterns",
        "refresh_days": 365,  # Annual updates
        "source_url": "https://data.census.gov/",
    },
    "sahie": {
        "name": "Census SAHIE Insurance Estimates",
        "refresh_days": 365,  # Annual updates
        "source_url": "https://data.census.gov/",
    },
    "census_aian": {
        "name": "Census AI/AN Population Data",
        "refresh_days": 365,  # Annual updates
        "source_url": "https://data.census.gov/",
    },
    "mgma": {
        "name": "MGMA DataDive Benchmarks",
        "refresh_days": 365,  # Annual updates
        "source_url": "https://www.mgma.com/datadive",
    },
    "travel_time": {
        "name": "Travel Time / Isochrone Data",
        "refresh_days": 90,  # Quarterly updates
        "source_url": "https://openrouteservice.org/",
    },
    "broadband": {
        "name": "FCC Broadband Data",
        "refresh_days": 180,  # Semi-annual updates
        "source_url": "https://broadbandmap.fcc.gov/",
    },
    "nri": {
        "name": "FEMA National Risk Index",
        "refresh_days": 365,  # Annual updates
        "source_url": "https://hazards.fema.gov/nri/",
    },
    "svi": {
        "name": "CDC Social Vulnerability Index",
        "refresh_days": 365,  # Annual updates
        "source_url": "https://www.atsdr.cdc.gov/placeandhealth/svi/",
    },
}


def _get_freshness_file() -> Path:
    """Get path to freshness tracking file."""
    freshness_dir = _DATA_DIR / "cache"
    freshness_dir.mkdir(parents=True, exist_ok=True)
    return freshness_dir / "data_freshness.json"


def _load_freshness_data() -> Dict[str, Any]:
    """Load freshness tracking data from file."""
    freshness_file = _get_freshness_file()
    if freshness_file.exists():
        try:
            return json.loads(freshness_file.read_text())
        except (json.JSONDecodeError, Exception):
            return {}
    return {}


def _save_freshness_data(data: Dict[str, Any]) -> None:
    """Save freshness tracking data to file."""
    freshness_file = _get_freshness_file()
    freshness_file.write_text(json.dumps(data, indent=2))


def record_data_fetch(dataset: str, row_count: int = 0, source: str = "api") -> None:
    """
    Record that a dataset was fetched/updated.

    Args:
        dataset: Dataset identifier (e.g., 'hospital_info', 'hcahps')
        row_count: Number of records fetched
        source: Source of the data ('api', 'csv', 'cache')
    """
    data = _load_freshness_data()
    data[dataset] = {
        "last_updated": datetime.utcnow().isoformat(),
        "row_count": row_count,
        "source": source,
    }
    _save_freshness_data(data)


def get_data_age(dataset: str) -> Optional[timedelta]:
    """
    Get the age of a dataset since last fetch.

    Args:
        dataset: Dataset identifier

    Returns:
        timedelta since last fetch, or None if never fetched
    """
    data = _load_freshness_data()
    if dataset not in data:
        return None

    try:
        last_updated = datetime.fromisoformat(data[dataset]["last_updated"])
        return datetime.utcnow() - last_updated
    except (KeyError, ValueError):
        return None


def is_data_stale(dataset: str) -> bool:
    """
    Check if a dataset is stale based on its refresh schedule.

    Args:
        dataset: Dataset identifier

    Returns:
        True if data is stale or never fetched, False otherwise
    """
    age = get_data_age(dataset)
    if age is None:
        return True

    schedule = DATA_REFRESH_SCHEDULES.get(dataset, {})
    refresh_days = schedule.get("refresh_days", 30)

    return age > timedelta(days=refresh_days)


def get_freshness_status(dataset: str) -> Dict[str, Any]:
    """
    Get detailed freshness status for a dataset.

    Args:
        dataset: Dataset identifier

    Returns:
        Dictionary with freshness status details
    """
    data = _load_freshness_data()
    schedule = DATA_REFRESH_SCHEDULES.get(dataset, {})
    refresh_days = schedule.get("refresh_days", 30)

    if dataset not in data:
        return {
            "dataset": dataset,
            "name": schedule.get("name", dataset),
            "status": "never_fetched",
            "is_stale": True,
            "last_updated": None,
            "age_days": None,
            "refresh_days": refresh_days,
            "source_url": schedule.get("source_url"),
        }

    try:
        last_updated = datetime.fromisoformat(data[dataset]["last_updated"])
        age = datetime.utcnow() - last_updated
        age_days = age.days

        return {
            "dataset": dataset,
            "name": schedule.get("name", dataset),
            "status": "stale" if age_days > refresh_days else "fresh",
            "is_stale": age_days > refresh_days,
            "last_updated": last_updated.strftime("%Y-%m-%d %H:%M"),
            "age_days": age_days,
            "refresh_days": refresh_days,
            "row_count": data[dataset].get("row_count", 0),
            "source": data[dataset].get("source", "unknown"),
            "source_url": schedule.get("source_url"),
        }
    except (KeyError, ValueError) as e:
        return {
            "dataset": dataset,
            "name": schedule.get("name", dataset),
            "status": "error",
            "is_stale": True,
            "error": str(e),
        }


def get_all_freshness_status() -> Dict[str, Dict[str, Any]]:
    """
    Get freshness status for all tracked datasets.

    Returns:
        Dictionary mapping dataset names to their freshness status
    """
    result = {}
    for dataset in DATA_REFRESH_SCHEDULES:
        result[dataset] = get_freshness_status(dataset)
    return result


def format_freshness_for_display(dataset: str) -> str:
    """
    Format freshness status for UI display.

    Args:
        dataset: Dataset identifier

    Returns:
        Formatted string like "Data as of 2024-01-15 (3 days ago)"
    """
    status = get_freshness_status(dataset)

    if status["status"] == "never_fetched":
        return "Data not yet loaded"

    if status["status"] == "error":
        return "Data status unknown"

    last_updated = status.get("last_updated", "Unknown")
    age_days = status.get("age_days", 0)

    if age_days == 0:
        age_str = "today"
    elif age_days == 1:
        age_str = "yesterday"
    else:
        age_str = f"{age_days} days ago"

    if status["is_stale"]:
        return f"Data as of {last_updated} ({age_str}) - ⚠️ Refresh recommended"
    else:
        return f"Data as of {last_updated} ({age_str})"


def clear_dataset_freshness(dataset: str) -> None:
    """Remove a dataset's freshness entry, forcing it to show as 'never fetched'."""
    data = _load_freshness_data()
    if dataset in data:
        del data[dataset]
        _save_freshness_data(data)


def clear_all_freshness() -> None:
    """Remove all freshness entries."""
    _save_freshness_data({})


def check_stale_datasets() -> list:
    """
    Check all datasets and return list of stale ones.

    Returns:
        List of dataset identifiers that need refreshing
    """
    stale = []
    for dataset in DATA_REFRESH_SCHEDULES:
        if is_data_stale(dataset):
            stale.append(dataset)
    return stale
