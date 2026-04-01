"""
Data fetching functions for healthcare datasets.
Enhanced with 3-tier fallback, CSV caching, and auto-refresh.
"""

import sys
import requests
import pandas as pd
from typing import Optional, Dict, Any, List, Tuple
from pathlib import Path
import io
import zipfile
from datetime import datetime, timedelta
import re
import calendar
import json

from fetchers.cache import cache_key, is_fresh, read_cache_df, write_cache_df
from fetchers.config import settings
from fetchers.utils.data_freshness import record_data_fetch


# ============================================
# CSV CACHE MANAGEMENT
# ============================================

def _get_csv_cache_dir() -> Path:
    """Get CSV cache directory."""
    csv_dir = settings.cache_dir / "csv_cache"
    csv_dir.mkdir(parents=True, exist_ok=True)
    return csv_dir


def _get_csv_path(dataset_name: str) -> Path:
    """Get path for cached CSV with date versioning."""
    cache_dir = _get_csv_cache_dir()
    # Look for existing CSV (any date)
    pattern = f"{dataset_name}_*.csv"
    existing = list(cache_dir.glob(pattern))
    
    if existing:
        # Return most recent
        return max(existing, key=lambda p: p.stat().st_mtime)
    
    # Return new path with current date
    date_str = datetime.now().strftime("%Y-%m-%d")
    return cache_dir / f"{dataset_name}_{date_str}.csv"


def _is_csv_stale(csv_path: Path, max_age_days: int = 90) -> bool:
    """Check if CSV is older than max_age_days."""
    if not csv_path.exists():
        return True
    
    file_age = datetime.now() - datetime.fromtimestamp(csv_path.stat().st_mtime)
    return file_age > timedelta(days=max_age_days)


def _save_csv_version(df: pd.DataFrame, dataset_name: str) -> Path:
    """Save CSV with date versioning."""
    date_str = datetime.now().strftime("%Y-%m-%d")
    cache_dir = _get_csv_cache_dir()
    
    # Remove old versions
    pattern = f"{dataset_name}_*.csv"
    for old_file in cache_dir.glob(pattern):
        try:
            old_file.unlink()
        except:
            pass
    
    # Save new version
    new_path = cache_dir / f"{dataset_name}_{date_str}.csv"
    df.to_csv(new_path, index=False)
    print(f"  Saved CSV: {new_path.name}")
    return new_path


def _read_csv_safe(csv_path: Path) -> Optional[pd.DataFrame]:
    """Safely read CSV with multiple encoding attempts."""
    if not csv_path.exists():
        return None
    
    encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
    
    for encoding in encodings:
        try:
            return pd.read_csv(csv_path, encoding=encoding, low_memory=False)
        except:
            continue
    
    return None


# ============================================
# HOSPITAL DIRECTORY & GENERAL INFO
# ============================================

def load_hospital_info(
    state: Optional[str] = None,
    county: Optional[str] = None,
    city: Optional[str] = None,
    zip_code: Optional[str] = None,
    ccn: Optional[str] = None,
    refresh: bool = False
) -> pd.DataFrame:
    """
    Load hospital general information from CMS Provider Data.
    3-tier fallback: API -> CSV download -> Cached CSV
    Dataset: Hospital General Information (xubh-q36u)
    Tier: quarterly
    """
    try:
        print(f"\n{'='*60}", file=sys.stderr)
        print(f"HOSPITAL INFO SEARCH:", file=sys.stderr)
        print(f"  State: {state}, City: {city}, County: {county}", file=sys.stderr)
        print(f"{'='*60}\n", file=sys.stderr)
    except (OSError, ValueError):
        pass  # stderr may be unavailable in Streamlit on Windows
    
    key = cache_key("hospital_info", state=state or "", county=county or "", 
                    city=city or "", zip=zip_code or "", ccn=ccn or "")
    
    if not refresh and is_fresh(key, tier="quarterly"):
        return read_cache_df(key)
    
    print(f"[Hospital] Fetching hospital information from CMS...")

    # TIER 1: Try CMS Provider Data API (working endpoint) with pagination
    url = "https://data.cms.gov/provider-data/api/1/datastore/query/xubh-q36u/0"

    # Build filter params for the API (uses conditions[n][property]/[value] syntax)
    base_params = {}
    condition_idx = 0
    if state:
        base_params[f"conditions[{condition_idx}][property]"] = "state"
        base_params[f"conditions[{condition_idx}][value]"] = state.upper()
        condition_idx += 1
    if city:
        base_params[f"conditions[{condition_idx}][property]"] = "citytown"
        base_params[f"conditions[{condition_idx}][value]"] = city.upper()
        condition_idx += 1
    if county:
        base_params[f"conditions[{condition_idx}][property]"] = "countyparish"
        base_params[f"conditions[{condition_idx}][value]"] = county.upper()
        condition_idx += 1
    if zip_code:
        base_params[f"conditions[{condition_idx}][property]"] = "zip_code"
        base_params[f"conditions[{condition_idx}][value]"] = zip_code
        condition_idx += 1
    if ccn:
        base_params[f"conditions[{condition_idx}][property]"] = "facility_id"
        base_params[f"conditions[{condition_idx}][value]"] = ccn
        condition_idx += 1

    try:
        all_results = []
        offset = 0
        page_size = 1000  # API max per request

        while True:
            params = {**base_params, "limit": page_size, "offset": offset}
            response = requests.get(url, params=params, timeout=60)

            if response.status_code != 200:
                print(f"  -> Response status: {response.status_code}")
                break

            data = response.json()
            results = data.get('results', [])

            if not results:
                break

            all_results.extend(results)
            print(f"  -> Fetched {len(results)} hospitals (total: {len(all_results)})")

            # If we got fewer than page_size, we're done
            if len(results) < page_size:
                break

            offset += page_size

            # Safety limit to prevent infinite loops
            if offset > 10000:
                print(f"  -> Reached safety limit, stopping pagination")
                break

        if all_results:
            df = pd.DataFrame(all_results)
            # Normalize column names to match expected format
            column_map = {
                'citytown': 'city',
                'countyparish': 'county_name',
            }
            df = df.rename(columns=column_map)
            print(f"[OK] Retrieved {len(df)} hospitals from CMS API")
            record_data_fetch("hospital_info", row_count=len(df), source="api")
            return write_cache_df(key, df, tier="quarterly")
        else:
            print(f"  -> API returned empty results")
    except Exception as e:
        print(f"[WARNING]  Tier 1 (API) failed: {e}")
    
    # TIER 2: Try CSV download from Socrata
    print(f"  -> Trying CSV download...")
    csv_url = "https://data.cms.gov/provider-data/sites/default/files/data_dictionaries/hospital/Hospital_General_Information.csv"
    
    try:
        csv_response = requests.get(csv_url, timeout=90)
        if csv_response.status_code == 200:
            df = pd.read_csv(io.StringIO(csv_response.text), low_memory=False)
            
            # Filter locally
            if state and 'State' in df.columns:
                df = df[df['State'].str.upper() == state.upper()]
            if city and 'City' in df.columns:
                df = df[df['City'].str.lower() == city.lower()]
            
            if not df.empty:
                print(f"[OK] Retrieved {len(df)} hospitals from CSV")
                _save_csv_version(df, "hospital_info")
                record_data_fetch("hospital_info", row_count=len(df), source="csv")
                return write_cache_df(key, df, tier="quarterly")
    except Exception as e:
        print(f"[WARNING]  Tier 2 (CSV) failed: {e}")
    
    # TIER 3: Check cached CSV
    csv_path = _get_csv_path("hospital_info")
    if csv_path.exists():
        print(f"  -> Loading from cached CSV: {csv_path.name}")
        df = _read_csv_safe(csv_path)
        if df is not None and not df.empty:
            # Filter locally
            if state and 'State' in df.columns:
                df = df[df['State'].str.upper() == state.upper()]
            if city and 'City' in df.columns:
                df = df[df['City'].str.lower() == city.lower()]
            
            if not df.empty:
                print(f"[OK] Loaded {len(df)} hospitals from cache")
                return write_cache_df(key, df, tier="quarterly")
    
    print(f"[WARNING] All tiers failed - returning empty DataFrame (no sample data)")
    # Return empty DataFrame instead of sample data to avoid confusion
    empty_df = pd.DataFrame(columns=[
        'facility_id', 'facility_name', 'city', 'state', 'zip_code',
        'county_name', 'hospital_type', 'hospital_ownership', 'latitude', 'longitude'
    ])
    return write_cache_df(key, empty_df, tier="quarterly")


# ============================================
# HOSPITAL UTILIZATION
# ============================================

def load_hhs_util(
    state: Optional[str] = None,
    refresh: bool = False
) -> pd.DataFrame:
    """
    Load HHS hospital utilization data.
    Dataset: COVID-19 Hospital Capacity (g62h-syeh)
    Tier: weekly
    """
    key = cache_key("hhs_util", state=state or "all")
    
    if not refresh and is_fresh(key, tier="weekly"):
        return read_cache_df(key)
    
    print(f"Fetching hospital utilization from HHS...")
    
    url = "https://healthdata.gov/resource/g62h-syeh.json"
    params = {"$limit": 5000, "$order": "date DESC"}
    
    if state:
        params["state"] = state.upper()
    
    try:
        headers = {}
        if settings.socrata_app_token:
            headers["X-App-Token"] = settings.socrata_app_token
        
        response = requests.get(url, params=params, headers=headers, timeout=60)
        
        if response.status_code == 200:
            data = response.json()
            if data:
                df = pd.DataFrame(data)
                print(f"[OK] Retrieved {len(df)} HHS records")
                return write_cache_df(key, df, tier="weekly")
    except Exception as e:
        print(f"[WARNING]  HHS API error: {e}")

    # Return empty DataFrame instead of sample data
    print(f"[WARNING] HHS utilization data unavailable - returning empty DataFrame")
    empty_df = pd.DataFrame(columns=[
        'hospital_pk', 'hospital_name', 'state', 'date',
        'inpatient_beds', 'inpatient_beds_used',
        'total_beds_7_day_avg', 'all_adult_hospital_beds_7_day_avg'
    ])
    return write_cache_df(key, empty_df, tier="weekly")


# ============================================
# QUALITY METRICS - HCAHPS
# ============================================

def load_hcahps(
    state: Optional[str] = None,
    ccn: Optional[str] = None,
    refresh: bool = False
) -> pd.DataFrame:
    """
    Load HCAHPS patient survey scores.
    3-tier fallback: API -> CSV -> Cache
    Dataset: Patient Survey HCAHPS (dgck-syfz)
    Tier: quarterly
    """
    key = cache_key("hcahps", state=state or "", ccn=ccn or "")
    
    if not refresh and is_fresh(key, tier="quarterly"):
        return read_cache_df(key)
    
    print(f"⭐ Fetching HCAHPS scores from CMS...")
    
    # TIER 1: Try CMS API
    url = "https://data.cms.gov/data-api/v1/dataset/dgck-syfz/data"
    
    filters = []
    if state:
        filters.append(f"state = '{state.upper()}'")
    if ccn:
        filters.append(f"facility_id = '{ccn}'")
    
    params = {"size": 5000, "offset": 0}
    if filters:
        params["filter"] = " AND ".join(filters)
    
    try:
        response = requests.get(url, params=params, timeout=60)
        
        if response.status_code == 200:
            data = response.json()
            if data and len(data) > 0:
                df = pd.DataFrame(data)
                print(f"[OK] Retrieved {len(df)} HCAHPS records")
                return write_cache_df(key, df, tier="quarterly")
    except Exception as e:
        print(f"[WARNING]  HCAHPS API error: {e}")
    
    # TIER 2: Try CSV download
    print(f"  -> Trying HCAHPS CSV download...")
    csv_url = "https://data.cms.gov/provider-data/sites/default/files/data_dictionaries/hospital/HCAHPS_Hospital.csv"
    
    try:
        csv_response = requests.get(csv_url, timeout=90)
        if csv_response.status_code == 200:
            df = pd.read_csv(io.StringIO(csv_response.text), low_memory=False)
            
            # Filter locally
            if state and 'State' in df.columns:
                df = df[df['State'].str.upper() == state.upper()]
            
            if not df.empty:
                print(f"[OK] Retrieved {len(df)} HCAHPS records from CSV")
                _save_csv_version(df, "hcahps")
                return write_cache_df(key, df, tier="quarterly")
    except Exception as e:
        print(f"[WARNING]  HCAHPS CSV failed: {e}")
    
    # TIER 3: Cached CSV
    csv_path = _get_csv_path("hcahps")
    if csv_path.exists():
        df = _read_csv_safe(csv_path)
        if df is not None and not df.empty:
            if state and 'State' in df.columns:
                df = df[df['State'].str.upper() == state.upper()]
            if not df.empty:
                print(f"[OK] Loaded {len(df)} HCAHPS from cache")
                return write_cache_df(key, df, tier="quarterly")
    
    return write_cache_df(key, pd.DataFrame(), tier="quarterly")


# ============================================
# QUALITY METRICS - OUTCOMES
# ============================================

def load_quality_outcomes(
    state: Optional[str] = None,
    refresh: bool = False
) -> pd.DataFrame:
    """
    Load hospital quality outcome measures.
    Datasets:
    - ynj2-r877: Complications & Deaths
    - 632h-zaca: Unplanned Hospital Visits
    - yv7e-xc69: Timely & Effective Care
    Tier: quarterly
    """
    key = cache_key("quality_outcomes", state=state or "all")
    
    if not refresh and is_fresh(key, tier="quarterly"):
        return read_cache_df(key)
    
    print(f"Fetching quality outcome measures from CMS...")
    
    datasets = [
        ("ynj2-r877", "Complications & Deaths"),
        ("632h-zaca", "Unplanned Visits"),
        ("yv7e-xc69", "Timely & Effective Care")
    ]
    
    frames = []
    for ds_id, name in datasets:
        try:
            url = f"https://data.cms.gov/data-api/v1/dataset/{ds_id}/data"
            params = {"size": 5000, "offset": 0}
            
            if state:
                params["filter"] = f"state = '{state.upper()}'"
            
            response = requests.get(url, params=params, timeout=60)
            
            if response.status_code == 200:
                data = response.json()
                if data:
                    df = pd.DataFrame(data)
                    df['dataset_source'] = name
                    frames.append(df)
                    print(f"  ✓ {name}: {len(df)} records")
        except Exception as e:
            print(f"  ✗ {name}: {e}")
            continue
    
    if frames:
        result = pd.concat(frames, ignore_index=True, sort=False)
        print(f"[OK] Retrieved {len(result)} total quality records")
        return write_cache_df(key, result, tier="quarterly")
    
    return write_cache_df(key, pd.DataFrame(), tier="quarterly")


# ============================================
# INPATIENT DATA (DRG/IPPS)
# ============================================

def load_inpatient_drg(
    state: Optional[str] = None,
    ccn: Optional[str] = None,
    year: str = "2023",
    refresh: bool = False
) -> pd.DataFrame:
    """
    Load Medicare inpatient DRG data by provider.
    Dataset: Medicare Inpatient Hospital by Provider and Service
    Tier: annual
    """
    key = cache_key("inpatient_drg", state=state or "", ccn=ccn or "", year=year)
    
    if not refresh and is_fresh(key, tier="annual"):
        return read_cache_df(key)
    
    print(f"Fetching inpatient DRG data from CMS...")
    
    # Try multiple dataset IDs (CMS changes these)
    dataset_ids = ["c05p-pd6e", "97k6-zzx3"]  # Different years
    
    for ds_id in dataset_ids:
        try:
            url = f"https://data.cms.gov/data-api/v1/dataset/{ds_id}/data"
            
            filters = []
            # Try different column name variations
            if state:
                filters.append(f"rndrng_prvdr_state_abrvtn = '{state.upper()}'")
            if ccn:
                filters.append(f"rndrng_prvdr_ccn = '{ccn}'")
            
            params = {"size": 10000, "offset": 0}
            if filters:
                params["filter"] = " AND ".join(filters)
            
            response = requests.get(url, params=params, timeout=90)
            
            if response.status_code == 200:
                data = response.json()
                if data and len(data) > 0:
                    df = pd.DataFrame(data)
                    
                    # Normalize column names
                    df = _normalize_cms_columns(df)
                    
                    print(f"[OK] Retrieved {len(df)} inpatient DRG records")
                    return write_cache_df(key, df, tier="annual")
        except Exception as e:
            print(f"  [WARNING] Dataset {ds_id} failed: {e}")
            continue
    
    return write_cache_df(key, pd.DataFrame(), tier="annual")


def _normalize_cms_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize CMS column names to consistent format."""
    if df.empty:
        return df
    
    # Column mapping for common variations
    column_map = {
        # DRG columns
        'drg_cd': 'drg',
        'drg_definition': 'drg_desc',
        'ms_drg': 'drg',
        
        # Discharge/Service columns
        'tot_dschrgs': 'discharges',
        'total_discharges': 'discharges',
        'tot_srvcs': 'services',
        'total_services': 'services',
        
        # Charge columns
        'avg_submtd_cvrd_chrg': 'avg_covered_charges',
        'average_covered_charges': 'avg_covered_charges',
        'avg_mdcr_pymt_amt': 'avg_medicare_payments',
        'average_medicare_payments': 'avg_medicare_payments',
        'avg_tot_pymt_amt': 'avg_total_payments',
        'average_total_payments': 'avg_total_payments',
        
        # Provider columns
        'rndrng_prvdr_ccn': 'provider_ccn',
        'provider_id': 'provider_ccn',
        'rndrng_prvdr_org_name': 'provider_name',
        'provider_name': 'provider_name',
        'rndrng_prvdr_state_abrvtn': 'state',
        'provider_state': 'state',
        
        # HCPCS columns
        'hcpcs_cd': 'hcpcs',
        'hcpcs_code': 'hcpcs',
        'hcpcs_description': 'hcpcs_desc',
        
        # Beneficiary columns
        'bene_cnt': 'beneficiaries',
        'distinct_beneficiaries': 'beneficiaries',
    }
    
    # Rename columns (case-insensitive)
    df_lower = {k.lower(): k for k in df.columns}
    rename_dict = {}
    
    for old_lower, new_name in column_map.items():
        if old_lower in df_lower:
            rename_dict[df_lower[old_lower]] = new_name
    
    if rename_dict:
        df = df.rename(columns=rename_dict)
    
    return df


# ============================================
# OUTPATIENT DATA (HCPCS/APC)
# ============================================

def load_outpatient_opd(
    state: Optional[str] = None,
    ccn: Optional[str] = None,
    year: str = "2023",
    refresh: bool = False
) -> pd.DataFrame:
    """
    Load Medicare outpatient procedure data by provider.
    Dataset: Medicare Hospital Outpatient PUF
    Tier: annual
    """
    key = cache_key("outpatient_opd", state=state or "", ccn=ccn or "", year=year)
    
    if not refresh and is_fresh(key, tier="annual"):
        return read_cache_df(key)
    
    print(f"[Hospital] Fetching outpatient procedure data from CMS...")
    
    # Try multiple dataset IDs
    dataset_ids = ["f2my-mvp6", "4vq6-qz7d"]
    
    for ds_id in dataset_ids:
        try:
            url = f"https://data.cms.gov/data-api/v1/dataset/{ds_id}/data"
            
            filters = []
            if state:
                filters.append(f"rndrng_prvdr_state_abrvtn = '{state.upper()}'")
            if ccn:
                filters.append(f"rndrng_prvdr_ccn = '{ccn}'")
            
            params = {"size": 10000, "offset": 0}
            if filters:
                params["filter"] = " AND ".join(filters)
            
            response = requests.get(url, params=params, timeout=90)
            
            if response.status_code == 200:
                data = response.json()
                if data and len(data) > 0:
                    df = pd.DataFrame(data)
                    
                    # Normalize column names
                    df = _normalize_cms_columns(df)
                    
                    print(f"[OK] Retrieved {len(df)} outpatient procedure records")
                    return write_cache_df(key, df, tier="annual")
        except Exception as e:
            print(f"  [WARNING] Dataset {ds_id} failed: {e}")
            continue
    
    return write_cache_df(key, pd.DataFrame(), tier="annual")


# ============================================
# CENSUS & DEMOGRAPHICS
# ============================================

def load_population(
    state_fips: Optional[str] = None,
    county_fips: Optional[str] = None,
    refresh: bool = False
) -> pd.DataFrame:
    """
    Load Census population data with detailed demographics.
    Includes age groups and income brackets for visualization.
    Note: Census API has 50-variable limit, so we make multiple requests.
    Tier: annual
    """
    key = cache_key("population_acs5", state=state_fips or "", county=county_fips or "")

    if not refresh and is_fresh(key, tier="annual"):
        cached = read_cache_df(key)
        if cached is not None and not cached.empty:
            return cached

    print(f"Fetching population data from Census Bureau...")

    try:
        year = "2022"
        base_url = f"https://api.census.gov/data/{year}/acs/acs5"

        # Split variables into batches (Census API limit is 50 variables)
        # Batch 1: Core demographics + male age groups
        variables_batch1 = [
            "NAME", "B01001_001E", "B01002_001E", "B19013_001E", "B17001_002E",
            "B01001_003E", "B01001_004E", "B01001_005E", "B01001_006E",
            "B01001_007E", "B01001_008E", "B01001_009E", "B01001_010E",
            "B01001_011E", "B01001_012E", "B01001_013E", "B01001_014E",
            "B01001_015E", "B01001_016E", "B01001_017E", "B01001_018E",
            "B01001_019E", "B01001_020E", "B01001_021E", "B01001_022E",
            "B01001_023E", "B01001_024E", "B01001_025E",
            "B01001_027E", "B01001_028E", "B01001_029E", "B01001_030E",
            "B01001_031E", "B01001_032E", "B01001_033E", "B01001_034E",
            "B01001_035E", "B01001_036E", "B01001_037E", "B01001_038E",
            "B01001_039E", "B01001_040E", "B01001_041E", "B01001_042E",
            "B01001_043E", "B01001_044E", "B01001_045E", "B01001_046E",
            "B01001_047E", "B01001_048E"
        ]

        params = {
            "get": ",".join(variables_batch1),
            "for": f"county:{county_fips}" if county_fips else f"state:{state_fips}",
        }
        if county_fips:
            params["in"] = f"state:{state_fips}"

        response = requests.get(base_url, params=params, timeout=60)

        if response.status_code == 200:
            data = response.json()

            if len(data) > 1:
                df = pd.DataFrame(data[1:], columns=data[0])

                # Fetch batch 2: Female age groups + Income brackets
                variables_batch2 = [
                    "B01001_049E",  # Female 85+
                    "B19001_002E", "B19001_003E", "B19001_004E", "B19001_005E",
                    "B19001_006E", "B19001_007E", "B19001_008E", "B19001_009E",
                    "B19001_010E", "B19001_011E", "B19001_012E", "B19001_013E",
                    "B19001_014E", "B19001_015E", "B19001_016E", "B19001_017E"
                ]

                params2 = {
                    "get": ",".join(variables_batch2),
                    "for": f"county:{county_fips}" if county_fips else f"state:{state_fips}",
                }
                if county_fips:
                    params2["in"] = f"state:{state_fips}"

                response2 = requests.get(base_url, params=params2, timeout=60)
                if response2.status_code == 200:
                    data2 = response2.json()
                    if len(data2) > 1:
                        df2 = pd.DataFrame(data2[1:], columns=data2[0])
                        # Merge batch 2 columns (excluding state/county duplicates)
                        for col in df2.columns:
                            if col.startswith('B') and col not in df.columns:
                                df[col] = df2[col]

                # Convert all numeric columns
                for col in df.columns:
                    if col.startswith('B'):
                        df[col] = pd.to_numeric(df[col], errors='coerce')

                # Create summary columns
                df['population_total'] = df.get('B01001_001E', 0)
                df['median_age'] = df.get('B01002_001E', 0)
                df['median_household_income'] = df.get('B19013_001E', 0)
                df['poverty_count'] = df.get('B17001_002E', 0)

                # Age groups: Under 18
                under18_cols = ['B01001_003E', 'B01001_004E', 'B01001_005E', 'B01001_006E',
                               'B01001_027E', 'B01001_028E', 'B01001_029E', 'B01001_030E']
                df['age_under_18'] = df[[c for c in under18_cols if c in df.columns]].sum(axis=1)

                # Age groups: 18-64 (working age)
                working_age_cols = ['B01001_007E', 'B01001_008E', 'B01001_009E', 'B01001_010E',
                                   'B01001_011E', 'B01001_012E', 'B01001_013E', 'B01001_014E',
                                   'B01001_015E', 'B01001_016E', 'B01001_017E', 'B01001_018E',
                                   'B01001_019E', 'B01001_031E', 'B01001_032E', 'B01001_033E',
                                   'B01001_034E', 'B01001_035E', 'B01001_036E', 'B01001_037E',
                                   'B01001_038E', 'B01001_039E', 'B01001_040E', 'B01001_041E',
                                   'B01001_042E', 'B01001_043E']
                df['age_18_64'] = df[[c for c in working_age_cols if c in df.columns]].sum(axis=1)

                # Age groups: 65+
                senior_cols = ['B01001_020E', 'B01001_021E', 'B01001_022E', 'B01001_023E',
                              'B01001_024E', 'B01001_025E', 'B01001_044E', 'B01001_045E',
                              'B01001_046E', 'B01001_047E', 'B01001_048E', 'B01001_049E']
                df['age_65_plus'] = df[[c for c in senior_cols if c in df.columns]].sum(axis=1)

                # Income brackets
                df['income_under_25k'] = df[[c for c in ['B19001_002E', 'B19001_003E', 'B19001_004E', 'B19001_005E'] if c in df.columns]].sum(axis=1)
                df['income_25k_50k'] = df[[c for c in ['B19001_006E', 'B19001_007E', 'B19001_008E', 'B19001_009E', 'B19001_010E'] if c in df.columns]].sum(axis=1)
                df['income_50k_100k'] = df[[c for c in ['B19001_011E', 'B19001_012E', 'B19001_013E'] if c in df.columns]].sum(axis=1)
                df['income_100k_150k'] = df[[c for c in ['B19001_014E', 'B19001_015E'] if c in df.columns]].sum(axis=1)
                df['income_150k_plus'] = df[[c for c in ['B19001_016E', 'B19001_017E'] if c in df.columns]].sum(axis=1)

                # Calculate percentage rates
                pop = df['population_total'].iloc[0] if 'population_total' in df.columns and len(df) > 0 else 0
                if pop > 0:
                    df['pct_under_18'] = (df['age_under_18'] / pop * 100).round(1)
                    df['pct_18_64'] = (df['age_18_64'] / pop * 100).round(1)
                    df['pct_65_plus'] = (df['age_65_plus'] / pop * 100).round(1)
                    df['poverty_rate'] = (df['poverty_count'] / pop * 100).round(1)

                print(f"Retrieved Census population data with demographics")
                return write_cache_df(key, df, tier="annual")
        else:
            print(f"Census API returned status {response.status_code}")
    except Exception as e:
        print(f"Census API error: {e}")

    return write_cache_df(key, pd.DataFrame(), tier="annual")


def load_industry(
    year: str = "2022",
    state_fips: Optional[str] = None,
    county_fips: Optional[str] = None,
    naics: str = "00",
    refresh: bool = False
) -> pd.DataFrame:
    """Load Census industry data. Tier: annual"""
    key = cache_key("cbp", state=state_fips or "", county=county_fips or "",
                    naics=naics, year=year)

    if not refresh and is_fresh(key, tier="annual"):
        return read_cache_df(key)

    print(f"Fetching industry data from Census CBP...")

    try:
        base_url = f"https://api.census.gov/data/{year}/cbp"

        variables = ["EMP", "ESTAB", "PAYANN", "NAICS2017"]

        params = {
            "get": ",".join(variables),
            "NAICS2017": naics,
        }

        if county_fips:
            params["for"] = f"county:{county_fips}"
            params["in"] = f"state:{state_fips}"
        else:
            params["for"] = f"state:{state_fips}"

        response = requests.get(base_url, params=params, timeout=30)

        if response.status_code == 200:
            data = response.json()

            if len(data) > 1:
                df = pd.DataFrame(data[1:], columns=data[0])

                for col in ["EMP", "ESTAB", "PAYANN"]:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')

                print(f"[OK] Retrieved Census CBP data")
                return write_cache_df(key, df, tier="annual")
    except Exception as e:
        print(f"[WARNING]  Census CBP API error: {e}")

    return write_cache_df(key, pd.DataFrame(), tier="annual")


def load_healthcare_industry(
    state_fips: Optional[str] = None,
    county_fips: Optional[str] = None,
    year: str = "2022",
    refresh: bool = False
) -> pd.DataFrame:
    """
    Load healthcare industry data by NAICS subsector.
    Fetches NAICS 62 (Healthcare and Social Assistance) subsectors.
    Tier: annual
    """
    key = cache_key("cbp_healthcare", state=state_fips or "", county=county_fips or "", year=year)

    if not refresh and is_fresh(key, tier="annual"):
        return read_cache_df(key)

    print(f"[Hospital] Fetching healthcare industry data from Census CBP...")

    # Healthcare NAICS codes and their descriptions
    healthcare_naics = {
        '621': 'Ambulatory Health Care Services',
        '6211': 'Offices of Physicians',
        '6212': 'Offices of Dentists',
        '6213': 'Offices of Other Health Practitioners',
        '6214': 'Outpatient Care Centers',
        '6215': 'Medical & Diagnostic Laboratories',
        '6216': 'Home Health Care Services',
        '6219': 'Other Ambulatory Health Care',
        '622': 'Hospitals',
        '6221': 'General Medical & Surgical Hospitals',
        '6222': 'Psychiatric & Substance Abuse Hospitals',
        '6223': 'Specialty Hospitals',
        '623': 'Nursing & Residential Care',
        '6231': 'Nursing Care Facilities (SNF)',
        '6232': 'Residential Intellectual Disability Facilities',
        '6233': 'Continuing Care Retirement Communities',
        '6239': 'Other Residential Care Facilities',
        '624': 'Social Assistance',
        '6241': 'Individual & Family Services',
        '6242': 'Community Food & Housing Services',
        '6243': 'Vocational Rehabilitation Services',
        '6244': 'Child Day Care Services',
    }

    frames = []
    base_url = f"https://api.census.gov/data/{year}/cbp"

    for naics_code in healthcare_naics.keys():
        try:
            params = {
                "get": "EMP,ESTAB,PAYANN,NAICS2017",
                "NAICS2017": naics_code,
            }

            if county_fips:
                params["for"] = f"county:{county_fips}"
                params["in"] = f"state:{state_fips}"
            else:
                params["for"] = f"state:{state_fips}"

            response = requests.get(base_url, params=params, timeout=30)

            if response.status_code == 200:
                data = response.json()
                if len(data) > 1:
                    df = pd.DataFrame(data[1:], columns=data[0])
                    df['naics_code'] = naics_code
                    df['naics_description'] = healthcare_naics[naics_code]
                    frames.append(df)
        except Exception:
            continue

    if frames:
        result = pd.concat(frames, ignore_index=True)

        # Convert numeric columns
        for col in ["EMP", "ESTAB", "PAYANN"]:
            if col in result.columns:
                result[col] = pd.to_numeric(result[col], errors='coerce')

        # Add subsector grouping (3-digit level)
        result['subsector'] = result['naics_code'].str[:3]
        result['subsector_name'] = result['subsector'].map({
            '621': 'Ambulatory Care',
            '622': 'Hospitals',
            '623': 'Nursing & Residential',
            '624': 'Social Assistance'
        })

        print(f"[OK] Retrieved {len(result)} healthcare industry records")
        return write_cache_df(key, result, tier="annual")

    return write_cache_df(key, pd.DataFrame(), tier="annual")


# ============================================
# SAHIE - UNINSURED ESTIMATES
# ============================================

def load_sahie(
    year: str = "2022",
    state_fips: str = "06",
    county_fips: str = "000",
    refresh: bool = False
) -> pd.DataFrame:
    """
    Load SAHIE (Small Area Health Insurance Estimates).
    REAL Census API implementation.
    API: https://api.census.gov/data/timeseries/healthins/sahie

    Variables:
    - NUI_PT: Number uninsured (point estimate)
    - NIC_PT: Number insured (point estimate)
    - PCTUI_PT: Percent uninsured
    - PCTIC_PT: Percent insured with coverage

    Filters:
    - AGECAT=0: All ages (not filtered by age)
    - IPRCAT=0: All income levels
    - RACECAT=0: All races
    - SEXCAT=0: Both sexes

    Tier: annual
    """
    key = cache_key("sahie", state=state_fips, county=county_fips, year=year)

    if not refresh and is_fresh(key, tier="annual"):
        cached = read_cache_df(key)
        if cached is not None and not cached.empty:
            return cached

    print(f"[Hospital] Fetching uninsured estimates from Census SAHIE...")

    # Try timeseries API first (more reliable)
    try:
        base_url = "https://api.census.gov/data/timeseries/healthins/sahie"

        # Key SAHIE variables (NIPR_PT and NIPUB_PT not available in timeseries API)
        variables = [
            "NAME",          # Geography name
            "NUI_PT",        # Number uninsured (point estimate)
            "NIC_PT",        # Number insured (point estimate)
            "PCTUI_PT",      # Percent uninsured (point estimate)
            "PCTIC_PT",      # Percent insured with coverage (point estimate)
        ]

        params = {
            "get": ",".join(variables),
            "time": year,
            # Filter for all ages, all income levels to get total population estimate
            "AGECAT": "0",   # All ages
            "IPRCAT": "0",   # All income levels
            "RACECAT": "0",  # All races
            "SEXCAT": "0",   # Both sexes
        }

        # Support wildcard county queries (county_fips="*" returns all counties in state)
        if county_fips == "*":
            params["for"] = "county:*"
            params["in"] = f"state:{state_fips}"
        elif county_fips and county_fips != "000":
            params["for"] = f"county:{county_fips}"
            params["in"] = f"state:{state_fips}"
        else:
            params["for"] = f"state:{state_fips}"

        response = requests.get(base_url, params=params, timeout=30)

        if response.status_code == 200:
            data = response.json()

            if len(data) > 1:
                df = pd.DataFrame(data[1:], columns=data[0])

                # Convert numeric columns
                numeric_cols = ["NUI_PT", "NIC_PT", "PCTUI_PT", "PCTIC_PT"]
                for col in numeric_cols:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')

                # Add friendly column names
                df = df.rename(columns={
                    'NUI_PT': 'uninsured_count',
                    'NIC_PT': 'insured_count',
                    'PCTUI_PT': 'uninsured_pct',
                    'PCTIC_PT': 'insured_pct',
                })

                print(f"[OK] Retrieved SAHIE uninsured estimates")
                return write_cache_df(key, df, tier="annual")
        else:
            print(f"  [WARNING] SAHIE timeseries API returned {response.status_code}")
    except Exception as e:
        print(f"[WARNING]  SAHIE timeseries API error: {e}")

    # Fallback to standard year-based API
    try:
        base_url = f"https://api.census.gov/data/{year}/sahie"

        variables = ["PCTUI_PT", "NUI_PT", "NIC_PT", "PCTIC_PT"]

        params = {
            "get": ",".join(variables),
            "AGECAT": "0",   # All ages
            "IPRCAT": "0",   # All income levels
        }

        if county_fips == "*":
            params["for"] = "county:*"
            params["in"] = f"state:{state_fips}"
        elif county_fips and county_fips != "000":
            params["for"] = f"county:{county_fips}"
            params["in"] = f"state:{state_fips}"
        else:
            params["for"] = f"state:{state_fips}"

        response = requests.get(base_url, params=params, timeout=30)

        if response.status_code == 200:
            data = response.json()

            if len(data) > 1:
                df = pd.DataFrame(data[1:], columns=data[0])

                for col in ["PCTUI_PT", "NUI_PT", "NIC_PT", "PCTIC_PT"]:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')

                df = df.rename(columns={
                    'NUI_PT': 'uninsured_count',
                    'NIC_PT': 'insured_count',
                    'PCTUI_PT': 'uninsured_pct',
                    'PCTIC_PT': 'insured_pct',
                })

                print(f"[OK] Retrieved SAHIE data from year-based API")
                return write_cache_df(key, df, tier="annual")
    except Exception as e:
        print(f"[WARNING]  SAHIE year API error: {e}")

    # Return empty DataFrame with expected columns
    print(f"[WARNING] SAHIE data not available for this location")
    return write_cache_df(key, pd.DataFrame(columns=[
        'NAME', 'uninsured_count', 'insured_count', 'uninsured_pct',
        'insured_pct', 'state', 'county'
    ]), tier="annual")


# ============================================
# COUNTY HEALTH RANKINGS
# ============================================

def load_chr(
    state_fips: str,
    county_fips: str = "000",
    refresh: bool = False
) -> pd.DataFrame:
    """
    Load County Health Rankings data.
    Downloads from CHR website (CSV).
    Tier: annual
    """
    key = cache_key("chr", state=state_fips, county=county_fips)
    
    if not refresh and is_fresh(key, tier="annual"):
        return read_cache_df(key)
    
    print(f"Fetching County Health Rankings...")
    
    # Check cached CSV first
    csv_path = _get_csv_path("chr_national")
    
    if _is_csv_stale(csv_path, max_age_days=90) or refresh:
        # Download latest CHR data
        # Note: CHR releases annual data, URL may need updating
        year = datetime.now().year
        csv_url = f"https://www.countyhealthrankings.org/sites/default/files/media/document/analytic_data{year}.csv"
        
        try:
            print(f"  -> Downloading CHR data for {year}...")
            response = requests.get(csv_url, timeout=120)
            
            if response.status_code == 200:
                df = pd.read_csv(io.StringIO(response.text), low_memory=False)
                _save_csv_version(df, "chr_national")
                print(f"[OK] Downloaded CHR data: {len(df)} records")
            else:
                print(f"  [WARNING] CHR download failed: {response.status_code}")
                df = None
        except Exception as e:
            print(f"[WARNING]  CHR download error: {e}")
            df = None
    else:
        print(f"  -> Loading from cached CHR data...")
        df = _read_csv_safe(csv_path)
    
    # Filter to requested location
    if df is not None and not df.empty:
        # CHR uses different column names - normalize
        if 'statecode' in df.columns:
            df_filtered = df[df['statecode'] == state_fips]
            
            if county_fips != "000" and 'countycode' in df.columns:
                full_fips = f"{state_fips}{county_fips}"
                df_filtered = df_filtered[df_filtered['countycode'] == full_fips]
            
            if not df_filtered.empty:
                print(f"[OK] Filtered to {len(df_filtered)} CHR records")
                return write_cache_df(key, df_filtered, tier="annual")
    
    return write_cache_df(key, pd.DataFrame(), tier="annual")


# ============================================
# MEDICARE ENROLLMENT
# ============================================

# FIPS to State Abbreviation mapping (used by multiple functions)
FIPS_TO_STATE_ABBR = {
    '01': 'AL', '02': 'AK', '04': 'AZ', '05': 'AR', '06': 'CA',
    '08': 'CO', '09': 'CT', '10': 'DE', '11': 'DC', '12': 'FL',
    '13': 'GA', '15': 'HI', '16': 'ID', '17': 'IL', '18': 'IN',
    '19': 'IA', '20': 'KS', '21': 'KY', '22': 'LA', '23': 'ME',
    '24': 'MD', '25': 'MA', '26': 'MI', '27': 'MN', '28': 'MS',
    '29': 'MO', '30': 'MT', '31': 'NE', '32': 'NV', '33': 'NH',
    '34': 'NJ', '35': 'NM', '36': 'NY', '37': 'NC', '38': 'ND',
    '39': 'OH', '40': 'OK', '41': 'OR', '42': 'PA', '44': 'RI',
    '45': 'SC', '46': 'SD', '47': 'TN', '48': 'TX', '49': 'UT',
    '50': 'VT', '51': 'VA', '53': 'WA', '54': 'WV', '55': 'WI',
    '56': 'WY'
}


def load_medicare_county(
    year: Optional[str] = None,
    state_fips: str = "",
    county_fips: str = "",
    refresh: bool = False
) -> pd.DataFrame:
    """
    Load Medicare enrollment by county from CMS Geographic Variation Public Use File.

    API: https://data.cms.gov/data-api/v1/dataset/6219697b-8f6c-4164-bed4-cd9317c58ebc/data

    Key columns:
    - BENE_GEO_LVL: Geographic level (National, State, County)
    - BENE_GEO_CD: FIPS code (5-digit for county)
    - BENE_GEO_DESC: Geography name
    - BENES_TOTAL_CNT: Total Medicare beneficiaries
    - BENES_FFS_CNT: Original Medicare (FFS) beneficiaries
    - BENES_MA_CNT: Medicare Advantage beneficiaries
    - MA_PRTCPTN_RATE: MA participation rate (0-1)

    Tier: annual
    """
    key = cache_key("medicare", state=state_fips, county=county_fips, year=year or "")

    if not refresh and is_fresh(key, tier="annual"):
        cached = read_cache_df(key)
        if cached is not None and not cached.empty:
            return cached

    print(f"Fetching Medicare enrollment data from CMS Geographic Variation...")

    # Default to most recent available year (data typically 1-2 years behind)
    year_str = year or "2022"

    # Build FIPS for filtering
    full_fips = f"{state_fips}{county_fips}" if county_fips and county_fips != "000" else None
    geo_level = "County" if full_fips else "State"
    geo_code = full_fips if full_fips else state_fips

    # TIER 1: CMS Geographic Variation API (preferred source)
    try:
        # Medicare Geographic Variation by National, State & County
        url = "https://data.cms.gov/data-api/v1/dataset/6219697b-8f6c-4164-bed4-cd9317c58ebc/data"

        # Build filter for specific geography
        filter_str = f"BENE_GEO_LVL = '{geo_level}' AND YEAR = '{year_str}'"
        if full_fips:
            filter_str += f" AND BENE_GEO_CD = '{full_fips}'"
        else:
            # For state level, filter by state code
            state_abbr = FIPS_TO_STATE_ABBR.get(state_fips, '')
            filter_str += f" AND BENE_GEO_DESC LIKE '%{state_abbr}%'"

        params = {
            "size": 100,
            "offset": 0,
            "filter": filter_str
        }

        response = requests.get(url, params=params, timeout=60)

        if response.status_code == 200:
            data = response.json()
            if data and len(data) > 0:
                df = pd.DataFrame(data)

                # Normalize column names
                df = _normalize_medicare_columns(df)

                # Get the most recent year if multiple
                if 'year' in df.columns:
                    df = df.sort_values('year', ascending=False)
                    df = df.head(1)  # Take most recent

                print(f"[OK] Retrieved Medicare data: {len(df)} records from CMS Geographic Variation API")
                return write_cache_df(key, df, tier="annual")
        else:
            print(f"  [WARNING] CMS API returned status {response.status_code}")

    except Exception as e:
        print(f"[WARNING]  Medicare Geographic Variation API error: {e}")

    # TIER 2: Try broader query without strict filter
    try:
        url = "https://data.cms.gov/data-api/v1/dataset/6219697b-8f6c-4164-bed4-cd9317c58ebc/data"

        # Query for county level data in the state
        params = {
            "size": 500,
            "offset": 0
        }

        response = requests.get(url, params=params, timeout=60)

        if response.status_code == 200:
            data = response.json()
            if data:
                df = pd.DataFrame(data)

                # Filter in Python
                df = df[df['BENE_GEO_LVL'].astype(str) == geo_level]

                if full_fips:
                    df = df[df['BENE_GEO_CD'].astype(str) == full_fips]
                else:
                    # State-level filter
                    state_abbr = FIPS_TO_STATE_ABBR.get(state_fips, '')
                    df = df[df['BENE_GEO_DESC'].astype(str).str.contains(state_abbr, case=False, na=False)]

                if not df.empty:
                    df = _normalize_medicare_columns(df)
                    print(f"[OK] Retrieved Medicare data from filtered results")
                    return write_cache_df(key, df, tier="annual")

    except Exception as e:
        print(f"[WARNING]  Medicare fallback query error: {e}")

    # TIER 3: Return empty with warning (no synthetic estimates)
    print(f"  [WARNING] Medicare data unavailable for {geo_level} {geo_code}")
    empty_df = pd.DataFrame(columns=[
        'state_fips', 'county_fips', 'total_beneficiaries',
        'original_medicare', 'medicare_advantage', 'ma_penetration_pct',
        'year', 'data_source'
    ])
    return write_cache_df(key, empty_df, tier="annual")


def _normalize_medicare_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize Medicare column names for consistency.

    Handles columns from:
    - CMS Geographic Variation API (BENES_TOTAL_CNT, BENES_FFS_CNT, etc.)
    - Older Medicare enrollment files (tot_benes, ma_benes, etc.)
    """
    if df.empty:
        return df

    # Lowercase all columns first
    df.columns = [c.lower().strip() for c in df.columns]

    # Column mappings from various sources
    rename_map = {
        # Geographic Variation API columns
        'benes_total_cnt': 'total_beneficiaries',
        'benes_ffs_cnt': 'original_medicare',
        'benes_ma_cnt': 'medicare_advantage',
        'ma_prtcptn_rate': 'ma_penetration_rate',
        'bene_geo_cd': 'fips',
        'bene_geo_desc': 'geography_name',
        'bene_geo_lvl': 'geography_level',
        'bene_avg_age': 'avg_beneficiary_age',
        'bene_feml_pct': 'female_pct',
        'bene_male_pct': 'male_pct',
        'bene_dual_pct': 'dual_eligible_pct',

        # Older file column names
        'tot_benes': 'total_beneficiaries',
        'bene_total': 'total_beneficiaries',
        'total_medicare_beneficiaries': 'total_beneficiaries',
        'ma_benes': 'medicare_advantage',
        'ffs_benes': 'original_medicare',
    }

    # Apply renames where columns exist
    for old, new in rename_map.items():
        if old in df.columns:
            df = df.rename(columns={old: new})

    # Convert numeric columns
    numeric_cols = [
        'total_beneficiaries', 'original_medicare', 'medicare_advantage',
        'ma_penetration_rate', 'avg_beneficiary_age', 'female_pct', 'male_pct',
        'dual_eligible_pct'
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Convert MA penetration rate to percentage if it's a decimal (0-1)
    if 'ma_penetration_rate' in df.columns:
        max_val = df['ma_penetration_rate'].max()
        if max_val <= 1.0:
            df['ma_penetration_pct'] = (df['ma_penetration_rate'] * 100).round(1)
        else:
            df['ma_penetration_pct'] = df['ma_penetration_rate'].round(1)

    return df


# ============================================
# MEDICAID ENROLLMENT
# ============================================

def load_medicaid_state(
    state_abbr: str = "",
    period: Optional[str] = None,
    refresh: bool = False
) -> pd.DataFrame:
    """
    Load Medicaid enrollment by state.
    Uses Medicaid.gov Performance Dashboard data.
    Source: Medicaid.gov Enrollment Data
    Tier: monthly
    """
    key = cache_key("medicaid", state=state_abbr, period=period or "")

    if not refresh and is_fresh(key, tier="monthly"):
        return read_cache_df(key)

    print(f"[Hospital] Fetching Medicaid enrollment data for {state_abbr}...")

    # Check for cached CSV
    csv_path = _get_csv_path("medicaid_enrollment")
    df = None

    if _is_csv_stale(csv_path, max_age_days=30) or refresh:
        # Try multiple Medicaid data sources
        download_urls = [
            "https://www.medicaid.gov/medicaid/national-medicaid-chip-program-information/downloads/medicaid-chip-enrollment-data.csv",
            "https://data.medicaid.gov/api/1/datastore/query/6165f45b-ca93-5bb5-9d06-db29c692a360/0/download?format=csv",
        ]

        for csv_url in download_urls:
            try:
                print(f"  -> Trying: {csv_url[:50]}...")
                response = requests.get(csv_url, timeout=60)

                if response.status_code == 200:
                    df = pd.read_csv(io.StringIO(response.text), low_memory=False)
                    _save_csv_version(df, "medicaid_enrollment")
                    print(f"[OK] Downloaded Medicaid data: {len(df)} records")
                    break
            except Exception as e:
                print(f"  [WARNING] Download failed: {e}")
                continue
    else:
        df = _read_csv_safe(csv_path)

    # Filter to state
    if df is not None and not df.empty:
        # Find state column (various naming conventions)
        state_col = None
        for col in df.columns:
            if col.lower() in ('state', 'state_name', 'state_abbr', 'location'):
                state_col = col
                break

        if state_col:
            # Filter by state (handle both abbreviations and full names)
            state_upper = state_abbr.upper()
            state_name_map = {
                'AL': 'ALABAMA', 'AK': 'ALASKA', 'AZ': 'ARIZONA', 'AR': 'ARKANSAS',
                'CA': 'CALIFORNIA', 'CO': 'COLORADO', 'CT': 'CONNECTICUT', 'DE': 'DELAWARE',
                'FL': 'FLORIDA', 'GA': 'GEORGIA', 'HI': 'HAWAII', 'ID': 'IDAHO',
                'IL': 'ILLINOIS', 'IN': 'INDIANA', 'IA': 'IOWA', 'KS': 'KANSAS',
                'KY': 'KENTUCKY', 'LA': 'LOUISIANA', 'ME': 'MAINE', 'MD': 'MARYLAND',
                'MA': 'MASSACHUSETTS', 'MI': 'MICHIGAN', 'MN': 'MINNESOTA', 'MS': 'MISSISSIPPI',
                'MO': 'MISSOURI', 'MT': 'MONTANA', 'NE': 'NEBRASKA', 'NV': 'NEVADA',
                'NH': 'NEW HAMPSHIRE', 'NJ': 'NEW JERSEY', 'NM': 'NEW MEXICO', 'NY': 'NEW YORK',
                'NC': 'NORTH CAROLINA', 'ND': 'NORTH DAKOTA', 'OH': 'OHIO', 'OK': 'OKLAHOMA',
                'OR': 'OREGON', 'PA': 'PENNSYLVANIA', 'RI': 'RHODE ISLAND', 'SC': 'SOUTH CAROLINA',
                'SD': 'SOUTH DAKOTA', 'TN': 'TENNESSEE', 'TX': 'TEXAS', 'UT': 'UTAH',
                'VT': 'VERMONT', 'VA': 'VIRGINIA', 'WA': 'WASHINGTON', 'WV': 'WEST VIRGINIA',
                'WI': 'WISCONSIN', 'WY': 'WYOMING', 'DC': 'DISTRICT OF COLUMBIA'
            }
            state_full = state_name_map.get(state_upper, state_upper)

            df_filtered = df[
                (df[state_col].astype(str).str.upper() == state_upper) |
                (df[state_col].astype(str).str.upper() == state_full)
            ]

            if not df_filtered.empty:
                # Normalize columns
                df_filtered = _normalize_medicaid_columns(df_filtered.copy())
                print(f"[OK] Filtered to {len(df_filtered)} Medicaid records for {state_abbr}")
                return write_cache_df(key, df_filtered, tier="monthly")

    # TIER 3: Return empty with warning (no synthetic estimates)
    print(f"  [WARNING] Medicaid data unavailable for {state_abbr}")
    empty_df = pd.DataFrame(columns=[
        'state', 'total_enrollment', 'medicaid_enrollment',
        'chip_enrollment', 'period', 'data_source'
    ])
    return write_cache_df(key, empty_df, tier="monthly")


def _normalize_medicaid_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize Medicaid column names for consistency."""
    if df.empty:
        return df

    # Find and rename enrollment columns
    rename_map = {}
    for col in df.columns:
        col_lower = col.lower()
        if 'total' in col_lower and 'enroll' in col_lower:
            rename_map[col] = 'total_enrollment'
        elif 'medicaid' in col_lower and 'enroll' in col_lower:
            rename_map[col] = 'medicaid_enrollment'
        elif 'chip' in col_lower and 'enroll' in col_lower:
            rename_map[col] = 'chip_enrollment'
        elif col_lower in ('enrollment', 'total'):
            rename_map[col] = 'total_enrollment'

    if rename_map:
        df = df.rename(columns=rename_map)

    # Convert enrollment columns to numeric
    for col in ['total_enrollment', 'medicaid_enrollment', 'chip_enrollment']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce')

    return df


# ============================================
# BULK DOWNLOAD FUNCTION
# ============================================

def download_all_national_datasets(force: bool = False) -> Dict[str, bool]:
    """
    Download all national-level datasets as CSVs.
    Only downloads if cache is stale (>90 days) or force=True.
    Returns dict of {dataset_name: success_status}
    """
    print("\n" + "="*60)
    print("BULK DATASET DOWNLOAD")
    print("="*60 + "\n")
    
    results = {}
    
    # List of datasets to download
    datasets = [
        ("hospital_info", "https://data.cms.gov/provider-data/sites/default/files/data_dictionaries/hospital/Hospital_General_Information.csv"),
        ("hcahps", "https://data.cms.gov/provider-data/sites/default/files/data_dictionaries/hospital/HCAHPS_Hospital.csv"),
        ("medicaid_enrollment", "https://www.medicaid.gov/medicaid/national-medicaid-chip-program-information/downloads/medicaid-chip-enrollment-data.csv"),
    ]
    
    for name, url in datasets:
        csv_path = _get_csv_path(name)
        
        # Check if download needed
        if not force and not _is_csv_stale(csv_path, max_age_days=90):
            print(f"⏭️  {name}: Already fresh (skipping)")
            results[name] = True
            continue
        
        # Download
        try:
            print(f"Downloading {name}...")
            response = requests.get(url, timeout=120)
            
            if response.status_code == 200:
                df = pd.read_csv(io.StringIO(response.text), low_memory=False)
                saved_path = _save_csv_version(df, name)
                print(f"[OK] {name}: {len(df):,} records saved to {saved_path.name}")
                results[name] = True
            else:
                print(f"❌ {name}: HTTP {response.status_code}")
                results[name] = False
        except Exception as e:
            print(f"❌ {name}: {str(e)[:100]}")
            results[name] = False
    
    print("\n" + "="*60)
    print(f"DOWNLOAD COMPLETE: {sum(results.values())}/{len(results)} successful")
    print("="*60 + "\n")
    
    return results


# ============================================
# OTHER STUB FUNCTIONS
# ============================================

def load_competition(state: str = "", zip_code: Optional[str] = None, refresh: bool = False) -> pd.DataFrame:
    """Load HRSA health centers (FQHCs). Tier: monthly

    Uses HRSA Socrata API: https://data.hrsa.gov/resource/gnvw-3y4v.json
    """
    key = cache_key("hrsa_health_centers", state=state, zip=zip_code or "")
    if not refresh and is_fresh(key, tier="monthly"):
        cached = read_cache_df(key)
        if cached is not None and not cached.empty:
            return cached

    try:
        from .competition_providers import health_centers_by_geo
        df = health_centers_by_geo(state=state, zip_code=zip_code)
        if df is not None and not df.empty:
            print(f"[OK] Retrieved {len(df)} HRSA health centers for {state}")
            record_data_fetch("hrsa_health_centers", row_count=len(df), source="api")
            return write_cache_df(key, df, tier="monthly")
    except Exception as e:
        print(f"[WARNING] HRSA health center fetch failed: {e}")

    # Fallback to any cached data
    cached = read_cache_df(key)
    if cached is not None and not cached.empty:
        return cached

    return pd.DataFrame()

def load_emergencies(state: str = "", county: Optional[str] = None, refresh: bool = False) -> pd.DataFrame:
    """Load FEMA disasters. Tier: monthly"""
    key = cache_key("fema", state=state, county=county or "")
    if not refresh and is_fresh(key, tier="monthly"):
        return read_cache_df(key)
    return write_cache_df(key, pd.DataFrame(), tier="monthly")

def load_ahrf(local_path: Optional[Path] = None, county_fips: Optional[str] = None, refresh: bool = False) -> pd.DataFrame:
    """Load AHRF data. Tier: annual"""
    key = cache_key("ahrf", county=county_fips or "")
    if not refresh and is_fresh(key, tier="annual"):
        return read_cache_df(key)
    return write_cache_df(key, pd.DataFrame(), tier="annual")

def load_npi_obgyn(city: str, state: str, refresh: bool = False) -> pd.DataFrame:
    """
    Load OB/GYN providers from CMS NPI Registry API.
    API docs: https://npiregistry.cms.hhs.gov/api-page

    Args:
        city: City name to search
        state: 2-letter state abbreviation
        refresh: Force refresh from API

    Returns:
        DataFrame with OB/GYN provider data from NPI Registry
    """
    key = cache_key("npi_obgyn", city=city, state=state)
    if not refresh and is_fresh(key, tier="monthly"):
        cached = read_cache_df(key)
        if cached is not None and not cached.empty:
            return cached

    print(f"[NPI] Fetching OB/GYN providers from NPI Registry for {city}, {state}...")

    NPI_API = "https://npiregistry.cms.hhs.gov/api/"
    providers = []

    def _parse_npi_results(results):
        """Parse NPI API results into provider dicts."""
        parsed = []
        for result in results:
            basic = result.get('basic', {})
            addresses = result.get('addresses', [])
            taxonomies = result.get('taxonomies', [])

            # Get practice address (LOCATION type preferred)
            practice_addr = {}
            for addr in addresses:
                if addr.get('address_purpose', '') == 'LOCATION':
                    practice_addr = addr
                    break
            if not practice_addr and addresses:
                practice_addr = addresses[0]

            # Get primary taxonomy description
            primary_taxonomy = ''
            for tax in taxonomies:
                if tax.get('primary', False):
                    primary_taxonomy = tax.get('desc', '')
                    break
            if not primary_taxonomy and taxonomies:
                primary_taxonomy = taxonomies[0].get('desc', '')

            first_name = basic.get('first_name', '')
            last_name = basic.get('last_name', '')
            credential = basic.get('credential', '')

            name_parts = []
            if first_name:
                name_parts.append(first_name.title())
            if last_name:
                name_parts.append(last_name.title())
            provider_name = ' '.join(name_parts)
            if credential:
                provider_name += f", {credential}"

            parsed.append({
                'npi': str(result.get('number', '')),
                'provider_name': provider_name,
                'first_name': first_name.title() if first_name else '',
                'last_name': last_name.title() if last_name else '',
                'credential': credential,
                'specialty': primary_taxonomy,
                'city': practice_addr.get('city', '').title(),
                'state': practice_addr.get('state', ''),
                'zip_code': str(practice_addr.get('postal_code', ''))[:5],
                'phone': practice_addr.get('telephone_number', ''),
            })
        return parsed

    # TIER 1: Search by city + state
    try:
        params = {
            'version': '2.1',
            'taxonomy_description': 'Obstetrics & Gynecology',
            'city': city,
            'state': state,
            'limit': 200,
            'enumeration_type': 'NPI-1',  # Individual providers only
        }
        response = requests.get(NPI_API, params=params, timeout=15)

        if response.status_code == 200:
            data = response.json()
            result_count = data.get('result_count', 0)
            providers = _parse_npi_results(data.get('results', []))
            print(f"  -> City search: {len(providers)} providers (API reported {result_count})")
        else:
            print(f"  [WARNING] NPI Registry returned status {response.status_code}")
    except Exception as e:
        print(f"  [WARNING] NPI Registry API error: {e}")

    # TIER 2: If few results, try state-wide search
    if len(providers) < 5 and state:
        try:
            print(f"  -> Trying state-wide search for {state}...")
            params_state = {
                'version': '2.1',
                'taxonomy_description': 'Obstetrics & Gynecology',
                'state': state,
                'limit': 200,
                'enumeration_type': 'NPI-1',
            }
            response = requests.get(NPI_API, params=params_state, timeout=15)

            if response.status_code == 200:
                data = response.json()
                existing_npis = {p['npi'] for p in providers}
                state_providers = _parse_npi_results(data.get('results', []))
                for p in state_providers:
                    if p['npi'] not in existing_npis:
                        providers.append(p)
                print(f"  -> State-wide total: {len(providers)} providers")
        except Exception as e:
            print(f"  [WARNING] NPI state search error: {e}")

    if providers:
        df = pd.DataFrame(providers)
        print(f"[OK] Retrieved {len(df)} OB/GYN providers from NPI Registry")
        record_data_fetch("npi_obgyn", row_count=len(df), source="api")
        return write_cache_df(key, df, tier="monthly")

    # Return empty DataFrame - no sample data
    print(f"[WARNING] No OB/GYN providers found for {city}, {state}")
    return write_cache_df(key, pd.DataFrame(columns=[
        'npi', 'provider_name', 'first_name', 'last_name', 'credential',
        'specialty', 'city', 'state', 'zip_code', 'phone'
    ]), tier="monthly")


# Service line taxonomy mappings for NPI Registry lookups
SERVICE_LINE_TAXONOMIES = {
    "primary_care": [
        "Family Medicine",
        "General Practice",
        "Internal Medicine",
        "Nurse Practitioner",
        "Physician Assistant",
    ],
    "behavioral": [
        "Psychiatry",
        "Psychology",
        "Clinical Social Worker",
        "Mental Health Counselor",
        "Addiction Medicine",
    ],
    "obgyn": [
        "Obstetrics & Gynecology",
        "Midwife",
    ],
    "pediatrics": [
        "Pediatrics",
        "Pediatric Medicine",
    ],
    "radiology": [
        "Radiology",
        "Diagnostic Radiology",
    ],
    "pharmacy": [
        "Pharmacist",
        "Pharmacy",
    ],
    "dental": [
        "Dentist",
        "Dental",
        "General Practice Dentistry",
    ],
}


def load_npi_service_line(
    service_line: str,
    city: str,
    state: str,
    refresh: bool = False,
    limit: int = 200,
) -> pd.DataFrame:
    """
    Load providers for a specific service line from CMS NPI Registry API.

    Args:
        service_line: One of primary_care, behavioral, obgyn, pediatrics,
                      radiology, pharmacy, dental
        city: City name to search
        state: 2-letter state abbreviation
        refresh: Force refresh from API
        limit: Max results per taxonomy search

    Returns:
        DataFrame with provider data including lat/lon if available
    """
    key = cache_key(f"npi_{service_line}", city=city, state=state)
    if not refresh and is_fresh(key, tier="monthly"):
        cached = read_cache_df(key)
        if cached is not None and not cached.empty:
            return cached

    taxonomies = SERVICE_LINE_TAXONOMIES.get(service_line, [])
    if not taxonomies:
        print(f"[WARNING] Unknown service line: {service_line}")
        return pd.DataFrame()

    print(f"[NPI] Fetching {service_line} providers for {city}, {state}...")

    NPI_API = "https://npiregistry.cms.hhs.gov/api/"
    all_providers = []
    seen_npis = set()

    def _parse_results(results):
        parsed = []
        for result in results:
            npi = str(result.get('number', ''))
            if npi in seen_npis:
                continue
            seen_npis.add(npi)

            basic = result.get('basic', {})
            addresses = result.get('addresses', [])
            tax_list = result.get('taxonomies', [])

            # Get practice address (LOCATION type preferred)
            practice_addr = {}
            for addr in addresses:
                if addr.get('address_purpose', '') == 'LOCATION':
                    practice_addr = addr
                    break
            if not practice_addr and addresses:
                practice_addr = addresses[0]

            # Get primary taxonomy
            primary_tax = ''
            for t in tax_list:
                if t.get('primary', False):
                    primary_tax = t.get('desc', '')
                    break
            if not primary_tax and tax_list:
                primary_tax = tax_list[0].get('desc', '')

            first_name = basic.get('first_name', '') or ''
            last_name = basic.get('last_name', '') or ''
            org_name = basic.get('organization_name', '') or ''
            credential = basic.get('credential', '') or ''

            if org_name:
                provider_name = org_name.title()
            else:
                name_parts = []
                if first_name:
                    name_parts.append(first_name.title())
                if last_name:
                    name_parts.append(last_name.title())
                provider_name = ' '.join(name_parts)
                if credential:
                    provider_name += f", {credential}"

            # Extract lat/lon if available (newer NPI records may have geocoded addresses)
            lat_val = practice_addr.get('latitude')
            lon_val = practice_addr.get('longitude')

            # Build full address for geocoding fallback
            addr_parts = []
            if practice_addr.get('address_1'):
                addr_parts.append(practice_addr['address_1'])
            if practice_addr.get('city'):
                addr_parts.append(practice_addr['city'])
            if practice_addr.get('state'):
                addr_parts.append(practice_addr['state'])
            full_address = ', '.join(addr_parts)

            parsed.append({
                'npi': npi,
                'provider_name': provider_name,
                'specialty': primary_tax,
                'service_line': service_line,
                'city': (practice_addr.get('city') or '').title(),
                'state': practice_addr.get('state', ''),
                'zip_code': str(practice_addr.get('postal_code', ''))[:5],
                'phone': practice_addr.get('telephone_number', ''),
                'address': full_address,
                'lat': float(lat_val) if lat_val else None,
                'lon': float(lon_val) if lon_val else None,
            })
        return parsed

    # Search each taxonomy
    for taxonomy in taxonomies:
        try:
            params = {
                'version': '2.1',
                'taxonomy_description': taxonomy,
                'city': city,
                'state': state,
                'limit': limit,
            }
            response = requests.get(NPI_API, params=params, timeout=15)
            if response.status_code == 200:
                data = response.json()
                results = data.get('results', [])
                parsed = _parse_results(results)
                all_providers.extend(parsed)
                print(f"  -> {taxonomy}: {len(parsed)} new providers")
        except Exception as e:
            print(f"  [WARNING] NPI search for '{taxonomy}' failed: {e}")

    if all_providers:
        df = pd.DataFrame(all_providers)
        print(f"[OK] Retrieved {len(df)} {service_line} providers")
        record_data_fetch(f"npi_{service_line}", row_count=len(df), source="api")
        return write_cache_df(key, df, tier="monthly")

    print(f"[WARNING] No {service_line} providers found for {city}, {state}")
    return write_cache_df(key, pd.DataFrame(columns=[
        'npi', 'provider_name', 'specialty', 'service_line', 'city', 'state',
        'zip_code', 'phone', 'address', 'lat', 'lon'
    ]), tier="monthly")


# ============================================
# SPATIAL ANALYSIS
# ============================================

def nearest_minutes(origin_points: pd.DataFrame, destination_points: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    """Calculate travel times using Haversine distance."""
    if destination_points is None or destination_points.empty:
        return None
    
    from math import radians, cos, sin, asin, sqrt
    
    def haversine(lon1, lat1, lon2, lat2):
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * asin(sqrt(a))
        miles = 3956 * c
        return miles
    
    results = []
    for idx, origin in origin_points.iterrows():
        distances = []
        for dest_idx, dest in destination_points.iterrows():
            dist = haversine(origin['lon'], origin['lat'], dest['lon'], dest['lat'])
            distances.append({'origin': idx, 'destination': dest_idx, 'miles': dist, 'minutes': dist / 60 * 60})
        distances.sort(key=lambda x: x['miles'])
        results.extend(distances[:5])
    
    return pd.DataFrame(results)

def load_isochrones(origin_points: pd.DataFrame, minutes_list: List[int] = [15, 30, 45]) -> Optional[Dict[str, Any]]:
    """Generate isochrone polygons."""
    if origin_points.empty:
        return None
    
    features = []
    for minutes in minutes_list:
        radius_miles = minutes * 0.6
        radius_degrees = radius_miles / 69
        center_lat = origin_points.iloc[0]['lat']
        center_lon = origin_points.iloc[0]['lon']
        
        import math
        points = []
        for i in range(36):
            angle = math.radians(i * 10)
            lat = center_lat + (radius_degrees * math.cos(angle))
            lon = center_lon + (radius_degrees * math.sin(angle) / math.cos(math.radians(center_lat)))
            points.append([lon, lat])
        points.append(points[0])
        
        feature = {
            "type": "Feature",
            "properties": {"minutes": minutes, "time": minutes, "value": minutes},
            "geometry": {"type": "Polygon", "coordinates": [points]}
        }
        features.append(feature)
    
    return {"type": "FeatureCollection", "features": features}


# ============================================
# ALIAS FUNCTIONS FOR PAGE COMPATIBILITY
# ============================================

def load_census_population(
    state_fips: Optional[str] = None,
    county_fips: Optional[str] = None,
    refresh: bool = False
) -> pd.DataFrame:
    """
    Alias for load_population - loads Census ACS population data.

    Args:
        state_fips: 2-digit state FIPS code
        county_fips: 3-digit county FIPS code (or 5-digit full FIPS)
        refresh: Force refresh from API

    Returns:
        DataFrame with population demographics
    """
    return load_population(state_fips=state_fips, county_fips=county_fips, refresh=refresh)


def _geocode_hospitals_by_zip(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add latitude/longitude to hospital data using zip code geocoding.
    Uses zippopotam.us API (fast, free, no API key) with local caching.
    """
    import json
    from concurrent.futures import ThreadPoolExecutor, as_completed

    # Find zip code column
    zip_col = None
    for col in df.columns:
        if 'zip' in col.lower():
            zip_col = col
            break

    if zip_col is None:
        print("No zip code column found for geocoding")
        return df

    # Get unique zip codes to minimize API calls
    unique_zips = df[zip_col].astype(str).str[:5].unique()
    zip_coords = {}

    print(f"Geocoding {len(unique_zips)} unique zip codes...")

    # Try to load cached zip coordinates
    zip_cache_path = settings.cache_dir / "zip_coordinates.json"
    if zip_cache_path.exists():
        try:
            with open(zip_cache_path, 'r') as f:
                zip_coords = json.load(f)
            print(f"  Loaded {len(zip_coords)} cached zip coordinates")
        except:
            pass

    # Find zip codes that need geocoding
    missing_zips = [z for z in unique_zips if z not in zip_coords and z.isdigit()]

    def geocode_zip(zip_code):
        """Geocode a single zip code using zippopotam.us API"""
        try:
            r = requests.get(f"http://api.zippopotam.us/us/{zip_code}", timeout=5)
            if r.status_code == 200:
                data = r.json()
                places = data.get('places', [])
                if places:
                    return zip_code, {
                        'lat': float(places[0].get('latitude', 0)),
                        'lon': float(places[0].get('longitude', 0))
                    }
        except:
            pass
        return zip_code, None

    # Geocode missing zip codes in parallel for speed
    if missing_zips:
        print(f"  Fetching {len(missing_zips)} new zip coordinates...")
        new_geocodes = 0

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(geocode_zip, z): z for z in missing_zips}
            for future in as_completed(futures):
                zip_code, coords = future.result()
                if coords and coords['lat'] != 0:
                    zip_coords[zip_code] = coords
                    new_geocodes += 1

        # Save updated cache
        if new_geocodes > 0:
            try:
                with open(zip_cache_path, 'w') as f:
                    json.dump(zip_coords, f)
                print(f"  Cached {new_geocodes} new zip coordinates")
            except:
                pass

    # Add coordinates to dataframe
    df = df.copy()
    df['latitude'] = df[zip_col].astype(str).str[:5].map(lambda z: zip_coords.get(z, {}).get('lat'))
    df['longitude'] = df[zip_col].astype(str).str[:5].map(lambda z: zip_coords.get(z, {}).get('lon'))

    geocoded_count = df['latitude'].notna().sum()
    print(f"[OK] Geocoded {geocoded_count}/{len(df)} hospitals")

    return df


def load_hospitals(
    lat: Optional[float] = None,
    lon: Optional[float] = None,
    radius_miles: float = 50,
    state: Optional[str] = None,
    refresh: bool = False
) -> pd.DataFrame:
    """
    Load hospitals with optional geographic filtering.

    Args:
        lat: Latitude of search center
        lon: Longitude of search center
        radius_miles: Search radius in miles (default 50)
        state: State abbreviation filter
        refresh: Force refresh from API

    Returns:
        DataFrame with hospital data, optionally filtered by location
    """
    from math import radians, cos, sin, asin, sqrt

    # Load hospital data
    df = load_hospital_info(state=state, refresh=refresh)

    if df.empty:
        return df

    # If no lat/lon provided, return all data for the state
    if lat is None or lon is None:
        return df

    # Define haversine function for distance calculation
    def haversine(lon1, lat1, lon2, lat2):
        """Calculate great circle distance in miles."""
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * asin(sqrt(a))
        miles = 3956 * c
        return miles

    # Find latitude/longitude columns
    lat_col = None
    lon_col = None
    for col in df.columns:
        col_lower = col.lower()
        if 'lat' in col_lower and lat_col is None:
            lat_col = col
        if 'lon' in col_lower and lon_col is None:
            lon_col = col

    if lat_col is None or lon_col is None:
        print("Hospital data does not have lat/lon columns - attempting to geocode via zip codes...")
        df = _geocode_hospitals_by_zip(df)
        # Try to find lat/lon columns again after geocoding
        for col in df.columns:
            col_lower = col.lower()
            if 'lat' in col_lower and lat_col is None:
                lat_col = col
            if 'lon' in col_lower and lon_col is None:
                lon_col = col
        if lat_col is None or lon_col is None:
            print("Warning: Could not geocode hospitals, returning all results")
            return df

    # Convert to numeric
    df[lat_col] = pd.to_numeric(df[lat_col], errors='coerce')
    df[lon_col] = pd.to_numeric(df[lon_col], errors='coerce')

    # Filter out rows without valid coordinates
    valid_coords = df[lat_col].notna() & df[lon_col].notna()
    df_with_coords = df[valid_coords].copy()

    # Calculate distances
    df_with_coords['distance_miles'] = df_with_coords.apply(
        lambda row: haversine(lon, lat, row[lon_col], row[lat_col]),
        axis=1
    )

    # Filter by radius
    result = df_with_coords[df_with_coords['distance_miles'] <= radius_miles].copy()
    result = result.sort_values('distance_miles')

    return result


def load_hospital_quality(
    lat: Optional[float] = None,
    lon: Optional[float] = None,
    radius_miles: float = 50,
    state: Optional[str] = None,
    refresh: bool = False
) -> pd.DataFrame:
    """
    Load hospital quality ratings with star ratings.
    This is an alias for load_hospitals with quality data included.

    Args:
        lat: Latitude of search center
        lon: Longitude of search center
        radius_miles: Search radius in miles
        state: State abbreviation filter
        refresh: Force refresh from API

    Returns:
        DataFrame with hospital quality ratings
    """
    # Load hospital info which includes quality ratings
    df = load_hospitals(lat=lat, lon=lon, radius_miles=radius_miles, state=state, refresh=refresh)

    if df.empty:
        return df

    # The CMS hospital data includes hospital_overall_rating column
    # If it doesn't exist, add a placeholder
    if 'hospital_overall_rating' not in df.columns:
        df['hospital_overall_rating'] = None

    return df


def load_complications(
    state: Optional[str] = None,
    state_abbr: Optional[str] = None,
    refresh: bool = False
) -> pd.DataFrame:
    """
    Load complications and outcomes data from CMS.

    Args:
        state: State abbreviation (e.g., 'CA')
        state_abbr: Alias for state parameter
        refresh: Force refresh from API

    Returns:
        DataFrame with complications and outcomes measures
    """
    # Use state_abbr if state not provided
    if state is None and state_abbr:
        state = state_abbr

    return load_quality_outcomes(state=state, refresh=refresh)


# ============================================
# ADDITIONAL ALIAS FUNCTIONS
# ============================================

def load_cbp(
    state_fips: Optional[str] = None,
    county_fips: Optional[str] = None,
    year: str = "2022",
    naics: str = "00",
    refresh: bool = False
) -> pd.DataFrame:
    """
    Alias for load_industry - loads County Business Patterns data.

    Args:
        state_fips: 2-digit state FIPS code
        county_fips: 3-digit county FIPS code
        year: Data year
        naics: NAICS industry code
        refresh: Force refresh from API

    Returns:
        DataFrame with industry/employment data
    """
    return load_industry(
        year=year,
        state_fips=state_fips,
        county_fips=county_fips,
        naics=naics,
        refresh=refresh
    )


# ============================================================================
# PHASE 4: SUPPLEMENTARY DATA SOURCES
# ============================================================================

def load_healthcare_cbp(
    state_fips: Optional[str] = None,
    county_fips: Optional[str] = None,
    year: str = "2021",
    refresh: bool = False
) -> pd.DataFrame:
    """
    Load healthcare-specific County Business Patterns (NAICS 62).

    Provides establishment counts, employment, and payroll for:
    - Ambulatory Care (621)
    - Hospitals (622)
    - Nursing/Residential Care (623)
    - Social Assistance (624)

    Args:
        state_fips: 2-digit state FIPS code
        county_fips: 3-digit county FIPS code
        year: Data year (default 2021)
        refresh: Force refresh from API

    Returns:
        DataFrame with healthcare employment by subsector
    """
    from .industry_cbp import fetch_healthcare_cbp
    return fetch_healthcare_cbp(
        state_fips=state_fips or "",
        county_fips=county_fips or "",
        year=year,
        refresh=refresh
    )


def load_ruca(
    state_fips: Optional[str] = None,
    county_fips: Optional[str] = None,
    zip_code: Optional[str] = None,
    refresh: bool = False
) -> pd.DataFrame:
    """
    Load RUCA (Rural-Urban Commuting Area) codes.

    Classifies areas as urban/suburban/rural based on commuting patterns.
    Data from USDA Economic Research Service.

    Args:
        state_fips: 2-digit state FIPS (for county-level query)
        county_fips: 3-digit county FIPS (for county-level query)
        zip_code: 5-digit ZIP code (alternative to county query)
        refresh: Force refresh

    Returns:
        DataFrame with RUCA codes and classifications
    """
    from .hpsa_mua import fetch_ruca_data, fetch_ruca_by_zip

    if zip_code:
        return fetch_ruca_by_zip(refresh=refresh)
    else:
        return fetch_ruca_data(refresh=refresh)


def get_ruca_summary(
    state_fips: str,
    county_fips: str
) -> dict:
    """
    Get RUCA summary for a county.

    Args:
        state_fips: 2-digit state FIPS
        county_fips: 3-digit county FIPS

    Returns:
        Dict with urban/rural classification breakdown
    """
    from .hpsa_mua import get_ruca_for_county
    return get_ruca_for_county(state_fips, county_fips)


def load_ihs_facilities(
    lat: Optional[float] = None,
    lon: Optional[float] = None,
    state: Optional[str] = None,
    radius_miles: float = 50,
    facility_types: Optional[list] = None,
    refresh: bool = False
) -> pd.DataFrame:
    """
    Load IHS (Indian Health Service) facility data.

    Args:
        lat: Latitude for proximity search
        lon: Longitude for proximity search
        state: State abbreviation filter
        radius_miles: Search radius in miles
        facility_types: Filter by type (IHS, Tribal, Urban)
        refresh: Force refresh

    Returns:
        DataFrame with IHS facilities
    """
    from .ihs_data import find_ihs_facilities_near, get_ihs_facilities_by_state

    if lat is not None and lon is not None:
        return find_ihs_facilities_near(
            lat=lat,
            lon=lon,
            radius_miles=radius_miles,
            facility_types=facility_types,
            refresh=refresh
        )
    elif state:
        return get_ihs_facilities_by_state(state, refresh=refresh)
    else:
        # Return all facilities
        from .ihs_data import IHS_FACILITIES_SEED
        return pd.DataFrame(IHS_FACILITIES_SEED)