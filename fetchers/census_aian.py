"""
Census ACS American Indian/Alaska Native (AI/AN) Demographics Module

Data Sources:
- Census Bureau American Community Survey (ACS) 5-Year Estimates
- AI/AN specific tables: B02001, B01001C, B27001

Variables:
- B02001_004E: AI/AN alone
- B02001_005E: AI/AN in combination
- B01001C_*: AI/AN age/sex breakdown
- B27001_*: Health insurance coverage (filtered for AI/AN)
"""
from __future__ import annotations
import pandas as pd
import requests
import os
import io
from typing import Optional, Dict, List, Any

# Census API configuration
ACS_YEAR = os.environ.get("ACS_YEAR", "2023")
ACS_BASE = f"https://api.census.gov/data/{ACS_YEAR}/acs/acs5"

# AI/AN Population variables
AIAN_POPULATION_VARS = {
    "B02001_001E": "total_population",
    "B02001_004E": "aian_alone",
    "B02001_005E": "asian_alone",  # For context
    "B02001_006E": "nhpi_alone",   # For context
}

# Detailed AI/AN Race variables (alone or in combination)
AIAN_DETAILED_VARS = {
    "B02010_001E": "aian_alone_or_combo_total",
}

# AI/AN Age/Sex breakdown (Table B01001C - AI/AN Alone)
AIAN_AGE_SEX_VARS = {
    "B01001C_001E": "aian_total",
    "B01001C_002E": "aian_male_total",
    "B01001C_003E": "aian_male_under5",
    "B01001C_004E": "aian_male_5to9",
    "B01001C_005E": "aian_male_10to14",
    "B01001C_006E": "aian_male_15to17",
    "B01001C_007E": "aian_male_18to19",
    "B01001C_008E": "aian_male_20to24",
    "B01001C_009E": "aian_male_25to29",
    "B01001C_010E": "aian_male_30to34",
    "B01001C_011E": "aian_male_35to44",
    "B01001C_012E": "aian_male_45to54",
    "B01001C_013E": "aian_male_55to64",
    "B01001C_014E": "aian_male_65to74",
    "B01001C_015E": "aian_male_75to84",
    "B01001C_016E": "aian_male_85plus",
    "B01001C_017E": "aian_female_total",
    "B01001C_018E": "aian_female_under5",
    "B01001C_019E": "aian_female_5to9",
    "B01001C_020E": "aian_female_10to14",
    "B01001C_021E": "aian_female_15to17",
    "B01001C_022E": "aian_female_18to19",
    "B01001C_023E": "aian_female_20to24",
    "B01001C_024E": "aian_female_25to29",
    "B01001C_025E": "aian_female_30to34",
    "B01001C_026E": "aian_female_35to44",
    "B01001C_027E": "aian_female_45to54",
    "B01001C_028E": "aian_female_55to64",
    "B01001C_029E": "aian_female_65to74",
    "B01001C_030E": "aian_female_75to84",
    "B01001C_031E": "aian_female_85plus",
}

# Health Insurance variables (B27001 - overall, need to calculate AI/AN specific)
# Note: B27001 is total population; for AI/AN specific, use C27001C
AIAN_INSURANCE_VARS = {
    "C27001C_001E": "aian_insurance_universe",
    "C27001C_002E": "aian_under19_total",
    "C27001C_003E": "aian_under19_with_insurance",
    "C27001C_004E": "aian_under19_no_insurance",
    "C27001C_005E": "aian_19to64_total",
    "C27001C_006E": "aian_19to64_with_insurance",
    "C27001C_007E": "aian_19to64_no_insurance",
    "C27001C_008E": "aian_65plus_total",
    "C27001C_009E": "aian_65plus_with_insurance",
    "C27001C_010E": "aian_65plus_no_insurance",
}

# Poverty status for AI/AN (Table B17001C)
AIAN_POVERTY_VARS = {
    "B17001C_001E": "aian_poverty_universe",
    "B17001C_002E": "aian_below_poverty",
}

# Educational attainment for AI/AN (Table C15002C)
AIAN_EDUCATION_VARS = {
    "C15002C_001E": "aian_edu_universe",
    "C15002C_002E": "aian_edu_male_total",
    "C15002C_006E": "aian_edu_male_bachelors_plus",
    "C15002C_007E": "aian_edu_female_total",
    "C15002C_011E": "aian_edu_female_bachelors_plus",
}


def _census_csv(params: Dict) -> pd.DataFrame:
    """Fetch Census API data as CSV."""
    url = ACS_BASE
    try:
        r = requests.get(url, params=params, timeout=60)
        r.raise_for_status()
        return pd.read_json(io.StringIO(r.text))
    except Exception as e:
        print(f"Census API error: {e}")
        return pd.DataFrame()


def _fetch_acs_vars(vars_dict: Dict[str, str], geography: str,
                    state_fips: Optional[str] = None,
                    county_fips: Optional[str] = None) -> pd.DataFrame:
    """
    Generic function to fetch ACS variables.

    Args:
        vars_dict: Dictionary of variable codes to friendly names
        geography: 'county', 'state', or 'tract'
        state_fips: 2-digit state FIPS
        county_fips: 3-digit county FIPS (within state)

    Returns:
        DataFrame with requested variables
    """
    var_list = ["NAME"] + list(vars_dict.keys())

    params = {"get": ",".join(var_list)}

    if geography == "county" and state_fips and county_fips:
        params["for"] = f"county:{county_fips}"
        params["in"] = f"state:{state_fips}"
    elif geography == "county" and state_fips:
        params["for"] = "county:*"
        params["in"] = f"state:{state_fips}"
    elif geography == "state" and state_fips:
        params["for"] = f"state:{state_fips}"
    elif geography == "state":
        params["for"] = "state:*"
    else:
        return pd.DataFrame()

    df = _census_csv(params)

    if df.empty:
        return df

    # First row is headers
    if len(df) > 0:
        df.columns = df.iloc[0]
        df = df.iloc[1:].reset_index(drop=True)

    # Rename columns to friendly names
    rename_map = {"NAME": "name"}
    rename_map.update(vars_dict)
    df = df.rename(columns=rename_map)

    # Convert numeric columns
    for col in vars_dict.values():
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def get_aian_population(state_fips: str, county_fips: Optional[str] = None) -> pd.DataFrame:
    """
    Get AI/AN population counts for a county or state.

    Args:
        state_fips: 2-digit state FIPS code
        county_fips: 3-digit county FIPS code (optional)

    Returns:
        DataFrame with AI/AN population data
    """
    geography = "county" if county_fips else "state"
    df = _fetch_acs_vars(AIAN_POPULATION_VARS, geography, state_fips, county_fips)

    if df.empty:
        return df

    # Calculate derived metrics
    if "aian_alone" in df.columns and "total_population" in df.columns:
        df["aian_alone_pct"] = (df["aian_alone"] / df["total_population"] * 100).round(2)

    return df


def get_aian_age_sex(state_fips: str, county_fips: Optional[str] = None) -> pd.DataFrame:
    """
    Get AI/AN age and sex breakdown.

    Args:
        state_fips: 2-digit state FIPS code
        county_fips: 3-digit county FIPS code (optional)

    Returns:
        DataFrame with AI/AN age/sex breakdown
    """
    geography = "county" if county_fips else "state"
    df = _fetch_acs_vars(AIAN_AGE_SEX_VARS, geography, state_fips, county_fips)

    if df.empty:
        return df

    # Calculate age group aggregates
    if "aian_total" in df.columns:
        # Under 18
        under18_male_cols = ["aian_male_under5", "aian_male_5to9", "aian_male_10to14", "aian_male_15to17"]
        under18_female_cols = ["aian_female_under5", "aian_female_5to9", "aian_female_10to14", "aian_female_15to17"]
        existing_male = [c for c in under18_male_cols if c in df.columns]
        existing_female = [c for c in under18_female_cols if c in df.columns]
        df["aian_under18"] = df[existing_male + existing_female].sum(axis=1)

        # 65+
        senior_male_cols = ["aian_male_65to74", "aian_male_75to84", "aian_male_85plus"]
        senior_female_cols = ["aian_female_65to74", "aian_female_75to84", "aian_female_85plus"]
        existing_senior_m = [c for c in senior_male_cols if c in df.columns]
        existing_senior_f = [c for c in senior_female_cols if c in df.columns]
        df["aian_65plus"] = df[existing_senior_m + existing_senior_f].sum(axis=1)

        # Working age (18-64)
        df["aian_18to64"] = df["aian_total"] - df["aian_under18"] - df["aian_65plus"]

        # Percentages
        df["aian_under18_pct"] = (df["aian_under18"] / df["aian_total"] * 100).round(1)
        df["aian_65plus_pct"] = (df["aian_65plus"] / df["aian_total"] * 100).round(1)

    return df


def get_aian_insurance(state_fips: str, county_fips: Optional[str] = None) -> pd.DataFrame:
    """
    Get AI/AN health insurance coverage data.

    Args:
        state_fips: 2-digit state FIPS code
        county_fips: 3-digit county FIPS code (optional)

    Returns:
        DataFrame with AI/AN insurance coverage data
    """
    geography = "county" if county_fips else "state"
    df = _fetch_acs_vars(AIAN_INSURANCE_VARS, geography, state_fips, county_fips)

    if df.empty:
        return df

    # Calculate uninsured totals and rates
    uninsured_cols = ["aian_under19_no_insurance", "aian_19to64_no_insurance", "aian_65plus_no_insurance"]
    existing = [c for c in uninsured_cols if c in df.columns]
    if existing:
        df["aian_uninsured_total"] = df[existing].sum(axis=1)

    if "aian_insurance_universe" in df.columns and "aian_uninsured_total" in df.columns:
        df["aian_uninsured_rate"] = (df["aian_uninsured_total"] / df["aian_insurance_universe"] * 100).round(1)
        df["aian_insured_rate"] = 100 - df["aian_uninsured_rate"]

    # Age-specific uninsured rates
    if "aian_under19_total" in df.columns and "aian_under19_no_insurance" in df.columns:
        df["aian_under19_uninsured_rate"] = (
            df["aian_under19_no_insurance"] / df["aian_under19_total"] * 100
        ).round(1)

    if "aian_19to64_total" in df.columns and "aian_19to64_no_insurance" in df.columns:
        df["aian_19to64_uninsured_rate"] = (
            df["aian_19to64_no_insurance"] / df["aian_19to64_total"] * 100
        ).round(1)

    if "aian_65plus_total" in df.columns and "aian_65plus_no_insurance" in df.columns:
        df["aian_65plus_uninsured_rate"] = (
            df["aian_65plus_no_insurance"] / df["aian_65plus_total"] * 100
        ).round(1)

    return df


def get_aian_poverty(state_fips: str, county_fips: Optional[str] = None) -> pd.DataFrame:
    """
    Get AI/AN poverty status data.

    Args:
        state_fips: 2-digit state FIPS code
        county_fips: 3-digit county FIPS code (optional)

    Returns:
        DataFrame with AI/AN poverty data
    """
    geography = "county" if county_fips else "state"
    df = _fetch_acs_vars(AIAN_POVERTY_VARS, geography, state_fips, county_fips)

    if df.empty:
        return df

    if "aian_poverty_universe" in df.columns and "aian_below_poverty" in df.columns:
        df["aian_poverty_rate"] = (df["aian_below_poverty"] / df["aian_poverty_universe"] * 100).round(1)

    return df


def get_aian_education(state_fips: str, county_fips: Optional[str] = None) -> pd.DataFrame:
    """
    Get AI/AN educational attainment data.

    Args:
        state_fips: 2-digit state FIPS code
        county_fips: 3-digit county FIPS code (optional)

    Returns:
        DataFrame with AI/AN education data
    """
    geography = "county" if county_fips else "state"
    df = _fetch_acs_vars(AIAN_EDUCATION_VARS, geography, state_fips, county_fips)

    if df.empty:
        return df

    # Calculate bachelor's+ rate
    bachelors_cols = ["aian_edu_male_bachelors_plus", "aian_edu_female_bachelors_plus"]
    existing = [c for c in bachelors_cols if c in df.columns]
    if existing:
        df["aian_bachelors_plus"] = df[existing].sum(axis=1)

    if "aian_edu_universe" in df.columns and "aian_bachelors_plus" in df.columns:
        df["aian_bachelors_plus_rate"] = (df["aian_bachelors_plus"] / df["aian_edu_universe"] * 100).round(1)

    return df


def get_aian_comprehensive(state_fips: str, county_fips: Optional[str] = None) -> Dict[str, Any]:
    """
    Get comprehensive AI/AN demographics summary.

    Args:
        state_fips: 2-digit state FIPS code
        county_fips: 3-digit county FIPS code (optional)

    Returns:
        Dict with comprehensive AI/AN demographic data
    """
    pop_df = get_aian_population(state_fips, county_fips)
    age_df = get_aian_age_sex(state_fips, county_fips)
    ins_df = get_aian_insurance(state_fips, county_fips)
    pov_df = get_aian_poverty(state_fips, county_fips)
    edu_df = get_aian_education(state_fips, county_fips)

    result = {
        "state_fips": state_fips,
        "county_fips": county_fips,
        "name": None,
        "population": {},
        "age_distribution": {},
        "insurance": {},
        "poverty": {},
        "education": {},
    }

    # Extract population data
    if not pop_df.empty:
        row = pop_df.iloc[0]
        result["name"] = row.get("name", "")
        result["population"] = {
            "total": int(row.get("total_population", 0) or 0),
            "aian_alone": int(row.get("aian_alone", 0) or 0),
            "aian_alone_pct": float(row.get("aian_alone_pct", 0) or 0),
        }

    # Extract age data
    if not age_df.empty:
        row = age_df.iloc[0]
        result["age_distribution"] = {
            "total": int(row.get("aian_total", 0) or 0),
            "under18": int(row.get("aian_under18", 0) or 0),
            "under18_pct": float(row.get("aian_under18_pct", 0) or 0),
            "age_18to64": int(row.get("aian_18to64", 0) or 0),
            "age_65plus": int(row.get("aian_65plus", 0) or 0),
            "age_65plus_pct": float(row.get("aian_65plus_pct", 0) or 0),
            "male_total": int(row.get("aian_male_total", 0) or 0),
            "female_total": int(row.get("aian_female_total", 0) or 0),
        }

    # Extract insurance data
    if not ins_df.empty:
        row = ins_df.iloc[0]
        result["insurance"] = {
            "universe": int(row.get("aian_insurance_universe", 0) or 0),
            "uninsured_total": int(row.get("aian_uninsured_total", 0) or 0),
            "uninsured_rate": float(row.get("aian_uninsured_rate", 0) or 0),
            "insured_rate": float(row.get("aian_insured_rate", 0) or 0),
            "under19_uninsured_rate": float(row.get("aian_under19_uninsured_rate", 0) or 0),
            "age_19to64_uninsured_rate": float(row.get("aian_19to64_uninsured_rate", 0) or 0),
            "age_65plus_uninsured_rate": float(row.get("aian_65plus_uninsured_rate", 0) or 0),
        }

    # Extract poverty data
    if not pov_df.empty:
        row = pov_df.iloc[0]
        result["poverty"] = {
            "universe": int(row.get("aian_poverty_universe", 0) or 0),
            "below_poverty": int(row.get("aian_below_poverty", 0) or 0),
            "poverty_rate": float(row.get("aian_poverty_rate", 0) or 0),
        }

    # Extract education data
    if not edu_df.empty:
        row = edu_df.iloc[0]
        result["education"] = {
            "universe": int(row.get("aian_edu_universe", 0) or 0),
            "bachelors_plus": int(row.get("aian_bachelors_plus", 0) or 0),
            "bachelors_plus_rate": float(row.get("aian_bachelors_plus_rate", 0) or 0),
        }

    return result


def compare_aian_to_total(state_fips: str, county_fips: Optional[str] = None) -> Dict[str, Any]:
    """
    Compare AI/AN demographics to total population demographics.

    Args:
        state_fips: 2-digit state FIPS code
        county_fips: 3-digit county FIPS code (optional)

    Returns:
        Dict with comparison metrics showing disparities
    """
    # Get AI/AN data
    aian = get_aian_comprehensive(state_fips, county_fips)

    # Get total population insurance/poverty (would need additional queries)
    # For now, return AI/AN data with national benchmarks
    national_benchmarks = {
        "total_uninsured_rate": 8.0,  # Approximate US average
        "total_poverty_rate": 11.5,   # Approximate US average
        "total_bachelors_rate": 33.0, # Approximate US average
    }

    result = {
        "aian_data": aian,
        "benchmarks": national_benchmarks,
        "disparities": {},
    }

    # Calculate disparities
    if aian["insurance"].get("uninsured_rate"):
        result["disparities"]["insurance_gap"] = round(
            aian["insurance"]["uninsured_rate"] - national_benchmarks["total_uninsured_rate"], 1
        )

    if aian["poverty"].get("poverty_rate"):
        result["disparities"]["poverty_gap"] = round(
            aian["poverty"]["poverty_rate"] - national_benchmarks["total_poverty_rate"], 1
        )

    if aian["education"].get("bachelors_plus_rate"):
        result["disparities"]["education_gap"] = round(
            aian["education"]["bachelors_plus_rate"] - national_benchmarks["total_bachelors_rate"], 1
        )

    return result


def get_states_with_highest_aian_population(top_n: int = 10) -> pd.DataFrame:
    """
    Get states with highest AI/AN population.

    Args:
        top_n: Number of states to return

    Returns:
        DataFrame with top states by AI/AN population
    """
    df = _fetch_acs_vars(AIAN_POPULATION_VARS, "state")

    if df.empty:
        return df

    df = df.sort_values("aian_alone", ascending=False).head(top_n)
    return df[["name", "aian_alone", "total_population", "aian_alone_pct"]]
