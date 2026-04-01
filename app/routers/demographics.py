"""Demographics endpoint — Census population + AI/AN data."""

import logging
from fastapi import APIRouter, Depends, HTTPException

from app.auth.dependencies import get_current_user, enforce_fips_access
from app.auth.jwt_validator import TokenClaims
from app.schemas.responses import (
    DemographicsResponse, PopulationData, AIANData,
    AIANPopulation, AIANInsurance, AIANPoverty, AIANEducation,
    DataSource,
)

router = APIRouter(prefix="/v1/demographics", tags=["demographics"])
log = logging.getLogger("iha.api.demographics")


@router.get("/{state_fips}/{county_fips}", response_model=DemographicsResponse)
async def get_demographics(
    state_fips: str,
    county_fips: str,
    user: TokenClaims = Depends(get_current_user),
):
    """Population, age, income, poverty, and AI/AN demographics for a county."""
    fips = f"{state_fips}{county_fips}"
    enforce_fips_access(user, fips)

    # -- General population --
    pop_data = PopulationData()
    try:
        from fetchers.fetchers import load_population
        df = load_population(state_fips=state_fips, county_fips=county_fips)
        if df is not None and not df.empty:
            row = df.iloc[0]
            pop_data = PopulationData(
                total=_safe_int(row, "population_total"),
                median_age=_safe_float(row, "median_age"),
                median_household_income=_safe_float(row, "median_household_income"),
                poverty_rate=_safe_float(row, "poverty_rate"),
                age_under_18=_safe_int(row, "age_under_18"),
                age_under_18_pct=_safe_float(row, "pct_under_18"),
                age_18_64=_safe_int(row, "age_18_64"),
                age_18_64_pct=_safe_float(row, "pct_18_64"),
                age_65_plus=_safe_int(row, "age_65_plus"),
                age_65_plus_pct=_safe_float(row, "pct_65_plus"),
            )
    except Exception as e:
        log.warning("Population fetch failed: %s", e)

    # -- AI/AN data --
    aian = AIANData()
    try:
        from fetchers.census_aian import get_aian_comprehensive
        result = get_aian_comprehensive(state_fips, county_fips)
        if result:
            pop = result.get("population", {})
            ins = result.get("insurance", {})
            pov = result.get("poverty", {})
            edu = result.get("education", {})
            aian = AIANData(
                population=AIANPopulation(
                    total=pop.get("total", 0),
                    aian_alone=pop.get("aian_alone", 0),
                    aian_alone_pct=pop.get("aian_alone_pct", 0.0),
                ),
                insurance=AIANInsurance(
                    universe=ins.get("universe", 0),
                    uninsured_total=ins.get("uninsured_total", 0),
                    uninsured_rate=ins.get("uninsured_rate", 0.0),
                    insured_rate=ins.get("insured_rate", 0.0),
                ),
                poverty=AIANPoverty(
                    universe=pov.get("universe", 0),
                    below_poverty=pov.get("below_poverty", 0),
                    poverty_rate=pov.get("poverty_rate", 0.0),
                ),
                education=AIANEducation(
                    universe=edu.get("universe", 0),
                    bachelors_plus=edu.get("bachelors_plus", 0),
                    bachelors_plus_rate=edu.get("bachelors_plus_rate", 0.0),
                ),
            )
    except Exception as e:
        log.warning("AIAN fetch failed: %s", e)

    return DemographicsResponse(
        fips=fips,
        state_fips=state_fips,
        county_fips=county_fips,
        population=pop_data,
        aian=aian,
        sources=[
            DataSource(name="U.S. Census Bureau ACS 5-Year Estimates", url="https://data.census.gov"),
        ],
    )


def _safe_int(row, col, default=0):
    try:
        val = row.get(col, default)
        if val is None:
            return default
        return int(val)
    except (ValueError, TypeError):
        return default


def _safe_float(row, col, default=None):
    try:
        val = row.get(col, default)
        if val is None:
            return default
        return round(float(val), 2)
    except (ValueError, TypeError):
        return default
