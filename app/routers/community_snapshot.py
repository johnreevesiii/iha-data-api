"""Community Snapshot — composite endpoint bundling all free-tier data."""

import logging
from fastapi import APIRouter, Depends

from app.auth.dependencies import get_current_user, enforce_fips_access
from app.auth.jwt_validator import TokenClaims
from app.schemas.responses import (
    CommunitySnapshot, PopulationData, AIANData,
    AIANPopulation, AIANInsurance, AIANPoverty, AIANEducation,
    HPSAResponse, HPSASummary, MUASummary, RUCASummary,
    HospitalResponse, Hospital, IHSResponse, IHSFacility,
    InsuranceData, DataSource,
)

router = APIRouter(prefix="/v1/community", tags=["community"])
log = logging.getLogger("iha.api.community")


# State FIPS → abbreviation lookup
_STATE_FIPS_TO_ABBR = {
    "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA",
    "08": "CO", "09": "CT", "10": "DE", "11": "DC", "12": "FL",
    "13": "GA", "15": "HI", "16": "ID", "17": "IL", "18": "IN",
    "19": "IA", "20": "KS", "21": "KY", "22": "LA", "23": "ME",
    "24": "MD", "25": "MA", "26": "MI", "27": "MN", "28": "MS",
    "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH",
    "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND",
    "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI",
    "45": "SC", "46": "SD", "47": "TN", "48": "TX", "49": "UT",
    "50": "VT", "51": "VA", "53": "WA", "54": "WV", "55": "WI",
    "56": "WY", "60": "AS", "66": "GU", "69": "MP", "72": "PR",
    "78": "VI",
}


@router.get("/{fips}", response_model=CommunitySnapshot)
async def get_community_snapshot(
    fips: str,
    user: TokenClaims = Depends(get_current_user),
):
    """Bundled community health snapshot: population, HPSA, hospitals,
    insurance, IHS, and AI/AN demographics.

    FIPS should be a 5-digit county FIPS code (e.g., 40109 for Oklahoma County, OK).
    """
    enforce_fips_access(user, fips)

    state_fips = fips[:2]
    county_fips = fips[2:]
    state_abbr = _STATE_FIPS_TO_ABBR.get(state_fips, "")

    # ── 1. Population ──────────────────────────────────
    pop_data = PopulationData()
    county_name = ""
    county_lat, county_lon = None, None
    try:
        from fetchers.fetchers import load_population
        df = load_population(state_fips=state_fips, county_fips=county_fips)
        if df is not None and not df.empty:
            row = df.iloc[0]
            pop_data = PopulationData(
                total=_int(row, "population_total"),
                median_age=_flt(row, "median_age"),
                median_household_income=_flt(row, "median_household_income"),
                poverty_rate=_flt(row, "poverty_rate"),
                age_under_18=_int(row, "age_under_18"),
                age_under_18_pct=_flt(row, "pct_under_18"),
                age_18_64=_int(row, "age_18_64"),
                age_18_64_pct=_flt(row, "pct_18_64"),
                age_65_plus=_int(row, "age_65_plus"),
                age_65_plus_pct=_flt(row, "pct_65_plus"),
            )
            name_field = row.get("NAME", "")
            if name_field:
                county_name = str(name_field).split(",")[0].strip()
    except Exception as e:
        log.warning("Population fetch failed for %s: %s", fips, e)

    # ── 2. AI/AN Demographics ──────────────────────────
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
            if result.get("name"):
                county_name = county_name or str(result["name"]).split(",")[0].strip()
    except Exception as e:
        log.warning("AIAN fetch failed for %s: %s", fips, e)

    # ── 3. HPSA / Shortage Areas ───────────────────────
    hpsa_resp = HPSAResponse(state=state_abbr, county=county_name, county_fips=fips)
    try:
        from fetchers.hpsa_data import get_shortage_area_summary
        hpsa_raw = get_shortage_area_summary(state_abbr, county_name, fips)
        hpsa_resp = HPSAResponse(
            state=state_abbr,
            county=county_name,
            county_fips=fips,
            underserved_score=hpsa_raw.get("underserved_score", 0),
            underserved_factors=hpsa_raw.get("underserved_factors", []),
            is_underserved=hpsa_raw.get("is_underserved", False),
        )
    except Exception as e:
        log.warning("HPSA fetch failed for %s: %s", fips, e)

    # ── 4. Hospitals ───────────────────────────────────
    hosp_resp = HospitalResponse()
    try:
        from fetchers.fetchers import load_hospitals
        # Use state filter since we may not have lat/lon
        df = load_hospitals(state=state_abbr)
        if df is not None and not df.empty:
            # Filter to county if possible
            if "county_name" in df.columns and county_name:
                county_df = df[df["county_name"].str.contains(county_name, case=False, na=False)]
                if not county_df.empty:
                    df = county_df

            hospitals = []
            for _, row in df.head(50).iterrows():
                hospitals.append(Hospital(
                    facility_id=str(row.get("facility_id", "")),
                    facility_name=str(row.get("facility_name", "")),
                    city=str(row.get("city", "")),
                    state=str(row.get("state", "")),
                    zip_code=str(row.get("zip_code", "")),
                    county_name=str(row.get("county_name", "")),
                    hospital_type=str(row.get("hospital_type", "")),
                    hospital_ownership=str(row.get("hospital_ownership", "")),
                    overall_rating=_str_safe(row.get("overall_rating")),
                    emergency_services=_str_safe(row.get("emergency_services")),
                    latitude=_flt_safe(row.get("latitude")),
                    longitude=_flt_safe(row.get("longitude")),
                ))
            hosp_resp = HospitalResponse(count=len(hospitals), hospitals=hospitals)
    except Exception as e:
        log.warning("Hospital fetch failed for %s: %s", fips, e)

    # ── 5. IHS Facilities ──────────────────────────────
    ihs_resp = IHSResponse()
    try:
        from fetchers.ihs_data import get_ihs_facilities_by_state
        df = get_ihs_facilities_by_state(state_abbr)
        if df is not None and not df.empty:
            facilities = []
            for _, row in df.head(50).iterrows():
                facilities.append(IHSFacility(
                    name=str(row.get("name", "")),
                    type=str(row.get("type", "")),
                    state=str(row.get("state", "")),
                    latitude=_flt_safe(row.get("lat")),
                    longitude=_flt_safe(row.get("lon")),
                ))
            ihs_resp = IHSResponse(count=len(facilities), facilities=facilities)
    except Exception as e:
        log.warning("IHS fetch failed for %s: %s", fips, e)

    # ── 6. Insurance (SAHIE) ───────────────────────────
    insurance = InsuranceData()
    try:
        from fetchers.fetchers import load_sahie
        df = load_sahie(state_fips=state_fips, county_fips=county_fips)
        if df is not None and not df.empty:
            row = df.iloc[0]
            insurance = InsuranceData(
                uninsured_rate=_flt(row, "PCTUI_PT"),
                insured_rate=_flt(row, "PCTIC_PT"),
                uninsured_count=_int(row, "NUI_PT"),
                insured_count=_int(row, "NIC_PT"),
            )
    except Exception as e:
        log.warning("Insurance fetch failed for %s: %s", fips, e)

    return CommunitySnapshot(
        fips=fips,
        state=state_abbr,
        county=county_name,
        population=pop_data,
        aian=aian,
        hpsa=hpsa_resp,
        hospitals=hosp_resp,
        ihs_facilities=ihs_resp,
        insurance=insurance,
        sources=[
            DataSource(name="U.S. Census Bureau ACS 5-Year", url="https://data.census.gov"),
            DataSource(name="HRSA HPSA/MUA", url="https://data.hrsa.gov"),
            DataSource(name="CMS Hospital Data", url="https://data.cms.gov"),
            DataSource(name="Indian Health Service", url="https://www.ihs.gov"),
            DataSource(name="Census SAHIE", url="https://www.census.gov/programs-surveys/sahie.html"),
        ],
    )


# ── Helpers ─────────────────────────────────────────────

def _int(row, col, default=0):
    try:
        v = row.get(col, default)
        return int(v) if v is not None else default
    except (ValueError, TypeError):
        return default


def _flt(row, col, default=None):
    try:
        v = row.get(col, default)
        return round(float(v), 2) if v is not None else default
    except (ValueError, TypeError):
        return default


def _str_safe(val):
    if val is None or str(val).lower() in ("nan", "none", ""):
        return None
    return str(val)


def _flt_safe(val):
    try:
        if val is None:
            return None
        f = float(val)
        if f != f:
            return None
        return round(f, 4)
    except (ValueError, TypeError):
        return None
