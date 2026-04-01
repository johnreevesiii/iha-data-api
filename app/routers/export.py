"""Export endpoint — CSV download of community snapshot data (Premium)."""

import io
import csv
import logging
from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse

from app.auth.dependencies import require_tier, enforce_fips_access
from app.auth.jwt_validator import TokenClaims

router = APIRouter(prefix="/v1/export", tags=["export"])
log = logging.getLogger("iha.api.export")

# State FIPS → abbreviation
_STATE_FIPS = {
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
    "56": "WY",
}


@router.get("/csv/{fips}")
async def export_csv(
    fips: str,
    user: TokenClaims = Depends(require_tier("premium", "internal")),
):
    """Export community snapshot as CSV download."""
    state_fips = fips[:2]
    county_fips = fips[2:]
    state_abbr = _STATE_FIPS.get(state_fips, "")

    rows = []

    # Population
    try:
        from fetchers.fetchers import load_population
        df = load_population(state_fips=state_fips, county_fips=county_fips)
        if df is not None and not df.empty:
            r = df.iloc[0]
            rows.append(("Demographics", "Total Population", _v(r, "population_total"), "Census ACS5"))
            rows.append(("Demographics", "Median Age", _v(r, "median_age"), "Census ACS5"))
            rows.append(("Demographics", "Median Household Income", _v(r, "median_household_income"), "Census ACS5"))
            rows.append(("Demographics", "Poverty Rate %", _v(r, "poverty_rate"), "Census ACS5"))
    except Exception as e:
        log.warning("Export pop failed: %s", e)

    # AI/AN
    try:
        from fetchers.census_aian import get_aian_comprehensive
        result = get_aian_comprehensive(state_fips, county_fips)
        if result:
            pop = result.get("population", {})
            ins = result.get("insurance", {})
            pov = result.get("poverty", {})
            rows.append(("AI/AN", "AI/AN Population", pop.get("aian_alone", ""), "Census ACS5"))
            rows.append(("AI/AN", "AI/AN % of Total", pop.get("aian_alone_pct", ""), "Census ACS5"))
            rows.append(("AI/AN", "AI/AN Uninsured Rate %", ins.get("uninsured_rate", ""), "Census ACS5"))
            rows.append(("AI/AN", "AI/AN Poverty Rate %", pov.get("poverty_rate", ""), "Census ACS5"))
    except Exception as e:
        log.warning("Export aian failed: %s", e)

    # HPSA
    try:
        from fetchers.hpsa_data import get_shortage_area_summary
        hpsa = get_shortage_area_summary(state_abbr, "", fips)
        rows.append(("Designations", "Underserved Score", hpsa.get("underserved_score", ""), "HRSA"))
        rows.append(("Designations", "Is Underserved", hpsa.get("is_underserved", ""), "HRSA"))
        for f in hpsa.get("underserved_factors", []):
            rows.append(("Designations", "Designation", f, "HRSA"))
    except Exception as e:
        log.warning("Export hpsa failed: %s", e)

    # Insurance
    try:
        from fetchers.fetchers import load_sahie
        df = load_sahie(state_fips=state_fips, county_fips=county_fips)
        if df is not None and not df.empty:
            r = df.iloc[0]
            rows.append(("Insurance", "Uninsured Rate %", _v(r, "PCTUI_PT"), "Census SAHIE"))
            rows.append(("Insurance", "Uninsured Count", _v(r, "NUI_PT"), "Census SAHIE"))
    except Exception as e:
        log.warning("Export insurance failed: %s", e)

    # Hospitals
    try:
        from fetchers.fetchers import load_hospitals
        df = load_hospitals(state=state_abbr)
        if df is not None and not df.empty:
            for _, row in df.head(50).iterrows():
                rows.append(("Hospitals", str(row.get("facility_name", "")),
                            f"{row.get('city','')}, {row.get('state','')}", "CMS"))
    except Exception as e:
        log.warning("Export hospitals failed: %s", e)

    # Build CSV
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Category", "Metric", "Value", "Source"])
    writer.writerows(rows)

    content = output.getvalue()
    return StreamingResponse(
        io.BytesIO(content.encode("utf-8")),
        media_type="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename=community_snapshot_{fips}.csv",
        },
    )


def _v(row, col):
    try:
        val = row.get(col)
        if val is None or str(val).lower() in ("nan", "none"):
            return ""
        return val
    except Exception:
        return ""
