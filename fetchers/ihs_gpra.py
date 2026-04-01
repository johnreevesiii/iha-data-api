"""
IHS GPRA/GPRAMA Performance Measures — Benchmark Reference Data.

There is no public API for IHS GPRA data. This module embeds national-level
benchmark data extracted from IHS published reports (FY 2020-2024) and provides
lookup by IHS Area Office for regional comparison.

Sources:
  - https://www.ihs.gov/quality/government-performance-and-results-act-gpra/
  - IHS FY 2024 GPRA/GPRAMA National and Area Results (PDF)
  - IHS Congressional Budget Justification FY 2026
"""

from typing import Optional, Dict, Any, List

# ---------------------------------------------------------------------------
# IHS Area Office → State mapping
# ---------------------------------------------------------------------------

IHS_AREA_OFFICES = {
    "Alaska": ["AK"],
    "Albuquerque": ["NM", "CO", "TX"],
    "Bemidji": ["MN", "WI", "MI", "IN", "OH", "IA", "IL", "CT", "ME", "MA",
                "NH", "RI", "VT", "NY", "PA"],
    "Billings": ["MT", "WY"],
    "California": ["CA"],
    "Great Plains": ["ND", "SD", "NE", "IA"],
    "Nashville": ["AL", "CT", "DE", "FL", "GA", "KY", "LA", "ME", "MD", "MA",
                  "MI", "MS", "NC", "NH", "NJ", "NY", "OH", "PA", "RI", "SC",
                  "TN", "VA", "VT", "WV"],
    "Navajo": ["AZ", "NM", "UT"],
    "Oklahoma": ["OK", "KS", "TX"],
    "Phoenix": ["AZ", "NV", "UT"],
    "Portland": ["OR", "WA", "ID"],
    "Tucson": ["AZ"],
}


def get_ihs_area(state_abbr: str) -> Optional[str]:
    """Return the primary IHS Area Office for a state abbreviation."""
    state_upper = state_abbr.upper()
    for area, states in IHS_AREA_OFFICES.items():
        if state_upper in states:
            return area
    return None


# ---------------------------------------------------------------------------
# GPRA Measures — National Benchmarks FY 2020-2024
# ---------------------------------------------------------------------------

GPRA_MEASURES: List[Dict[str, Any]] = [
    # ---- DENTAL (3 measures) ----
    {
        "id": "dental_access",
        "category": "Dental",
        "name": "Access to Dental Services",
        "description": "Percent of active patients with a dental visit in the past year",
        "direction": "higher_better",
        "results": {
            "FY2024": {"national": 27.8, "target": 24.4},
            "FY2023": {"national": 25.3, "target": 24.4},
            "FY2022": {"national": 22.3, "target": 28.8},
            "FY2021": {"national": 19.5, "target": 26.6},
            "FY2020": {"national": 17.1, "target": 26.6},
        },
    },
    {
        "id": "dental_sealants",
        "category": "Dental",
        "name": "Dental Sealants",
        "description": "Percent of eligible children (6-9 years) with dental sealants",
        "direction": "higher_better",
        "results": {
            "FY2024": {"national": 11.8, "target": 9.9},
            "FY2023": {"national": 11.0, "target": 9.9},
            "FY2022": {"national": 9.0, "target": 10.9},
            "FY2021": {"national": 6.7, "target": 10.9},
            "FY2020": {"national": 5.7, "target": 10.9},
        },
    },
    {
        "id": "topical_fluoride",
        "category": "Dental",
        "name": "Topical Fluoride",
        "description": "Percent of patients 1-5 years receiving topical fluoride",
        "direction": "higher_better",
        "results": {
            "FY2024": {"national": 28.4, "target": 21.1},
            "FY2023": {"national": 25.6, "target": 21.1},
            "FY2022": {"national": 20.9, "target": 23.2},
            "FY2021": {"national": 16.3, "target": 23.2},
            "FY2020": {"national": 13.7, "target": 23.2},
        },
    },
    # ---- DIABETES (5 measures) ----
    {
        "id": "bp_control",
        "category": "Diabetes",
        "name": "Controlled Blood Pressure <140/90",
        "description": "Percent of diabetic patients with blood pressure under control",
        "direction": "higher_better",
        "results": {
            "FY2024": {"national": 55.8, "target": 52.4},
            "FY2023": {"national": 54.6, "target": 52.4},
            "FY2022": {"national": 53.0, "target": 52.4},
            "FY2021": {"national": 52.6, "target": 50.9},
            "FY2020": {"national": 55.1, "target": 50.9},
        },
    },
    {
        "id": "poor_glycemic",
        "category": "Diabetes",
        "name": "Poor Glycemic Control (A1c >9%)",
        "description": "Percent of diabetic patients with A1c >9% (lower is better)",
        "direction": "lower_better",
        "results": {
            "FY2024": {"national": 12.1, "target": 14.4},
            "FY2023": {"national": 13.2, "target": 14.4},
            "FY2022": {"national": 14.6, "target": 14.4},
            "FY2021": {"national": 14.3, "target": 16.8},
            "FY2020": {"national": 12.9, "target": 16.8},
        },
    },
    {
        "id": "nephropathy",
        "category": "Diabetes",
        "name": "Nephropathy Assessment",
        "description": "Percent of diabetic patients with nephropathy screening/assessment",
        "direction": "higher_better",
        "results": {
            "FY2024": {"national": 41.8, "target": 45.1},
            "FY2023": {"national": 42.5, "target": 45.1},
            "FY2022": {"national": 41.0, "target": 45.1},
            "FY2021": {"national": 38.2, "target": 41.1},
            "FY2020": {"national": 41.5, "target": 41.1},
        },
    },
    {
        "id": "retinopathy",
        "category": "Diabetes",
        "name": "Retinopathy Exam",
        "description": "Percent of diabetic patients with retinal eye exam",
        "direction": "higher_better",
        "results": {
            "FY2024": {"national": 45.4, "target": 44.7},
            "FY2023": {"national": 45.2, "target": 44.7},
            "FY2022": {"national": 41.8, "target": 44.7},
            "FY2021": {"national": 35.7, "target": 41.6},
            "FY2020": {"national": 37.4, "target": 41.6},
        },
    },
    {
        "id": "statin_diabetes",
        "category": "Diabetes",
        "name": "Statin Therapy (Diabetes)",
        "description": "Percent of diabetic patients on statin therapy",
        "direction": "higher_better",
        "results": {
            "FY2024": {"national": 49.5, "target": 54.5},
            "FY2023": {"national": 49.9, "target": 54.5},
            "FY2022": {"national": 58.5, "target": 54.5},
            "FY2021": {"national": 57.6, "target": 56.5},
            "FY2020": {"national": 58.1, "target": 56.5},
        },
    },
    # ---- IMMUNIZATIONS (4 measures) ----
    {
        "id": "childhood_imm",
        "category": "Immunizations",
        "name": "Childhood Immunizations",
        "description": "Percent of children aged 19-35 months fully immunized (4:3:1:3:3:1:4)",
        "direction": "higher_better",
        "results": {
            "FY2024": {"national": 36.1, "target": 40.9},
            "FY2023": {"national": 35.9, "target": 40.9},
            "FY2022": {"national": 36.1, "target": 40.9},
            "FY2021": {"national": 38.3, "target": 38.1},
            "FY2020": {"national": 36.0, "target": 38.1},
        },
    },
    {
        "id": "flu_child",
        "category": "Immunizations",
        "name": "Influenza Vaccination (6mo-17yr)",
        "description": "Percent of patients 6 months-17 years with influenza vaccination",
        "direction": "higher_better",
        "results": {
            "FY2024": {"national": 15.7, "target": 19.8},
            "FY2023": {"national": 17.4, "target": 19.8},
            "FY2022": {"national": 18.5, "target": 19.8},
            "FY2021": {"national": 18.1, "target": 22.5},
            "FY2020": {"national": 20.6, "target": 22.5},
        },
    },
    {
        "id": "flu_adult",
        "category": "Immunizations",
        "name": "Influenza Vaccination (18+)",
        "description": "Percent of adults 18+ with influenza vaccination",
        "direction": "higher_better",
        "results": {
            "FY2024": {"national": 18.4, "target": 19.7},
            "FY2023": {"national": 19.9, "target": 19.7},
            "FY2022": {"national": 20.0, "target": 19.7},
            "FY2021": {"national": 19.2, "target": 22.3},
            "FY2020": {"national": 20.1, "target": 22.3},
        },
    },
    {
        "id": "adult_imm",
        "category": "Immunizations",
        "name": "Adult Composite Immunization",
        "description": "Composite adult immunization score (Td, pneumococcal, zoster)",
        "direction": "higher_better",
        "results": {
            "FY2024": {"national": 38.6, "target": 37.0},
            "FY2023": {"national": 37.0, "target": 37.0},
            "FY2022": {"national": 36.1, "target": 37.0},
            "FY2021": {"national": 35.9, "target": 35.5},
            "FY2020": {"national": 33.1, "target": 35.5},
        },
    },
    # ---- PREVENTION (9 measures) ----
    {
        "id": "cervical_screen",
        "category": "Prevention",
        "name": "Cervical Cancer Screening",
        "description": "Percent of women 21-64 with cervical cancer screening",
        "direction": "higher_better",
        "results": {
            "FY2024": {"national": 34.4, "target": 33.2},
            "FY2023": {"national": 33.8, "target": 33.2},
            "FY2022": {"national": 33.2, "target": 33.2},
            "FY2021": {"national": 29.5, "target": 33.0},
            "FY2020": {"national": 28.3, "target": 33.0},
        },
    },
    {
        "id": "colorectal_screen",
        "category": "Prevention",
        "name": "Colorectal Cancer Screening",
        "description": "Percent of adults 45-75 with colorectal cancer screening",
        "direction": "higher_better",
        "results": {
            "FY2024": {"national": 23.6, "target": 23.7},
            "FY2023": {"national": 23.3, "target": 23.7},
            "FY2022": {"national": 23.7, "target": 23.7},
            "FY2021": {"national": 21.9, "target": 24.5},
            "FY2020": {"national": 22.3, "target": 24.5},
        },
    },
    {
        "id": "mammography",
        "category": "Prevention",
        "name": "Mammography Screening",
        "description": "Percent of women 50-74 with mammography screening",
        "direction": "higher_better",
        "results": {
            "FY2024": {"national": 38.9, "target": 28.7},
            "FY2023": {"national": 38.4, "target": 28.7},
            "FY2022": {"national": 29.2, "target": 28.7},
            "FY2021": {"national": 26.3, "target": 28.3},
            "FY2020": {"national": 23.4, "target": 28.3},
        },
    },
    {
        "id": "tobacco_cessation",
        "category": "Prevention",
        "name": "Tobacco Cessation",
        "description": "Percent of tobacco users with cessation counseling or medication",
        "direction": "higher_better",
        "results": {
            "FY2024": {"national": 26.2, "target": 24.4},
            "FY2023": {"national": 26.1, "target": 24.4},
            "FY2022": {"national": 25.0, "target": 24.4},
            "FY2021": {"national": 24.1, "target": 24.3},
            "FY2020": {"national": 23.1, "target": 24.3},
        },
    },
    {
        "id": "hiv_screening",
        "category": "Prevention",
        "name": "HIV Screening",
        "description": "Percent of patients 15-65 ever screened for HIV",
        "direction": "higher_better",
        "results": {
            "FY2024": {"national": 43.4, "target": 38.9},
            "FY2023": {"national": 40.3, "target": 38.9},
            "FY2022": {"national": 38.0, "target": 38.9},
            "FY2021": {"national": 35.2, "target": 36.3},
            "FY2020": {"national": 33.0, "target": 36.3},
        },
    },
    {
        "id": "cvd_statin",
        "category": "Prevention",
        "name": "CVD Statin Therapy",
        "description": "Percent of high-risk CVD patients on statin therapy",
        "direction": "higher_better",
        "results": {
            "FY2024": {"national": 35.4, "target": 37.8},
            "FY2023": {"national": 35.0, "target": 37.8},
            "FY2022": {"national": 42.7, "target": 37.8},
            "FY2021": {"national": 42.0, "target": 41.5},
            "FY2020": {"national": 42.2, "target": 41.5},
        },
    },
    {
        "id": "bp_control_mh",
        "category": "Prevention",
        "name": "Controlling High Blood Pressure",
        "description": "Percent of hypertensive patients with blood pressure controlled (Million Hearts)",
        "direction": "higher_better",
        "results": {
            "FY2024": {"national": 46.9, "target": 45.8},
            "FY2023": {"national": 45.7, "target": 45.8},
            "FY2022": {"national": 45.5, "target": 45.8},
            "FY2021": {"national": 45.3, "target": 45.0},
            "FY2020": {"national": 47.9, "target": 45.0},
        },
    },
    {
        "id": "childhood_weight",
        "category": "Prevention",
        "name": "Childhood Weight Assessment",
        "description": "Percent of children 2-17 with BMI assessment and counseling",
        "direction": "higher_better",
        "results": {
            "FY2024": {"national": 21.9, "target": 23.0},
            "FY2023": {"national": 22.0, "target": 23.0},
            "FY2022": {"national": 23.0, "target": 23.0},
            "FY2021": {"national": 20.6, "target": 22.1},
            "FY2020": {"national": 19.7, "target": 22.1},
        },
    },
    {
        "id": "breastfeeding",
        "category": "Prevention",
        "name": "Breastfeeding Rates",
        "description": "Percent of infants exclusively breastfed at hospital discharge",
        "direction": "higher_better",
        "results": {
            "FY2024": {"national": 43.0, "target": 42.6},
            "FY2023": {"national": 41.8, "target": 42.6},
            "FY2022": {"national": 39.3, "target": 42.6},
            "FY2021": {"national": 39.3, "target": 41.2},
            "FY2020": {"national": 37.4, "target": 41.2},
        },
    },
    # ---- BEHAVIORAL HEALTH (5 measures) ----
    {
        "id": "alcohol_screen",
        "category": "Behavioral Health",
        "name": "Universal Alcohol Screening",
        "description": "Percent of patients 12+ screened for unhealthy alcohol use",
        "direction": "higher_better",
        "results": {
            "FY2024": {"national": 34.4, "target": 32.2},
            "FY2023": {"national": 34.2, "target": 32.2},
            "FY2022": {"national": 33.2, "target": 32.2},
            "FY2021": {"national": 30.3, "target": 32.0},
            "FY2020": {"national": 29.5, "target": 32.0},
        },
    },
    {
        "id": "sbirt",
        "category": "Behavioral Health",
        "name": "SBIRT",
        "description": "Screening, Brief Intervention, and Referral to Treatment",
        "direction": "higher_better",
        "results": {
            "FY2024": {"national": 17.9, "target": 15.0},
            "FY2023": {"national": 15.0, "target": 15.0},
            "FY2022": {"national": 14.3, "target": 15.0},
            "FY2021": {"national": 12.9, "target": 14.3},
            "FY2020": {"national": 12.1, "target": 14.3},
        },
    },
    {
        "id": "depression_screen_youth",
        "category": "Behavioral Health",
        "name": "Depression Screening (12-17yr)",
        "description": "Percent of adolescents 12-17 screened for depression",
        "direction": "higher_better",
        "results": {
            "FY2024": {"national": 32.7, "target": 29.5},
            "FY2023": {"national": 34.1, "target": 29.5},
            "FY2022": {"national": 32.1, "target": 29.5},
            "FY2021": {"national": 27.6, "target": 32.2},
            "FY2020": {"national": 26.6, "target": 32.2},
        },
    },
    {
        "id": "depression_screen_adult",
        "category": "Behavioral Health",
        "name": "Depression Screening (18+)",
        "description": "Percent of adults 18+ screened for depression",
        "direction": "higher_better",
        "results": {
            "FY2024": {"national": 37.1, "target": 36.4},
            "FY2023": {"national": 37.4, "target": 36.4},
            "FY2022": {"national": 37.0, "target": 36.4},
            "FY2021": {"national": 34.2, "target": 37.5},
            "FY2020": {"national": 33.2, "target": 37.5},
        },
    },
    {
        "id": "ipv_screening",
        "category": "Behavioral Health",
        "name": "IPV/Domestic Violence Screening",
        "description": "Percent of women 14-46 screened for intimate partner violence",
        "direction": "higher_better",
        "results": {
            "FY2024": {"national": 31.9, "target": 29.6},
            "FY2023": {"national": 28.9, "target": 29.6},
            "FY2022": {"national": 28.3, "target": 29.6},
            "FY2021": {"national": 27.7, "target": 28.1},
            "FY2020": {"national": 24.8, "target": 28.1},
        },
    },
]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_gpra_benchmarks(
    category: Optional[str] = None,
    year: str = "FY2024",
) -> List[Dict[str, Any]]:
    """
    Return national GPRA benchmark data, optionally filtered by category.

    Args:
        category: One of Dental, Diabetes, Immunizations, Prevention,
                  Behavioral Health. None returns all.
        year: Fiscal year key (default FY2024).

    Returns:
        List of measure dicts with ``national``, ``target``, and ``met_target``.
    """
    results = []
    for measure in GPRA_MEASURES:
        if category and measure["category"] != category:
            continue

        yr_data = measure["results"].get(year, {})
        national = yr_data.get("national")
        target = yr_data.get("target")

        if measure["direction"] == "lower_better":
            met = national <= target if national is not None and target is not None else None
        else:
            met = national >= target if national is not None and target is not None else None

        results.append({
            "id": measure["id"],
            "category": measure["category"],
            "name": measure["name"],
            "description": measure["description"],
            "national": national,
            "target": target,
            "met_target": met,
            "direction": measure["direction"],
        })

    return results


def get_gpra_trends(measure_id: str) -> Dict[str, Any]:
    """
    Return multi-year trend data for a single GPRA measure.

    Args:
        measure_id: e.g. ``dental_access``, ``poor_glycemic``.

    Returns:
        Dict with ``name``, ``category``, ``years`` (list of year/value dicts).
    """
    for measure in GPRA_MEASURES:
        if measure["id"] == measure_id:
            years = []
            for yr_key in sorted(measure["results"].keys()):
                yr_data = measure["results"][yr_key]
                years.append({
                    "year": yr_key,
                    "national": yr_data.get("national"),
                    "target": yr_data.get("target"),
                })
            return {
                "id": measure["id"],
                "name": measure["name"],
                "category": measure["category"],
                "direction": measure["direction"],
                "years": years,
            }
    return {}


def get_gpra_summary(state_abbr: Optional[str] = None) -> Dict[str, Any]:
    """
    Build a structured GPRA summary for display, optionally noting the IHS Area.

    Returns:
        Dict with ``ihs_area``, ``categories`` (grouped measures), and
        ``targets_met`` / ``targets_missed`` counts.
    """
    area = get_ihs_area(state_abbr) if state_abbr else None
    benchmarks = get_gpra_benchmarks(year="FY2024")

    categories: Dict[str, list] = {}
    met = 0
    missed = 0
    for b in benchmarks:
        categories.setdefault(b["category"], []).append(b)
        if b["met_target"] is True:
            met += 1
        elif b["met_target"] is False:
            missed += 1

    return {
        "ihs_area": area,
        "categories": categories,
        "targets_met": met,
        "targets_missed": missed,
        "total_measures": len(benchmarks),
        "year": "FY2024",
    }
