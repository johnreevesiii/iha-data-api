"""
HPSA (Health Professional Shortage Areas) and MUA/MUP Data Module

Data Sources:
- HRSA Data Warehouse CSV downloads for HPSA designations (PC, MH, DH)
- HRSA Data Warehouse CSV download for MUA/MUP designations
- USDA ERS for RUCA (Rural-Urban Commuting Area) codes

CSV endpoints confirmed working 2026-02-14:
  PC: https://data.hrsa.gov/DataDownload/DD_Files/BCD_HPSA_FCT_DET_PC.csv
  MH: https://data.hrsa.gov/DataDownload/DD_Files/BCD_HPSA_FCT_DET_MH.csv
  DH: https://data.hrsa.gov/DataDownload/DD_Files/BCD_HPSA_FCT_DET_DH.csv
  MUA: https://data.hrsa.gov/DataDownload/DD_Files/MUA_DET.csv
"""
from __future__ import annotations
import pandas as pd
import requests
import io
from pathlib import Path
from typing import Optional, Dict, List, Any

# ---------------------------------------------------------------------------
# HRSA CSV endpoints — one file per HPSA discipline
# ---------------------------------------------------------------------------
_HRSA_HPSA_URLS = {
    "Primary Care": "https://data.hrsa.gov/DataDownload/DD_Files/BCD_HPSA_FCT_DET_PC.csv",
    "Mental Health": "https://data.hrsa.gov/DataDownload/DD_Files/BCD_HPSA_FCT_DET_MH.csv",
    "Dental Health": "https://data.hrsa.gov/DataDownload/DD_Files/BCD_HPSA_FCT_DET_DH.csv",
}
_HRSA_MUA_URL = "https://data.hrsa.gov/DataDownload/DD_Files/MUA_DET.csv"

# Local data paths (fallback)
LOCAL_PATHS = [
    Path("data/hrsa/hpsa.csv"),
    Path("data/community/hpsa.csv"),
    Path("HCA_data/hrsa/hpsa.csv"),
]

MUA_LOCAL_PATHS = [
    Path("data/hrsa/mua.csv"),
    Path("data/community/mua.csv"),
    Path("HCA_data/hrsa/mua.csv"),
]

RUCA_LOCAL_PATHS = [
    Path("data/usda/ruca2010revised.csv"),
    Path("data/community/ruca.csv"),
    Path("HCA_data/usda/ruca.csv"),
]

# RUCA code definitions (2010 revision, 1-10 primary codes)
RUCA_CODES = {
    1: ("Metropolitan area core: primary flow within an urbanized area (UA)", "urban"),
    2: ("Metropolitan area high commuting: 30%+ flow to a UA", "urban"),
    3: ("Metropolitan area low commuting: 10-30% flow to a UA", "urban"),
    4: ("Micropolitan area core: primary flow within an urban cluster (UC) 10k-49,999", "suburban"),
    5: ("Micropolitan high commuting: 30%+ flow to a large UC", "suburban"),
    6: ("Micropolitan low commuting: 10-30% flow to a large UC", "suburban"),
    7: ("Small town core: primary flow within a UC 2,500-9,999", "rural"),
    8: ("Small town high commuting: 30%+ flow to a small UC", "rural"),
    9: ("Small town low commuting: 10-30% flow to a small UC", "rural"),
    10: ("Rural areas: primary flow to a tract outside a UA or UC", "rural"),
    99: ("Not coded: Census tract has zero population", "unknown"),
}

# HPSA discipline types
HPSA_DISCIPLINES = {
    "PC": "Primary Care",
    "MH": "Mental Health",
    "DH": "Dental Health",
}

# HPSA component types
HPSA_TYPES = {
    "Geographic": "Geographic HPSA - entire area is underserved",
    "Population": "Population HPSA - specific population group is underserved",
    "Facility": "Facility HPSA - specific facility serves underserved population",
    "Auto": "Auto-designated facility (FQHCs, IHS, etc.)",
    "Indian Health Service, Tribal Health, and Urban Indian Health Organizations":
        "IHS/Tribal/Urban Indian Health - auto-designated facility",
    "High Needs Geographic HPSA": "Geographic HPSA with high-needs designation",
    "HPSA Population": "Population-based HPSA designation",
}

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _try_local(paths: List[Path]) -> Optional[pd.DataFrame]:
    """Try to load data from local paths."""
    for p in paths:
        if p.exists():
            try:
                return pd.read_csv(p, low_memory=False)
            except Exception:
                continue
    return None


def _fetch_hrsa_csv(url: str, timeout: int = 120) -> Optional[pd.DataFrame]:
    """Fetch CSV data from HRSA endpoint."""
    try:
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        return pd.read_csv(io.StringIO(resp.text), low_memory=False)
    except Exception as e:
        print(f"[HPSA] HRSA CSV fetch error ({url}): {e}")
        return None


def _normalize_county(name: str) -> str:
    """Normalize county name for matching: lowercase, strip 'county' suffix."""
    return name.lower().replace(" county", "").strip()


def _filter_county_rows(df: pd.DataFrame, county_name: str,
                        state_abbr: str) -> pd.DataFrame:
    """Filter a HRSA dataframe to rows matching a county + state.

    Matches against 'Common County Name' (format: "Oglala Lakota County, SD")
    and falls back to 'County Equivalent Name' or any column with 'county' in
    the header.  Only keeps rows with HPSA Status == 'Designated'.
    """
    if df.empty:
        return df

    county_norm = _normalize_county(county_name)

    # Filter to designated (active) records only
    status_col = next((c for c in df.columns if c == "HPSA Status"), None)
    if status_col:
        df = df[df[status_col].astype(str).str.strip() == "Designated"]

    # Primary match: 'Common County Name' which has format "Oglala Lakota County, SD"
    if "Common County Name" in df.columns:
        mask = df["Common County Name"].astype(str).str.lower().apply(
            lambda v: county_norm in _normalize_county(v)
        )
        # Also verify state
        if "Common State Abbreviation" in df.columns:
            mask = mask & (df["Common State Abbreviation"].astype(str).str.upper() == state_abbr.upper())
        if mask.any():
            return df[mask]

    # Fallback: try other county columns
    for col in df.columns:
        if "county" in col.lower() or col == "HPSA Name":
            try:
                mask = df[col].astype(str).str.lower().str.contains(county_norm, na=False)
                if mask.any():
                    return df[mask]
            except Exception:
                continue

    return pd.DataFrame()


# ---------------------------------------------------------------------------
# HPSA data fetching — queries all three discipline files
# ---------------------------------------------------------------------------

def _fetch_hpsa_for_county(state_abbr: str, county_name: str,
                           discipline_label: str) -> pd.DataFrame:
    """Fetch HPSA records for a single discipline filtered to one county."""
    url = _HRSA_HPSA_URLS.get(discipline_label)
    if not url:
        return pd.DataFrame()

    df = _fetch_hrsa_csv(url)
    if df is None or df.empty:
        print(f"[HPSA] No data returned for {discipline_label}")
        return pd.DataFrame()

    return _filter_county_rows(df, county_name, state_abbr)


def _best_hpsa_record(county_df: pd.DataFrame) -> Dict[str, Any]:
    """Pick the best (highest score) designated record from a filtered set."""
    if county_df.empty:
        return {"designated": False, "score": None, "type": None, "shortage": None}

    # Parse scores to numeric for comparison
    scores = pd.to_numeric(county_df.get("HPSA Score", pd.Series(dtype=float)),
                           errors="coerce")
    best_idx = scores.idxmax() if scores.notna().any() else county_df.index[0]
    row = county_df.loc[best_idx]

    score = row.get("HPSA Score", None)
    try:
        score = int(float(score))
    except (TypeError, ValueError):
        pass

    shortage = row.get("HPSA Shortage", None)
    try:
        shortage = float(shortage)
        if pd.isna(shortage):
            shortage = None
    except (TypeError, ValueError):
        shortage = None

    return {
        "designated": True,
        "score": score,
        "type": row.get("Designation Type", row.get("HPSA Type", "Geographic")),
        "shortage": shortage,
    }


def fetch_hpsa_data(state_abbr: Optional[str] = None,
                    discipline: Optional[str] = None,
                    refresh: bool = False) -> pd.DataFrame:
    """Fetch HPSA designations — all three disciplines combined.

    Used by the statewide view. Downloads all three discipline CSVs and
    concatenates them, then filters by state.
    """
    # Try local first
    if not refresh:
        df = _try_local(LOCAL_PATHS)
        if df is not None and not df.empty:
            return _filter_hpsa(df, state_abbr, discipline)

    frames = []
    for label, url in _HRSA_HPSA_URLS.items():
        part = _fetch_hrsa_csv(url)
        if part is not None and not part.empty:
            frames.append(part)

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    return _filter_hpsa(df, state_abbr, discipline)


def _filter_hpsa(df: pd.DataFrame, state_abbr: Optional[str],
                 discipline: Optional[str]) -> pd.DataFrame:
    """Apply state/discipline filters to a combined HPSA dataframe."""
    if df.empty:
        return df

    if state_abbr:
        for col in ("Common State Abbreviation", "Primary State Abbreviation", "State Abbreviation"):
            if col in df.columns:
                df = df[df[col].astype(str).str.upper() == state_abbr.upper()]
                break

    if discipline:
        disc_upper = discipline.upper()
        if "HPSA Discipline Class" in df.columns:
            mask = df["HPSA Discipline Class"].astype(str).str.upper().str.contains(disc_upper, na=False)
            df = df[mask]

    return df


# ---------------------------------------------------------------------------
# MUA data fetching
# ---------------------------------------------------------------------------

def fetch_mua_data(state_abbr: Optional[str] = None,
                   refresh: bool = False) -> pd.DataFrame:
    """Fetch MUA/MUP designations from HRSA."""
    if not refresh:
        df = _try_local(MUA_LOCAL_PATHS)
        if df is not None and not df.empty:
            return _filter_mua(df, state_abbr)

    df = _fetch_hrsa_csv(_HRSA_MUA_URL)
    if df is None:
        return pd.DataFrame()

    return _filter_mua(df, state_abbr)


def _filter_mua(df: pd.DataFrame, state_abbr: Optional[str]) -> pd.DataFrame:
    """Filter MUA dataframe by state."""
    if df.empty or not state_abbr:
        return df

    for col in ("State Abbreviation", "Common State Abbreviation", "Primary State Abbreviation"):
        if col in df.columns:
            df = df[df[col].astype(str).str.upper() == state_abbr.upper()]
            break

    return df


# ---------------------------------------------------------------------------
# RUCA rural classification
# ---------------------------------------------------------------------------

def fetch_ruca_codes(county_fips: Optional[str] = None,
                     tract_fips: Optional[str] = None) -> pd.DataFrame:
    """Fetch RUCA codes for rural classification."""
    df = _try_local(RUCA_LOCAL_PATHS)

    if df is None:
        ruca_url = "https://www.ers.usda.gov/webdocs/DataFiles/53241/ruca2010revised.csv"
        try:
            df = pd.read_csv(ruca_url, low_memory=False)
        except Exception:
            return pd.DataFrame()

    if df is None or df.empty:
        return pd.DataFrame()

    df.columns = [c.lower().strip() for c in df.columns]

    if county_fips:
        county_fips = str(county_fips).zfill(5)
        fips_cols = [c for c in df.columns if "fips" in c or "county" in c]
        for col in fips_cols:
            try:
                mask = df[col].astype(str).str.zfill(5).str.startswith(county_fips[:2])
                mask &= df[col].astype(str).str[2:5] == county_fips[2:5]
                if mask.any():
                    df = df[mask]
                    break
            except Exception:
                continue

    if tract_fips:
        tract_cols = [c for c in df.columns if "tract" in c]
        for col in tract_cols:
            try:
                df = df[df[col].astype(str).str.zfill(11) == str(tract_fips).zfill(11)]
                break
            except Exception:
                continue

    return df


# ---------------------------------------------------------------------------
# County-level summaries (used by the HPSA/MUA page)
# ---------------------------------------------------------------------------

def get_hpsa_summary_for_county(state_abbr: str, county_name: str) -> Dict[str, Any]:
    """Get HPSA designation summary for a county across all 3 disciplines.

    Queries each discipline CSV separately and filters to the target county.
    Only considers records with HPSA Status == 'Designated'.
    For each discipline, picks the record with the highest HPSA Score.
    """
    result = {
        "primary_care": {"designated": False, "score": None, "type": None, "shortage": None},
        "mental_health": {"designated": False, "score": None, "type": None, "shortage": None},
        "dental": {"designated": False, "score": None, "type": None, "shortage": None},
        "any_hpsa": False,
        "county": county_name,
        "state": state_abbr,
    }

    discipline_map = {
        "Primary Care": "primary_care",
        "Mental Health": "mental_health",
        "Dental Health": "dental",
    }

    for disc_label, result_key in discipline_map.items():
        try:
            county_df = _fetch_hpsa_for_county(state_abbr, county_name, disc_label)
            if not county_df.empty:
                result[result_key] = _best_hpsa_record(county_df)
                print(f"[HPSA] {disc_label} for {county_name}, {state_abbr}: "
                      f"score={result[result_key]['score']}, "
                      f"type={result[result_key]['type']}")
        except Exception as e:
            print(f"[HPSA] Error fetching {disc_label} for {county_name}: {e}")

    result["any_hpsa"] = any([
        result["primary_care"]["designated"],
        result["mental_health"]["designated"],
        result["dental"]["designated"],
    ])

    return result


def get_mua_summary_for_county(state_abbr: str, county_name: str) -> Dict[str, Any]:
    """Get MUA/MUP designation summary for a county."""
    result = {
        "designated": False,
        "designation_type": None,
        "imu_score": None,
        "designation_date": None,
        "county": county_name,
        "state": state_abbr,
        "population_served": None,
    }

    df = fetch_mua_data(state_abbr)
    if df.empty:
        print(f"[MUA] No MUA data returned for state {state_abbr}")
        return result

    county_norm = _normalize_county(county_name)

    # Filter to Designated only
    status_col = next((c for c in df.columns if "status" in c.lower() and "description" in c.lower()), None)
    if status_col:
        df = df[df[status_col].astype(str).str.strip() == "Designated"]

    # Match county — MUA CSV uses 'Complete County Name' (e.g. "Oglala Lakota County")
    county_df = pd.DataFrame()
    for col in ("Complete County Name", "Common County Name", "MUA/P Service Area Name"):
        if col in df.columns:
            mask = df[col].astype(str).str.lower().apply(
                lambda v: county_norm in _normalize_county(v)
            )
            if mask.any():
                county_df = df[mask]
                break

    if county_df.empty:
        # Broader fallback
        for col in df.columns:
            if "county" in col.lower() or "name" in col.lower() or "area" in col.lower():
                try:
                    mask = df[col].astype(str).str.lower().str.contains(county_norm, na=False)
                    if mask.any():
                        county_df = df[mask]
                        break
                except Exception:
                    continue

    if county_df.empty:
        print(f"[MUA] No MUA records found for {county_name}, {state_abbr}")
        return result

    row = county_df.iloc[0]
    result["designated"] = True
    result["designation_type"] = (
        row.get("Designation Type", row.get("MUA/P Designation", "MUA"))
    )

    imu = row.get("IMU Score", None)
    try:
        imu = float(imu)
    except (TypeError, ValueError):
        imu = None
    result["imu_score"] = imu

    result["designation_date"] = row.get("Designation Date",
                                         row.get("MUA/P Designation Date String", None))

    pop = row.get("Designation Population in a Medically Underserved Area/Population (MUA/P)",
                  row.get("Designation Population", None))
    try:
        pop = int(float(pop))
    except (TypeError, ValueError):
        pop = None
    result["population_served"] = pop

    print(f"[MUA] {county_name}, {state_abbr}: designated={result['designated']}, "
          f"IMU={result['imu_score']}, type={result['designation_type']}")

    return result


def get_ruca_classification(county_fips: str) -> Dict[str, Any]:
    """Get RUCA rural classification for a county."""
    df = fetch_ruca_codes(county_fips=county_fips)

    result = {
        "county_fips": county_fips,
        "primary_ruca": None,
        "classification": None,
        "description": None,
        "tracts_count": 0,
        "tracts_rural_pct": 0.0,
    }

    if df.empty:
        return result

    ruca_cols = [c for c in df.columns if "ruca" in c.lower() and "primary" in c.lower()]
    ruca_col = ruca_cols[0] if ruca_cols else next(
        (c for c in df.columns if "ruca" in c.lower()), None
    )

    if ruca_col:
        try:
            codes = pd.to_numeric(df[ruca_col], errors="coerce").dropna()
            if not codes.empty:
                primary = int(codes.mode().iloc[0]) if not codes.mode().empty else int(codes.median())
                result["primary_ruca"] = primary
                result["description"], result["classification"] = RUCA_CODES.get(primary, ("Unknown", "unknown"))
                rural_codes = [7, 8, 9, 10]
                result["tracts_count"] = len(codes)
                result["tracts_rural_pct"] = round((codes.isin(rural_codes).sum() / len(codes)) * 100, 1)
        except Exception:
            pass

    return result


# ---------------------------------------------------------------------------
# Comprehensive summary (main entry point used by the page)
# ---------------------------------------------------------------------------

def get_shortage_area_summary(state_abbr: str, county_name: str, county_fips: str) -> Dict[str, Any]:
    """Get comprehensive shortage area summary combining HPSA, MUA, and RUCA."""
    hpsa = get_hpsa_summary_for_county(state_abbr, county_name)
    mua = get_mua_summary_for_county(state_abbr, county_name)
    ruca = get_ruca_classification(county_fips)

    score = 0
    factors = []

    if hpsa["primary_care"]["designated"]:
        score += 25
        factors.append("Primary Care HPSA")
    if hpsa["mental_health"]["designated"]:
        score += 20
        factors.append("Mental Health HPSA")
    if hpsa["dental"]["designated"]:
        score += 15
        factors.append("Dental HPSA")
    if mua["designated"]:
        score += 25
        factors.append("MUA/MUP")
    if ruca["classification"] == "rural":
        score += 15
        factors.append("Rural (RUCA)")
    elif ruca["classification"] == "suburban":
        score += 5
        factors.append("Suburban (RUCA)")

    return {
        "hpsa": hpsa,
        "mua": mua,
        "ruca": ruca,
        "underserved_score": score,
        "underserved_factors": factors,
        "is_underserved": score >= 40,
        "state": state_abbr,
        "county": county_name,
        "county_fips": county_fips,
    }


def analyze_hpsa_trends(state_abbr: str) -> pd.DataFrame:
    """Analyze HPSA designations across a state."""
    df = fetch_hpsa_data(state_abbr)

    if df.empty:
        return pd.DataFrame()

    county_col = next((c for c in df.columns if "county" in c.lower()), None)

    if not county_col:
        return df

    agg = df.groupby(county_col).agg({col: "first" for col in df.columns if col != county_col}).reset_index()

    return agg
