"""Pydantic response models for API endpoints."""

from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any


# ── Shared ──────────────────────────────────────────────

class DataSource(BaseModel):
    name: str
    url: str = ""
    updated: str = ""


class ErrorResponse(BaseModel):
    detail: str


# ── Demographics ────────────────────────────────────────

class PopulationData(BaseModel):
    total: int = 0
    median_age: Optional[float] = None
    median_household_income: Optional[float] = None
    poverty_rate: Optional[float] = None
    age_under_18: int = 0
    age_under_18_pct: Optional[float] = None
    age_18_64: int = 0
    age_18_64_pct: Optional[float] = None
    age_65_plus: int = 0
    age_65_plus_pct: Optional[float] = None


class AIANPopulation(BaseModel):
    total: int = 0
    aian_alone: int = 0
    aian_alone_pct: float = 0.0


class AIANInsurance(BaseModel):
    universe: int = 0
    uninsured_total: int = 0
    uninsured_rate: float = 0.0
    insured_rate: float = 0.0


class AIANPoverty(BaseModel):
    universe: int = 0
    below_poverty: int = 0
    poverty_rate: float = 0.0


class AIANEducation(BaseModel):
    universe: int = 0
    bachelors_plus: int = 0
    bachelors_plus_rate: float = 0.0


class AIANData(BaseModel):
    population: AIANPopulation = AIANPopulation()
    insurance: AIANInsurance = AIANInsurance()
    poverty: AIANPoverty = AIANPoverty()
    education: AIANEducation = AIANEducation()


class DemographicsResponse(BaseModel):
    fips: str
    state_fips: str
    county_fips: str
    population: PopulationData = PopulationData()
    aian: AIANData = AIANData()
    sources: List[DataSource] = []


# ── Hospitals ───────────────────────────────────────────

class Hospital(BaseModel):
    facility_id: str = ""
    facility_name: str = ""
    city: str = ""
    state: str = ""
    zip_code: str = ""
    county_name: str = ""
    hospital_type: str = ""
    hospital_ownership: str = ""
    overall_rating: Optional[str] = None
    emergency_services: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    distance_miles: Optional[float] = None


class HospitalResponse(BaseModel):
    count: int = 0
    hospitals: List[Hospital] = []
    search_lat: Optional[float] = None
    search_lon: Optional[float] = None
    radius_miles: float = 50
    sources: List[DataSource] = []


# ── HPSA / Shortage Areas ──────────────────────────────

class HPSADesignation(BaseModel):
    designation_type: str = ""
    score: Optional[int] = None
    status: str = ""
    name: str = ""


class HPSASummary(BaseModel):
    primary_care: List[HPSADesignation] = []
    mental_health: List[HPSADesignation] = []
    dental: List[HPSADesignation] = []


class MUASummary(BaseModel):
    is_mua: bool = False
    is_mup: bool = False
    designations: List[Dict[str, Any]] = []


class RUCASummary(BaseModel):
    primary_code: Optional[int] = None
    classification: str = ""
    is_rural: bool = False


class HPSAResponse(BaseModel):
    state: str
    county: str
    county_fips: str
    hpsa: HPSASummary = HPSASummary()
    mua: MUASummary = MUASummary()
    ruca: RUCASummary = RUCASummary()
    underserved_score: int = 0
    underserved_factors: List[str] = []
    is_underserved: bool = False
    sources: List[DataSource] = []


# ── IHS Facilities ──────────────────────────────────────

class IHSFacility(BaseModel):
    name: str = ""
    type: str = ""
    state: str = ""
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    distance_miles: Optional[float] = None


class IHSResponse(BaseModel):
    count: int = 0
    facilities: List[IHSFacility] = []
    sources: List[DataSource] = []


# ── Insurance ───────────────────────────────────────────

class InsuranceData(BaseModel):
    uninsured_rate: Optional[float] = None
    insured_rate: Optional[float] = None
    uninsured_count: Optional[int] = None
    insured_count: Optional[int] = None


class InsuranceResponse(BaseModel):
    fips: str
    insurance: InsuranceData = InsuranceData()
    sources: List[DataSource] = []


# ── Community Snapshot (Composite) ──────────────────────

class CommunitySnapshot(BaseModel):
    """Composite response bundling population, HPSA, hospitals, insurance, AI/AN, and IHS data."""
    fips: str
    state: str = ""
    county: str = ""
    population: PopulationData = PopulationData()
    aian: AIANData = AIANData()
    hpsa: HPSAResponse = HPSAResponse(state="", county="", county_fips="")
    hospitals: HospitalResponse = HospitalResponse()
    ihs_facilities: IHSResponse = IHSResponse()
    insurance: InsuranceData = InsuranceData()
    sources: List[DataSource] = []
