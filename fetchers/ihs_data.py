"""
IHS (Indian Health Service) Facility Data Module

Fetches and processes data for:
- IHS Direct Service facilities
- Tribal 638 contract/compact facilities
- Urban Indian Health Organizations

Data sources:
- Web scraping from IHS Facility Locator (https://www.ihs.gov/findfacility/)
- Comprehensive seed data as fallback
"""
from __future__ import annotations
import pandas as pd
import requests
from bs4 import BeautifulSoup
from pathlib import Path
from typing import Optional, Dict, List, Any
from math import radians, sin, cos, sqrt, atan2
import re
import json

from fetchers.cache import cache_key, is_fresh, write_cache_df, read_cache_df

# IHS Area Office boundaries (approximate state mappings)
# Note: Some states (AZ, NM, UT, TX) are served by multiple IHS Areas
# due to different tribal nations. IHS_AREAS shows ALL areas serving a state.
IHS_AREAS = {
    "Alaska": ["AK"],
    "Albuquerque": ["CO", "NM", "TX"],
    "Bemidji": ["IL", "IN", "MI", "MN", "WI"],
    "Billings": ["MT", "WY"],
    "California": ["CA"],
    "Great Plains": ["IA", "ND", "NE", "SD"],
    "Nashville": ["AL", "AR", "CT", "DC", "DE", "FL", "GA", "HI", "KY", "LA", "MA", "MD", "ME",
                  "MO", "MS", "NC", "NH", "NJ", "NY", "OH", "PA", "RI", "SC", "TN", "VA", "VT", "WV"],
    "Navajo": ["AZ", "NM", "UT"],  # Navajo Nation specific
    "Oklahoma City": ["KS", "OK", "TX"],
    "Phoenix": ["AZ", "NV", "UT"],
    "Portland": ["ID", "OR", "WA"],
    "Tucson": ["AZ"]  # Tohono O'odham specific
}

# Primary IHS Area for each state (resolves multi-area states)
STATE_TO_IHS_AREA = {
    "AK": "Alaska",
    "AL": "Nashville", "AR": "Nashville", "CT": "Nashville", "DC": "Nashville",
    "DE": "Nashville", "FL": "Nashville", "GA": "Nashville", "HI": "Nashville",
    "KY": "Nashville", "LA": "Nashville", "MA": "Nashville", "MD": "Nashville",
    "ME": "Nashville", "MO": "Nashville", "MS": "Nashville", "NC": "Nashville",
    "NH": "Nashville", "NJ": "Nashville", "NY": "Nashville", "OH": "Nashville",
    "PA": "Nashville", "RI": "Nashville", "SC": "Nashville", "TN": "Nashville",
    "VA": "Nashville", "VT": "Nashville", "WV": "Nashville",
    "AZ": "Phoenix", "NV": "Phoenix", "UT": "Phoenix",
    "CA": "California",
    "CO": "Albuquerque", "NM": "Albuquerque",
    "IA": "Great Plains", "ND": "Great Plains", "NE": "Great Plains", "SD": "Great Plains",
    "ID": "Portland", "OR": "Portland", "WA": "Portland",
    "IL": "Bemidji", "IN": "Bemidji", "MI": "Bemidji", "MN": "Bemidji", "WI": "Bemidji",
    "KS": "Oklahoma City", "OK": "Oklahoma City", "TX": "Oklahoma City",
    "MT": "Billings", "WY": "Billings",
}

# IHS Area headquarters for reference
IHS_AREA_HQ = {
    "Alaska": {"city": "Anchorage", "state": "AK"},
    "Albuquerque": {"city": "Albuquerque", "state": "NM"},
    "Bemidji": {"city": "Bemidji", "state": "MN"},
    "Billings": {"city": "Billings", "state": "MT"},
    "California": {"city": "Sacramento", "state": "CA"},
    "Great Plains": {"city": "Aberdeen", "state": "SD"},
    "Nashville": {"city": "Nashville", "state": "TN"},
    "Navajo": {"city": "Window Rock", "state": "AZ"},
    "Oklahoma City": {"city": "Oklahoma City", "state": "OK"},
    "Phoenix": {"city": "Phoenix", "state": "AZ"},
    "Portland": {"city": "Portland", "state": "OR"},
    "Tucson": {"city": "Tucson", "state": "AZ"}
}


def _normalize_state(state: str) -> str:
    """Convert full state name to 2-letter abbreviation if needed."""
    state = state.strip()
    # Already a 2-letter abbreviation
    if len(state) == 2:
        return state.upper()

    # Full state name -> abbreviation lookup
    _STATE_NAME_TO_ABBR = {
        'ALABAMA': 'AL', 'ALASKA': 'AK', 'ARIZONA': 'AZ', 'ARKANSAS': 'AR',
        'CALIFORNIA': 'CA', 'COLORADO': 'CO', 'CONNECTICUT': 'CT', 'DELAWARE': 'DE',
        'FLORIDA': 'FL', 'GEORGIA': 'GA', 'HAWAII': 'HI', 'IDAHO': 'ID',
        'ILLINOIS': 'IL', 'INDIANA': 'IN', 'IOWA': 'IA', 'KANSAS': 'KS',
        'KENTUCKY': 'KY', 'LOUISIANA': 'LA', 'MAINE': 'ME', 'MARYLAND': 'MD',
        'MASSACHUSETTS': 'MA', 'MICHIGAN': 'MI', 'MINNESOTA': 'MN', 'MISSISSIPPI': 'MS',
        'MISSOURI': 'MO', 'MONTANA': 'MT', 'NEBRASKA': 'NE', 'NEVADA': 'NV',
        'NEW HAMPSHIRE': 'NH', 'NEW JERSEY': 'NJ', 'NEW MEXICO': 'NM', 'NEW YORK': 'NY',
        'NORTH CAROLINA': 'NC', 'NORTH DAKOTA': 'ND', 'OHIO': 'OH', 'OKLAHOMA': 'OK',
        'OREGON': 'OR', 'PENNSYLVANIA': 'PA', 'RHODE ISLAND': 'RI', 'SOUTH CAROLINA': 'SC',
        'SOUTH DAKOTA': 'SD', 'TENNESSEE': 'TN', 'TEXAS': 'TX', 'UTAH': 'UT',
        'VERMONT': 'VT', 'VIRGINIA': 'VA', 'WASHINGTON': 'WA', 'WEST VIRGINIA': 'WV',
        'WISCONSIN': 'WI', 'WYOMING': 'WY', 'DISTRICT OF COLUMBIA': 'DC',
    }
    return _STATE_NAME_TO_ABBR.get(state.upper(), state.upper())


def get_ihs_area(state: str) -> str:
    """Determine primary IHS Area Office for a given state.

    Args:
        state: State abbreviation (e.g. 'CA') or full name (e.g. 'California')

    Returns:
        Primary IHS Area name (e.g. 'Phoenix' for AZ, 'California' for CA)
    """
    if not state or not state.strip():
        return "Nashville"
    abbr = _normalize_state(state)
    return STATE_TO_IHS_AREA.get(abbr, "Nashville")


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate distance between two points in miles."""
    R = 3959  # Earth's radius in miles
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    return R * c


# ============================================================================
# COMPREHENSIVE SEED DATA - Expanded facility database
# ============================================================================

IHS_FACILITIES_SEED = [
    # -------------------------------------------------------------------------
    # ALASKA AREA
    # -------------------------------------------------------------------------
    {"name": "Alaska Native Medical Center", "type": "Tribal", "city": "Anchorage", "state": "AK",
     "address": "4315 Diplomacy Dr", "zip": "99508", "phone": "(907) 563-2662",
     "lat": 61.1878, "lon": -149.8003, "services": ["Hospital", "Emergency", "Specialty Care", "Surgery", "Behavioral Health", "Dental", "Pharmacy"]},
    {"name": "Samuel Simmonds Memorial Hospital", "type": "Tribal", "city": "Barrow", "state": "AK",
     "address": "7000 Uula St", "zip": "99723", "phone": "(907) 852-4611",
     "lat": 71.2906, "lon": -156.7886, "services": ["Hospital", "Emergency", "Primary Care", "Dental"]},
    {"name": "Yukon-Kuskokwim Health Corporation", "type": "Tribal", "city": "Bethel", "state": "AK",
     "address": "700 Chief Eddie Hoffman Hwy", "zip": "99559", "phone": "(907) 543-6000",
     "lat": 60.7922, "lon": -161.7558, "services": ["Hospital", "Emergency", "Primary Care", "Behavioral Health", "Dental"]},
    {"name": "Norton Sound Health Corporation", "type": "Tribal", "city": "Nome", "state": "AK",
     "address": "1000 Greg Kruschek Ave", "zip": "99762", "phone": "(907) 443-3311",
     "lat": 64.5011, "lon": -165.4064, "services": ["Hospital", "Emergency", "Primary Care", "Dental"]},
    {"name": "SouthEast Alaska Regional Health Consortium", "type": "Tribal", "city": "Juneau", "state": "AK",
     "address": "3245 Hospital Dr", "zip": "99801", "phone": "(907) 966-8000",
     "lat": 58.3575, "lon": -134.5525, "services": ["Hospital", "Primary Care", "Behavioral Health", "Dental", "Pharmacy"]},

    # -------------------------------------------------------------------------
    # BEMIDJI AREA (Great Lakes)
    # -------------------------------------------------------------------------
    # Michigan
    {"name": "Sault Ste. Marie Tribe Health Center", "type": "Tribal", "city": "Sault Ste. Marie", "state": "MI",
     "address": "2864 Ashmun St", "zip": "49783", "phone": "(906) 632-5200",
     "lat": 46.4953, "lon": -84.3453, "services": ["Primary Care", "Dental", "Behavioral Health", "Pharmacy", "Optometry"]},
    {"name": "Hannahville Indian Community Health Center", "type": "Tribal", "city": "Wilson", "state": "MI",
     "address": "N14911 Hannahville B1 Rd", "zip": "49896", "phone": "(906) 466-2782",
     "lat": 45.9261, "lon": -87.5936, "services": ["Primary Care", "Dental", "Pharmacy", "Behavioral Health"]},
    {"name": "Keweenaw Bay Indian Community Health Center", "type": "Tribal", "city": "Baraga", "state": "MI",
     "address": "16429 Beartown Rd", "zip": "49908", "phone": "(906) 353-4500",
     "lat": 46.7786, "lon": -88.4889, "services": ["Primary Care", "Dental", "Behavioral Health", "Pharmacy"]},
    {"name": "Bay Mills Indian Community Health Center", "type": "Tribal", "city": "Brimley", "state": "MI",
     "address": "12124 W Lakeshore Dr", "zip": "49715", "phone": "(906) 248-5527",
     "lat": 46.4175, "lon": -84.5583, "services": ["Primary Care", "Dental", "Behavioral Health"]},
    {"name": "Grand Traverse Band Health Center", "type": "Tribal", "city": "Peshawbestown", "state": "MI",
     "address": "2300 Peshawbestown Rd", "zip": "49682", "phone": "(231) 534-7200",
     "lat": 44.9175, "lon": -85.6264, "services": ["Primary Care", "Dental", "Behavioral Health", "Pharmacy", "Physical Therapy"]},
    {"name": "Little Traverse Bay Bands Health Center", "type": "Tribal", "city": "Petoskey", "state": "MI",
     "address": "1345 US Highway 31 N", "zip": "49770", "phone": "(231) 242-1600",
     "lat": 45.3736, "lon": -84.9553, "services": ["Primary Care", "Dental", "Optometry", "Pharmacy", "Behavioral Health"]},
    {"name": "Pokagon Band Health Services", "type": "Tribal", "city": "Dowagiac", "state": "MI",
     "address": "58620 Sink Rd", "zip": "49047", "phone": "(269) 782-4141",
     "lat": 41.9842, "lon": -86.1086, "services": ["Primary Care", "Dental", "Behavioral Health", "Pharmacy"]},
    {"name": "Gun Lake Tribe Health & Human Services", "type": "Tribal", "city": "Shelbyville", "state": "MI",
     "address": "2872 Mission Dr", "zip": "49344", "phone": "(269) 397-1780",
     "lat": 42.5903, "lon": -85.6564, "services": ["Primary Care", "Physical Therapy", "Behavioral Health", "Pharmacy", "Traditional Medicine"]},
    {"name": "Little River Band Health Services", "type": "Tribal", "city": "Manistee", "state": "MI",
     "address": "2608 Government Center Dr", "zip": "49660", "phone": "(231) 398-2221",
     "lat": 44.2631, "lon": -86.3119, "services": ["Primary Care", "Dental", "Behavioral Health"]},
    {"name": "Nottawaseppi Huron Band Health Center", "type": "Tribal", "city": "Fulton", "state": "MI",
     "address": "1485 Mno-Bmadzewen Way", "zip": "49052", "phone": "(269) 704-8317",
     "lat": 42.0806, "lon": -85.1989, "services": ["Primary Care", "Behavioral Health", "Pharmacy"]},

    # Minnesota
    {"name": "Red Lake Hospital", "type": "IHS", "city": "Red Lake", "state": "MN",
     "address": "24760 Hospital Dr", "zip": "56671", "phone": "(218) 679-3912",
     "lat": 47.8764, "lon": -95.0169, "services": ["Hospital", "Emergency", "Primary Care", "Dental", "Pharmacy"]},
    {"name": "Cass Lake IHS Hospital", "type": "IHS", "city": "Cass Lake", "state": "MN",
     "address": "425 7th St NW", "zip": "56633", "phone": "(218) 335-3200",
     "lat": 47.3797, "lon": -94.6028, "services": ["Hospital", "Emergency", "Primary Care", "Behavioral Health"]},
    {"name": "White Earth Health Center", "type": "Tribal", "city": "White Earth", "state": "MN",
     "address": "40520 Co Hwy 34", "zip": "56591", "phone": "(218) 983-6300",
     "lat": 47.0919, "lon": -95.8436, "services": ["Primary Care", "Dental", "Behavioral Health", "Pharmacy"]},
    {"name": "Fond du Lac Health Center", "type": "Tribal", "city": "Cloquet", "state": "MN",
     "address": "927 Trettel Ln", "zip": "55720", "phone": "(218) 879-1227",
     "lat": 46.7219, "lon": -92.4597, "services": ["Primary Care", "Dental", "Behavioral Health", "Pharmacy"]},
    {"name": "Mille Lacs Band Health Center", "type": "Tribal", "city": "Onamia", "state": "MN",
     "address": "43500 Oodena Dr", "zip": "56359", "phone": "(320) 532-4163",
     "lat": 46.0750, "lon": -93.6678, "services": ["Primary Care", "Dental", "Behavioral Health"]},

    # Wisconsin
    {"name": "Oneida Community Health Center", "type": "Tribal", "city": "Oneida", "state": "WI",
     "address": "525 Airport Dr", "zip": "54155", "phone": "(920) 869-2711",
     "lat": 44.4969, "lon": -88.1853, "services": ["Primary Care", "Dental", "Behavioral Health", "Pharmacy", "Optometry"]},
    {"name": "Ho-Chunk Health Care Center", "type": "Tribal", "city": "Black River Falls", "state": "WI",
     "address": "N6520 Lumberjack Guy Rd", "zip": "54615", "phone": "(715) 284-9851",
     "lat": 44.2947, "lon": -90.8514, "services": ["Primary Care", "Dental", "Behavioral Health", "Pharmacy"]},
    {"name": "Lac du Flambeau Health Center", "type": "Tribal", "city": "Lac du Flambeau", "state": "WI",
     "address": "560 Cowboy Rd", "zip": "54538", "phone": "(715) 588-3371",
     "lat": 45.9700, "lon": -89.8922, "services": ["Primary Care", "Dental", "Behavioral Health"]},
    {"name": "Menominee Tribal Clinic", "type": "Tribal", "city": "Keshena", "state": "WI",
     "address": "W3275 Wolf River Dr", "zip": "54135", "phone": "(715) 799-5100",
     "lat": 44.8836, "lon": -88.6289, "services": ["Primary Care", "Dental", "Behavioral Health", "Pharmacy"]},
    {"name": "Bad River Health Center", "type": "Tribal", "city": "Odanah", "state": "WI",
     "address": "54321 Muskrat Dr", "zip": "54861", "phone": "(715) 682-7111",
     "lat": 46.6122, "lon": -90.6836, "services": ["Primary Care", "Dental", "Behavioral Health"]},
    {"name": "Stockbridge-Munsee Health & Wellness Center", "type": "Tribal", "city": "Bowler", "state": "WI",
     "address": "W12802 County Hwy A", "zip": "54416", "phone": "(715) 793-5000",
     "lat": 44.8597, "lon": -88.9797, "services": ["Primary Care", "Dental", "Behavioral Health", "Pharmacy"]},

    # -------------------------------------------------------------------------
    # NAVAJO AREA
    # -------------------------------------------------------------------------
    {"name": "Phoenix Indian Medical Center", "type": "IHS", "city": "Phoenix", "state": "AZ",
     "address": "4212 N 16th St", "zip": "85016", "phone": "(602) 263-1200",
     "lat": 33.4584, "lon": -112.0740, "services": ["Hospital", "Emergency", "Specialty Care", "Surgery", "Behavioral Health", "Dental", "Pharmacy"]},
    {"name": "Gallup Indian Medical Center", "type": "IHS", "city": "Gallup", "state": "NM",
     "address": "516 E Nizhoni Blvd", "zip": "87301", "phone": "(505) 722-1000",
     "lat": 35.5281, "lon": -108.7426, "services": ["Hospital", "Emergency", "Specialty Care", "Surgery", "Dental", "Pharmacy"]},
    {"name": "Shiprock Northern Navajo Medical Center", "type": "IHS", "city": "Shiprock", "state": "NM",
     "address": "Hwy 491 N", "zip": "87420", "phone": "(505) 368-6001",
     "lat": 36.7856, "lon": -108.6870, "services": ["Hospital", "Emergency", "Primary Care", "Dental", "Behavioral Health"]},
    {"name": "Chinle Comprehensive Health Care Facility", "type": "IHS", "city": "Chinle", "state": "AZ",
     "address": "Hwy 191 & Hospital Dr", "zip": "86503", "phone": "(928) 674-7001",
     "lat": 36.1542, "lon": -109.5526, "services": ["Hospital", "Emergency", "Primary Care", "Dental", "Pharmacy"]},
    {"name": "Tuba City Regional Health Care", "type": "Tribal", "city": "Tuba City", "state": "AZ",
     "address": "167 N Main St", "zip": "86045", "phone": "(928) 283-2501",
     "lat": 36.1350, "lon": -111.2397, "services": ["Hospital", "Emergency", "Specialty Care", "Dental", "Pharmacy", "Dialysis"]},
    {"name": "Fort Defiance Indian Hospital", "type": "IHS", "city": "Fort Defiance", "state": "AZ",
     "address": "Fort Defiance Rd", "zip": "86504", "phone": "(928) 729-8000",
     "lat": 35.7447, "lon": -109.0639, "services": ["Hospital", "Emergency", "Primary Care", "Dental"]},
    {"name": "Kayenta Health Center", "type": "IHS", "city": "Kayenta", "state": "AZ",
     "address": "Hwy 163", "zip": "86033", "phone": "(928) 697-4000",
     "lat": 36.7278, "lon": -110.2544, "services": ["Primary Care", "Dental", "Pharmacy", "Behavioral Health"]},
    {"name": "Winslow Indian Health Care Center", "type": "IHS", "city": "Winslow", "state": "AZ",
     "address": "500 N Indiana Ave", "zip": "86047", "phone": "(928) 289-4646",
     "lat": 35.0286, "lon": -110.6975, "services": ["Primary Care", "Dental", "Behavioral Health"]},
    {"name": "Crownpoint Health Care Facility", "type": "IHS", "city": "Crownpoint", "state": "NM",
     "address": "Route 9 & Hospital Rd", "zip": "87313", "phone": "(505) 786-5291",
     "lat": 35.6778, "lon": -108.1506, "services": ["Hospital", "Emergency", "Primary Care", "Dental"]},
    {"name": "Tohatchi Health Center", "type": "IHS", "city": "Tohatchi", "state": "NM",
     "address": "Hwy 491", "zip": "87325", "phone": "(505) 733-2241",
     "lat": 35.8533, "lon": -108.7536, "services": ["Primary Care", "Dental", "Pharmacy"]},

    # -------------------------------------------------------------------------
    # PHOENIX AREA
    # -------------------------------------------------------------------------
    {"name": "San Carlos Apache Healthcare", "type": "Tribal", "city": "San Carlos", "state": "AZ",
     "address": "103 Medicine Way", "zip": "85550", "phone": "(928) 475-1400",
     "lat": 33.3464, "lon": -110.4564, "services": ["Hospital", "Emergency", "Primary Care", "Dental", "Behavioral Health"]},
    {"name": "Whiteriver Indian Hospital", "type": "IHS", "city": "Whiteriver", "state": "AZ",
     "address": "200 W Hospital Dr", "zip": "85941", "phone": "(928) 338-4911",
     "lat": 33.8342, "lon": -109.9656, "services": ["Hospital", "Emergency", "Primary Care", "Dental", "Pharmacy"]},
    {"name": "Salt River Pima-Maricopa Health Center", "type": "Tribal", "city": "Scottsdale", "state": "AZ",
     "address": "10005 E Osborn Rd", "zip": "85256", "phone": "(480) 946-9066",
     "lat": 33.4700, "lon": -111.8447, "services": ["Primary Care", "Dental", "Behavioral Health", "Pharmacy"]},
    {"name": "Gila River Health Care", "type": "Tribal", "city": "Sacaton", "state": "AZ",
     "address": "483 W Seed Farm Rd", "zip": "85147", "phone": "(520) 562-3321",
     "lat": 33.0778, "lon": -111.7397, "services": ["Hospital", "Primary Care", "Dental", "Pharmacy", "Dialysis"]},
    {"name": "Ak-Chin Health Clinic", "type": "Tribal", "city": "Maricopa", "state": "AZ",
     "address": "48203 W Farrell Rd", "zip": "85138", "phone": "(520) 568-3881",
     "lat": 33.0275, "lon": -112.0508, "services": ["Primary Care", "Dental", "Behavioral Health"]},
    {"name": "Pyramid Lake Health Clinic", "type": "Tribal", "city": "Nixon", "state": "NV",
     "address": "705 Highway 447", "zip": "89424", "phone": "(775) 574-1018",
     "lat": 39.8511, "lon": -119.3481, "services": ["Primary Care", "Dental", "Behavioral Health"]},
    {"name": "Reno-Sparks Indian Colony Health Center", "type": "Tribal", "city": "Reno", "state": "NV",
     "address": "34 Reservation Rd", "zip": "89502", "phone": "(775) 329-5162",
     "lat": 39.4847, "lon": -119.7897, "services": ["Primary Care", "Dental", "Behavioral Health", "Pharmacy"]},

    # -------------------------------------------------------------------------
    # TUCSON AREA
    # -------------------------------------------------------------------------
    {"name": "Sells Indian Hospital", "type": "IHS", "city": "Sells", "state": "AZ",
     "address": "Hwy 86", "zip": "85634", "phone": "(520) 383-7211",
     "lat": 31.9122, "lon": -111.8792, "services": ["Hospital", "Emergency", "Primary Care", "Dental", "Pharmacy"]},
    {"name": "San Xavier Health Center", "type": "IHS", "city": "Tucson", "state": "AZ",
     "address": "7900 S J Stock Rd", "zip": "85746", "phone": "(520) 295-2550",
     "lat": 32.1072, "lon": -111.0017, "services": ["Primary Care", "Dental", "Behavioral Health"]},
    {"name": "Santa Rosa Health Center", "type": "IHS", "city": "Santa Rosa", "state": "AZ",
     "address": "HC 01 Box 8600", "zip": "85239", "phone": "(520) 361-2211",
     "lat": 32.4711, "lon": -111.9000, "services": ["Primary Care", "Dental"]},

    # -------------------------------------------------------------------------
    # ALBUQUERQUE AREA
    # -------------------------------------------------------------------------
    {"name": "Albuquerque Indian Health Center", "type": "IHS", "city": "Albuquerque", "state": "NM",
     "address": "801 Vassar Dr NE", "zip": "87106", "phone": "(505) 248-4000",
     "lat": 35.0875, "lon": -106.6128, "services": ["Primary Care", "Dental", "Behavioral Health", "Pharmacy", "Optometry"]},
    {"name": "Acoma-Canoncito-Laguna Hospital", "type": "IHS", "city": "San Fidel", "state": "NM",
     "address": "808 Veterans Blvd", "zip": "87049", "phone": "(505) 552-5300",
     "lat": 35.0753, "lon": -107.5972, "services": ["Hospital", "Emergency", "Primary Care", "Dental"]},
    {"name": "Santa Fe Indian Hospital", "type": "IHS", "city": "Santa Fe", "state": "NM",
     "address": "1700 Cerrillos Rd", "zip": "87505", "phone": "(505) 946-9211",
     "lat": 35.6597, "lon": -105.9781, "services": ["Hospital", "Emergency", "Primary Care", "Dental", "Specialty Care"]},
    {"name": "Zuni Comprehensive Health Center", "type": "IHS", "city": "Zuni", "state": "NM",
     "address": "Route 301 N B St", "zip": "87327", "phone": "(505) 782-4431",
     "lat": 35.0711, "lon": -108.8450, "services": ["Hospital", "Emergency", "Primary Care", "Dental", "Pharmacy"]},
    {"name": "Taos-Picuris Health Center", "type": "Tribal", "city": "Taos", "state": "NM",
     "address": "1480 Weimer Rd", "zip": "87571", "phone": "(575) 758-7824",
     "lat": 36.4064, "lon": -105.5733, "services": ["Primary Care", "Dental", "Behavioral Health"]},
    {"name": "Southern Ute Health Center", "type": "Tribal", "city": "Ignacio", "state": "CO",
     "address": "356 Ouray Dr", "zip": "81137", "phone": "(970) 563-4581",
     "lat": 37.1203, "lon": -107.6322, "services": ["Primary Care", "Dental", "Behavioral Health", "Pharmacy"]},
    {"name": "Ute Mountain Ute Health Center", "type": "Tribal", "city": "Towaoc", "state": "CO",
     "address": "18 Mike Wash Rd", "zip": "81334", "phone": "(970) 564-5407",
     "lat": 37.2047, "lon": -108.7300, "services": ["Primary Care", "Dental", "Behavioral Health"]},

    # -------------------------------------------------------------------------
    # OKLAHOMA CITY AREA
    # -------------------------------------------------------------------------
    {"name": "Lawton Indian Hospital", "type": "IHS", "city": "Lawton", "state": "OK",
     "address": "1515 Lawrie Tatum Rd", "zip": "73507", "phone": "(580) 353-0350",
     "lat": 34.6247, "lon": -98.4092, "services": ["Hospital", "Emergency", "Primary Care", "Dental", "Pharmacy"]},
    {"name": "Claremore Indian Hospital", "type": "IHS", "city": "Claremore", "state": "OK",
     "address": "101 S Moore Ave", "zip": "74017", "phone": "(918) 342-6200",
     "lat": 36.3142, "lon": -95.6164, "services": ["Hospital", "Emergency", "Primary Care", "Dental", "Specialty Care"]},
    {"name": "Pawnee Indian Health Center", "type": "IHS", "city": "Pawnee", "state": "OK",
     "address": "1201 Heritage Circle", "zip": "74058", "phone": "(918) 762-2517",
     "lat": 36.3378, "lon": -96.8003, "services": ["Primary Care", "Dental", "Behavioral Health", "Pharmacy"]},
    {"name": "Chickasaw Nation Medical Center", "type": "Tribal", "city": "Ada", "state": "OK",
     "address": "1921 Stonecipher Blvd", "zip": "74820", "phone": "(580) 436-3980",
     "lat": 34.7647, "lon": -96.6781, "services": ["Hospital", "Emergency", "Specialty Care", "Surgery", "Dental", "Pharmacy"]},
    {"name": "Choctaw Nation Health Care Center", "type": "Tribal", "city": "Talihina", "state": "OK",
     "address": "One Choctaw Way", "zip": "74571", "phone": "(918) 567-7000",
     "lat": 34.7517, "lon": -95.0475, "services": ["Hospital", "Emergency", "Primary Care", "Dental", "Pharmacy"]},
    {"name": "Cherokee Nation W.W. Hastings Hospital", "type": "Tribal", "city": "Tahlequah", "state": "OK",
     "address": "100 S Bliss Ave", "zip": "74464", "phone": "(918) 458-3100",
     "lat": 35.9178, "lon": -94.9683, "services": ["Hospital", "Emergency", "Primary Care", "Dental", "Pharmacy", "Behavioral Health"]},
    {"name": "Muscogee (Creek) Nation Health Center", "type": "Tribal", "city": "Okmulgee", "state": "OK",
     "address": "1401 E Morris St", "zip": "74447", "phone": "(918) 756-1883",
     "lat": 35.6256, "lon": -95.9533, "services": ["Primary Care", "Dental", "Behavioral Health", "Pharmacy"]},
    {"name": "Citizen Potawatomi Nation Health Center", "type": "Tribal", "city": "Shawnee", "state": "OK",
     "address": "2307 S Gordon Cooper Dr", "zip": "74801", "phone": "(405) 273-5236",
     "lat": 35.3147, "lon": -96.9181, "services": ["Primary Care", "Dental", "Behavioral Health", "Pharmacy"]},
    {"name": "Haskell Health Center", "type": "IHS", "city": "Lawrence", "state": "KS",
     "address": "2415 Massachusetts St", "zip": "66046", "phone": "(785) 843-3750",
     "lat": 38.9283, "lon": -95.2367, "services": ["Primary Care", "Dental", "Behavioral Health"]},

    # -------------------------------------------------------------------------
    # GREAT PLAINS AREA
    # -------------------------------------------------------------------------
    {"name": "Pine Ridge Hospital", "type": "IHS", "city": "Pine Ridge", "state": "SD",
     "address": "E Hwy 18", "zip": "57770", "phone": "(605) 867-5131",
     "lat": 43.0258, "lon": -102.5561, "services": ["Hospital", "Emergency", "Primary Care", "Dental", "Pharmacy"]},
    {"name": "Rosebud Hospital", "type": "IHS", "city": "Rosebud", "state": "SD",
     "address": "Hwy 18", "zip": "57570", "phone": "(605) 747-2231",
     "lat": 43.2339, "lon": -100.8531, "services": ["Hospital", "Emergency", "Primary Care", "Dental", "Behavioral Health"]},
    {"name": "Eagle Butte Hospital", "type": "IHS", "city": "Eagle Butte", "state": "SD",
     "address": "317 Main St", "zip": "57625", "phone": "(605) 964-7724",
     "lat": 45.0044, "lon": -101.2331, "services": ["Hospital", "Emergency", "Primary Care", "Dental"]},
    {"name": "Rapid City Indian Health Service", "type": "IHS", "city": "Rapid City", "state": "SD",
     "address": "3200 Canyon Lake Dr", "zip": "57702", "phone": "(605) 355-2500",
     "lat": 44.0631, "lon": -103.2494, "services": ["Primary Care", "Dental", "Behavioral Health", "Pharmacy"]},
    {"name": "Winnebago Hospital", "type": "IHS", "city": "Winnebago", "state": "NE",
     "address": "100 Hospital Rd", "zip": "68071", "phone": "(402) 878-2231",
     "lat": 42.2361, "lon": -96.4711, "services": ["Hospital", "Emergency", "Primary Care", "Dental"]},
    {"name": "Omaha-Winnebago Health Center", "type": "IHS", "city": "Macy", "state": "NE",
     "address": "100 Hospital Dr", "zip": "68039", "phone": "(402) 837-5381",
     "lat": 42.1128, "lon": -96.3569, "services": ["Primary Care", "Dental", "Behavioral Health"]},
    {"name": "Fort Thompson Health Center", "type": "IHS", "city": "Fort Thompson", "state": "SD",
     "address": "E Main St", "zip": "57339", "phone": "(605) 245-2286",
     "lat": 44.0678, "lon": -99.4378, "services": ["Primary Care", "Dental", "Pharmacy"]},
    {"name": "Standing Rock IHS Hospital", "type": "IHS", "city": "Fort Yates", "state": "ND",
     "address": "10 N River Rd", "zip": "58538", "phone": "(701) 854-3831",
     "lat": 46.0869, "lon": -100.6297, "services": ["Hospital", "Emergency", "Primary Care", "Dental"]},
    {"name": "Turtle Mountain Health Center", "type": "Tribal", "city": "Belcourt", "state": "ND",
     "address": "2 Tribal Rd", "zip": "58316", "phone": "(701) 477-6111",
     "lat": 48.8392, "lon": -99.7469, "services": ["Primary Care", "Dental", "Behavioral Health", "Pharmacy"]},
    {"name": "Spirit Lake Health Center", "type": "IHS", "city": "Fort Totten", "state": "ND",
     "address": "7491 Highway 57", "zip": "58335", "phone": "(701) 766-4291",
     "lat": 47.9742, "lon": -99.0056, "services": ["Primary Care", "Dental", "Behavioral Health"]},

    # -------------------------------------------------------------------------
    # BILLINGS AREA
    # -------------------------------------------------------------------------
    {"name": "Crow/Northern Cheyenne Hospital", "type": "IHS", "city": "Crow Agency", "state": "MT",
     "address": "10110 S 7650 E", "zip": "59022", "phone": "(406) 638-2626",
     "lat": 45.6008, "lon": -107.4611, "services": ["Hospital", "Emergency", "Primary Care", "Dental"]},
    {"name": "Fort Belknap Health Center", "type": "IHS", "city": "Harlem", "state": "MT",
     "address": "669 Agency Main St", "zip": "59526", "phone": "(406) 353-3195",
     "lat": 48.5233, "lon": -108.7697, "services": ["Primary Care", "Dental", "Behavioral Health", "Pharmacy"]},
    {"name": "Blackfeet Community Hospital", "type": "IHS", "city": "Browning", "state": "MT",
     "address": "760 New Hospital Cir", "zip": "59417", "phone": "(406) 338-6100",
     "lat": 48.5572, "lon": -113.0139, "services": ["Hospital", "Emergency", "Primary Care", "Dental", "Pharmacy"]},
    {"name": "Fort Peck Health Center", "type": "IHS", "city": "Poplar", "state": "MT",
     "address": "605 Court Ave", "zip": "59255", "phone": "(406) 768-3491",
     "lat": 48.1103, "lon": -105.1978, "services": ["Primary Care", "Dental", "Behavioral Health"]},
    {"name": "Rocky Boy Health Center", "type": "IHS", "city": "Box Elder", "state": "MT",
     "address": "Rural Route 544", "zip": "59521", "phone": "(406) 395-4486",
     "lat": 48.2678, "lon": -109.8722, "services": ["Primary Care", "Dental", "Pharmacy"]},
    {"name": "Wind River Health Center", "type": "IHS", "city": "Fort Washakie", "state": "WY",
     "address": "22 Black Coal Dr", "zip": "82514", "phone": "(307) 332-9400",
     "lat": 42.9939, "lon": -108.9122, "services": ["Primary Care", "Dental", "Behavioral Health", "Pharmacy"]},

    # -------------------------------------------------------------------------
    # PORTLAND AREA
    # -------------------------------------------------------------------------
    {"name": "Yakama Indian Health Center", "type": "Tribal", "city": "Toppenish", "state": "WA",
     "address": "401 Buster Rd", "zip": "98948", "phone": "(509) 865-2102",
     "lat": 46.3792, "lon": -120.3106, "services": ["Primary Care", "Dental", "Behavioral Health", "Pharmacy"]},
    {"name": "Colville Tribal Health Clinic", "type": "Tribal", "city": "Nespelem", "state": "WA",
     "address": "21 Colville St", "zip": "99155", "phone": "(509) 634-2600",
     "lat": 48.1703, "lon": -118.9711, "services": ["Primary Care", "Dental", "Behavioral Health"]},
    {"name": "Tulalip Health Clinic", "type": "Tribal", "city": "Tulalip", "state": "WA",
     "address": "7520 Totem Beach Rd", "zip": "98271", "phone": "(360) 716-5600",
     "lat": 48.0706, "lon": -122.2892, "services": ["Primary Care", "Dental", "Behavioral Health", "Pharmacy"]},
    {"name": "Lummi Health Center", "type": "Tribal", "city": "Bellingham", "state": "WA",
     "address": "2592 Kwina Rd", "zip": "98226", "phone": "(360) 384-0464",
     "lat": 48.8119, "lon": -122.6197, "services": ["Primary Care", "Dental", "Behavioral Health"]},
    {"name": "Warm Springs Health & Wellness Center", "type": "Tribal", "city": "Warm Springs", "state": "OR",
     "address": "1268 Veterans St", "zip": "97761", "phone": "(541) 553-2610",
     "lat": 44.7631, "lon": -121.2608, "services": ["Primary Care", "Dental", "Behavioral Health", "Pharmacy"]},
    {"name": "Grand Ronde Health & Wellness Center", "type": "Tribal", "city": "Grand Ronde", "state": "OR",
     "address": "9615 Grand Ronde Rd", "zip": "97347", "phone": "(503) 879-2032",
     "lat": 45.0569, "lon": -123.6122, "services": ["Primary Care", "Dental", "Behavioral Health", "Pharmacy"]},
    {"name": "Nez Perce Tribal Health Center", "type": "Tribal", "city": "Lapwai", "state": "ID",
     "address": "111 Bever Grade Rd", "zip": "83540", "phone": "(208) 843-2271",
     "lat": 46.4036, "lon": -116.8019, "services": ["Primary Care", "Dental", "Behavioral Health"]},
    {"name": "Coeur d'Alene Tribal Health Center", "type": "Tribal", "city": "Plummer", "state": "ID",
     "address": "2 S Tekoa St", "zip": "83851", "phone": "(208) 686-1931",
     "lat": 47.3314, "lon": -116.8878, "services": ["Primary Care", "Dental", "Behavioral Health", "Pharmacy"]},
    {"name": "Shoshone-Bannock Health Center", "type": "Tribal", "city": "Fort Hall", "state": "ID",
     "address": "1 Clinic Rd", "zip": "83203", "phone": "(208) 238-5400",
     "lat": 43.0336, "lon": -112.4339, "services": ["Primary Care", "Dental", "Behavioral Health", "Pharmacy"]},

    # -------------------------------------------------------------------------
    # CALIFORNIA AREA
    # -------------------------------------------------------------------------
    {"name": "Sacramento Native American Health Center", "type": "Urban", "city": "Sacramento", "state": "CA",
     "address": "2020 J St", "zip": "95811", "phone": "(916) 341-0575",
     "lat": 38.5775, "lon": -121.4833, "services": ["Primary Care", "Dental", "Behavioral Health", "Pharmacy"]},
    {"name": "Native American Health Center Oakland", "type": "Urban", "city": "Oakland", "state": "CA",
     "address": "3124 International Blvd", "zip": "94601", "phone": "(510) 434-5433",
     "lat": 37.7794, "lon": -122.2189, "services": ["Primary Care", "Dental", "Behavioral Health", "Pharmacy"]},
    {"name": "Indian Health Council Inc", "type": "Tribal", "city": "Pauma Valley", "state": "CA",
     "address": "50100 Golsh Rd", "zip": "92061", "phone": "(760) 749-1410",
     "lat": 33.3400, "lon": -116.9794, "services": ["Primary Care", "Dental", "Behavioral Health", "Pharmacy"]},
    {"name": "United American Indian Involvement", "type": "Urban", "city": "Los Angeles", "state": "CA",
     "address": "1125 W 6th St", "zip": "90017", "phone": "(213) 202-3970",
     "lat": 34.0553, "lon": -118.2631, "services": ["Primary Care", "Behavioral Health", "Substance Abuse"]},
    {"name": "Sonoma County Indian Health Project", "type": "Tribal", "city": "Santa Rosa", "state": "CA",
     "address": "144 Stony Point Rd", "zip": "95401", "phone": "(707) 521-4545",
     "lat": 38.4397, "lon": -122.7314, "services": ["Primary Care", "Dental", "Behavioral Health"]},
    {"name": "K'ima:w Medical Center", "type": "Tribal", "city": "Hoopa", "state": "CA",
     "address": "485 K St", "zip": "95546", "phone": "(530) 625-4261",
     "lat": 41.0511, "lon": -123.6708, "services": ["Hospital", "Emergency", "Primary Care", "Dental", "Pharmacy"]},
    {"name": "Toiyabe Indian Health Project", "type": "Tribal", "city": "Bishop", "state": "CA",
     "address": "52 Tu Su Lane", "zip": "93514", "phone": "(760) 873-8464",
     "lat": 37.3633, "lon": -118.3942, "services": ["Primary Care", "Dental", "Behavioral Health", "Pharmacy"]},
    {"name": "Karuk Tribal Health Program", "type": "Tribal", "city": "Yreka", "state": "CA",
     "address": "64236 Second Ave", "zip": "96097", "phone": "(530) 842-3627",
     "lat": 41.7358, "lon": -122.6342, "services": ["Primary Care", "Dental", "Behavioral Health"]},
    {"name": "Tule River Indian Health Center", "type": "Tribal", "city": "Porterville", "state": "CA",
     "address": "340 N Reservation Rd", "zip": "93257", "phone": "(559) 781-4271",
     "lat": 36.0450, "lon": -118.9892, "services": ["Primary Care", "Dental", "Behavioral Health"]},

    # -------------------------------------------------------------------------
    # NASHVILLE AREA (Eastern United States)
    # -------------------------------------------------------------------------
    {"name": "Eastern Band of Cherokee Indians Health Center", "type": "Tribal", "city": "Cherokee", "state": "NC",
     "address": "1 Hospital Rd", "zip": "28719", "phone": "(828) 497-9163",
     "lat": 35.4775, "lon": -83.3147, "services": ["Hospital", "Emergency", "Primary Care", "Dental", "Pharmacy", "Dialysis"]},
    {"name": "Catawba Service Unit", "type": "IHS", "city": "Rock Hill", "state": "SC",
     "address": "996 Avenue of the Nations", "zip": "29730", "phone": "(803) 366-3062",
     "lat": 34.9483, "lon": -80.9906, "services": ["Primary Care", "Dental", "Behavioral Health"]},
    {"name": "Poarch Creek Indians Health Center", "type": "Tribal", "city": "Atmore", "state": "AL",
     "address": "5811 Jack Springs Rd", "zip": "36502", "phone": "(251) 368-9136",
     "lat": 31.0233, "lon": -87.4931, "services": ["Primary Care", "Dental", "Behavioral Health", "Pharmacy"]},
    {"name": "Mississippi Band of Choctaw Indians Health Center", "type": "Tribal", "city": "Philadelphia", "state": "MS",
     "address": "210 Hospital Circle", "zip": "39350", "phone": "(601) 389-4040",
     "lat": 32.7519, "lon": -89.1211, "services": ["Hospital", "Emergency", "Primary Care", "Dental", "Pharmacy"]},
    {"name": "Mashpee Wampanoag Health Service", "type": "Tribal", "city": "Mashpee", "state": "MA",
     "address": "483 Great Neck Rd S", "zip": "02649", "phone": "(508) 477-6967",
     "lat": 41.6489, "lon": -70.4839, "services": ["Primary Care", "Behavioral Health"]},
    {"name": "Mohegan Tribal Health Dept", "type": "Tribal", "city": "Uncasville", "state": "CT",
     "address": "13 Crow Hill Rd", "zip": "06382", "phone": "(860) 862-6100",
     "lat": 41.4467, "lon": -72.1106, "services": ["Primary Care", "Behavioral Health", "Dental"]},
    {"name": "Seminole Tribe Health Department", "type": "Tribal", "city": "Hollywood", "state": "FL",
     "address": "3006 Josie Billie Ave", "zip": "33024", "phone": "(954) 962-2009",
     "lat": 26.0425, "lon": -80.2131, "services": ["Primary Care", "Dental", "Behavioral Health", "Pharmacy"]},
    {"name": "Miccosukee Health Clinic", "type": "Tribal", "city": "Miami", "state": "FL",
     "address": "Mile Marker 70 US Hwy 41", "zip": "33194", "phone": "(305) 223-8380",
     "lat": 25.7617, "lon": -80.8261, "services": ["Primary Care", "Dental", "Pharmacy"]},
    {"name": "Seneca Nation Health System", "type": "Tribal", "city": "Salamanca", "state": "NY",
     "address": "987 RC Hoag Dr", "zip": "14779", "phone": "(716) 945-5894",
     "lat": 42.1578, "lon": -78.7150, "services": ["Primary Care", "Dental", "Behavioral Health", "Pharmacy"]},
    {"name": "St. Regis Mohawk Health Services", "type": "Tribal", "city": "Hogansburg", "state": "NY",
     "address": "412 State Route 37", "zip": "13655", "phone": "(518) 358-3141",
     "lat": 44.9808, "lon": -74.6628, "services": ["Primary Care", "Dental", "Behavioral Health", "Pharmacy"]},

    # -------------------------------------------------------------------------
    # URBAN INDIAN HEALTH ORGANIZATIONS
    # -------------------------------------------------------------------------
    {"name": "American Indian Health Service of Chicago", "type": "Urban", "city": "Chicago", "state": "IL",
     "address": "838 W Irving Park Rd", "zip": "60613", "phone": "(773) 883-9100",
     "lat": 41.9544, "lon": -87.6503, "services": ["Primary Care", "Behavioral Health", "Dental", "Substance Abuse"]},
    {"name": "Gerald L. Ignace Indian Health Center", "type": "Urban", "city": "Milwaukee", "state": "WI",
     "address": "930 N 27th St", "zip": "53208", "phone": "(414) 383-9526",
     "lat": 43.0428, "lon": -87.9478, "services": ["Primary Care", "Dental", "Behavioral Health"]},
    {"name": "Seattle Indian Health Board", "type": "Urban", "city": "Seattle", "state": "WA",
     "address": "611 12th Ave S", "zip": "98144", "phone": "(206) 324-9360",
     "lat": 47.5978, "lon": -122.3172, "services": ["Primary Care", "Dental", "Behavioral Health", "Traditional Medicine", "Pharmacy"]},
    {"name": "Denver Indian Health and Family Services", "type": "Urban", "city": "Denver", "state": "CO",
     "address": "1633 Fillmore St", "zip": "80206", "phone": "(303) 953-6200",
     "lat": 39.7436, "lon": -104.9536, "services": ["Primary Care", "Behavioral Health", "Dental", "Traditional Medicine"]},
    {"name": "Minneapolis American Indian Center Health", "type": "Urban", "city": "Minneapolis", "state": "MN",
     "address": "1530 E Franklin Ave", "zip": "55404", "phone": "(612) 879-1700",
     "lat": 44.9625, "lon": -93.2606, "services": ["Primary Care", "Behavioral Health", "Traditional Medicine"]},
    {"name": "Indian Health Board of Minneapolis", "type": "Urban", "city": "Minneapolis", "state": "MN",
     "address": "1315 E 24th St", "zip": "55404", "phone": "(612) 721-9800",
     "lat": 44.9569, "lon": -93.2569, "services": ["Primary Care", "Dental", "Behavioral Health", "Pharmacy"]},
    {"name": "Native American Community Health Center", "type": "Urban", "city": "Phoenix", "state": "AZ",
     "address": "4041 N Central Ave", "zip": "85012", "phone": "(602) 279-5262",
     "lat": 33.4931, "lon": -112.0739, "services": ["Primary Care", "Dental", "Behavioral Health", "Pharmacy"]},
    {"name": "First Nations Community Healthsource", "type": "Urban", "city": "Albuquerque", "state": "NM",
     "address": "5608 Zuni Rd SE", "zip": "87108", "phone": "(505) 262-2481",
     "lat": 35.0594, "lon": -106.5886, "services": ["Primary Care", "Dental", "Behavioral Health", "Pharmacy"]},
    {"name": "Hunter Health Clinic Native American Program", "type": "Urban", "city": "Wichita", "state": "KS",
     "address": "2318 E Central Ave", "zip": "67214", "phone": "(316) 262-2415",
     "lat": 37.6889, "lon": -97.3111, "services": ["Primary Care", "Dental", "Behavioral Health"]},
    {"name": "Native American Rehabilitation Association", "type": "Urban", "city": "Portland", "state": "OR",
     "address": "1776 SW Madison St", "zip": "97205", "phone": "(503) 224-1044",
     "lat": 45.5172, "lon": -122.6897, "services": ["Primary Care", "Behavioral Health", "Substance Abuse", "Traditional Medicine"]},
    {"name": "San Diego American Indian Health Center", "type": "Urban", "city": "San Diego", "state": "CA",
     "address": "2630 First Ave", "zip": "92103", "phone": "(619) 234-2158",
     "lat": 32.7372, "lon": -117.1628, "services": ["Primary Care", "Dental", "Behavioral Health"]},
    {"name": "Indian Health Center of Santa Clara Valley", "type": "Urban", "city": "San Jose", "state": "CA",
     "address": "602 E Santa Clara St", "zip": "95112", "phone": "(408) 445-3400",
     "lat": 37.3414, "lon": -121.8850, "services": ["Primary Care", "Dental", "Behavioral Health", "Pharmacy"]},
    {"name": "Urban Inter-Tribal Center of Texas", "type": "Urban", "city": "Dallas", "state": "TX",
     "address": "209 E Jefferson Blvd", "zip": "75203", "phone": "(214) 941-1050",
     "lat": 32.7478, "lon": -96.7919, "services": ["Primary Care", "Behavioral Health"]},
    {"name": "Native American Community Services", "type": "Urban", "city": "Buffalo", "state": "NY",
     "address": "1005 Grant St", "zip": "14207", "phone": "(716) 874-2797",
     "lat": 42.9314, "lon": -78.8928, "services": ["Primary Care", "Behavioral Health", "Traditional Medicine"]},
    {"name": "Baltimore American Indian Center Health", "type": "Urban", "city": "Baltimore", "state": "MD",
     "address": "113 S Broadway", "zip": "21231", "phone": "(410) 675-3535",
     "lat": 39.2872, "lon": -76.5928, "services": ["Primary Care", "Behavioral Health"]},
]


# ============================================================================
# HRSA HEALTH CENTER API
# ============================================================================

HRSA_API_URL = "https://data.hrsa.gov/HDWLocatorApi/HealthCenters/find"


def fetch_hrsa_health_centers(
    lat: float, lon: float, radius_miles: float = 50, refresh: bool = False
) -> pd.DataFrame:
    """
    Fetch FQHC / Community Health Centers from HRSA API.

    Args:
        lat: Search center latitude
        lon: Search center longitude
        radius_miles: Search radius in miles
        refresh: Force refresh bypassing cache

    Returns:
        DataFrame with HRSA health center data normalized to IHS schema
    """
    key = cache_key("hrsa_hc", lat=round(lat, 2), lon=round(lon, 2), r=int(radius_miles))

    if not refresh and is_fresh(key, tier="weekly"):
        cached = read_cache_df(key)
        if not cached.empty:
            return cached

    try:
        params = {"lat": lat, "lon": lon, "radius": int(radius_miles)}
        resp = requests.get(HRSA_API_URL, params=params, headers={"Accept": "*/*"}, timeout=20)
        resp.raise_for_status()
        records = resp.json()

        if not records:
            return pd.DataFrame()

        rows = []
        for rec in records:
            rows.append({
                "name": rec.get("CtrNm", ""),
                "type": "FQHC",
                "city": rec.get("CtrCity", ""),
                "state": rec.get("CtrStateAbbr", ""),
                "address": rec.get("CtrAddress", ""),
                "zip": rec.get("CtrZipCd", ""),
                "phone": rec.get("CtrPhoneNum", ""),
                "lat": rec.get("Latitude"),
                "lon": rec.get("Longitude"),
                "distance_miles": rec.get("Distance"),
                "parent_org": rec.get("ParentCtrNm", ""),
                "website": rec.get("UrlTxt", ""),
                "services": ["Primary Care"],  # FQHCs provide primary care at minimum
                "source": "HRSA",
            })

        df = pd.DataFrame(rows)
        df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
        df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
        df = df.dropna(subset=["lat", "lon"])

        write_cache_df(key, df, tier="weekly")
        return df

    except Exception as e:
        print(f"[WARN] HRSA Health Center API error: {e}")
        return pd.DataFrame()


# ============================================================================
# DATA FETCHING WITH CACHING
# ============================================================================

def get_ihs_facilities_df(refresh: bool = False) -> pd.DataFrame:
    """
    Get IHS facilities as DataFrame with caching.

    Args:
        refresh: Force refresh from source, bypassing cache

    Returns:
        DataFrame with all IHS facility data
    """
    key = cache_key("ihs_facilities", version="v3")

    # Check cache first
    if not refresh and is_fresh(key, tier="monthly"):
        cached = read_cache_df(key)
        if not cached.empty:
            return cached

    # Try web scraping first, fall back to seed data
    df = _scrape_ihs_facilities()

    if df.empty:
        print("IHS scraping unavailable, using seed data")
        df = pd.DataFrame(IHS_FACILITIES_SEED)

    # Tag source
    if 'source' not in df.columns:
        df['source'] = 'IHS'

    # Ensure required columns exist
    required_cols = ['name', 'type', 'city', 'state', 'lat', 'lon']
    for col in required_cols:
        if col not in df.columns:
            df[col] = None

    # Cache the result
    write_cache_df(key, df, tier="monthly")

    return df


def _scrape_ihs_facilities() -> pd.DataFrame:
    """
    Attempt to scrape IHS facilities from the IHS website.
    Returns empty DataFrame if scraping fails.
    """
    try:
        # IHS facility locator search endpoint
        # Note: This may require adjustment based on actual site structure
        base_url = "https://www.ihs.gov/findfacility/"

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }

        response = requests.get(base_url, headers=headers, timeout=30)

        if response.status_code != 200:
            return pd.DataFrame()

        soup = BeautifulSoup(response.text, 'html.parser')

        # Look for facility data - structure depends on actual page
        facilities = []

        # Try to find facility listings
        # This is a template - actual selectors depend on page structure
        facility_elements = soup.find_all(['div', 'tr'], class_=re.compile(r'facility|location|result', re.I))

        for elem in facility_elements:
            try:
                facility = _parse_facility_element(elem)
                if facility:
                    facilities.append(facility)
            except Exception:
                continue

        if facilities:
            return pd.DataFrame(facilities)

        return pd.DataFrame()

    except Exception as e:
        print(f"IHS scraping error: {e}")
        return pd.DataFrame()


def _parse_facility_element(elem) -> Optional[Dict]:
    """Parse a facility element from HTML."""
    try:
        # Extract text content
        text = elem.get_text(separator=' ', strip=True)

        # Look for common patterns
        name_match = re.search(r'^([^,]+(?:Hospital|Center|Clinic|Health))', text, re.I)

        if not name_match:
            return None

        facility = {
            'name': name_match.group(1).strip(),
            'type': 'IHS',  # Default, would need more parsing for accuracy
            'services': []
        }

        # Try to extract address components
        state_match = re.search(r'\b([A-Z]{2})\b\s*\d{5}', text)
        if state_match:
            facility['state'] = state_match.group(1)

        city_match = re.search(r',\s*([A-Za-z\s]+),?\s*[A-Z]{2}', text)
        if city_match:
            facility['city'] = city_match.group(1).strip()

        return facility if facility.get('name') else None

    except Exception:
        return None


def fetch_ihs_by_state(state: str, refresh: bool = False) -> pd.DataFrame:
    """
    Fetch IHS facilities for a specific state with caching.

    Args:
        state: Two-letter state code or full state name
        refresh: Force refresh

    Returns:
        DataFrame of facilities in that state
    """
    state = _normalize_state(state)
    key = cache_key("ihs_state", state=state.upper())

    if not refresh and is_fresh(key, tier="monthly"):
        cached = read_cache_df(key)
        if not cached.empty:
            return cached

    # Get all facilities and filter
    all_facilities = get_ihs_facilities_df(refresh=refresh)
    state_df = all_facilities[all_facilities['state'] == state.upper()].copy()

    # Cache state-specific result
    if not state_df.empty:
        write_cache_df(key, state_df, tier="monthly")

    return state_df


# ============================================================================
# LOCATION-BASED QUERIES
# ============================================================================

def find_ihs_facilities_near(
    lat: float,
    lon: float,
    radius_miles: float = 100,
    facility_types: Optional[List[str]] = None,
    refresh: bool = False,
    include_hrsa: bool = False
) -> pd.DataFrame:
    """
    Find IHS/Tribal/Urban Indian facilities within radius.

    Args:
        lat: Latitude of search center
        lon: Longitude of search center
        radius_miles: Search radius in miles
        facility_types: Filter by type ["IHS", "Tribal", "Urban", "FQHC"] or None for all
        refresh: Force data refresh
        include_hrsa: Also fetch FQHC data from HRSA API

    Returns:
        DataFrame with facilities sorted by distance
    """
    # Validate input coordinates
    if lat is None or lon is None:
        print("[WARN] find_ihs_facilities_near: lat/lon is None, returning empty DataFrame")
        return pd.DataFrame()

    try:
        lat = float(lat)
        lon = float(lon)
    except (TypeError, ValueError):
        print(f"[WARN] find_ihs_facilities_near: Invalid lat/lon values: {lat}, {lon}")
        return pd.DataFrame()

    df = get_ihs_facilities_df(refresh=refresh)

    if df.empty:
        return pd.DataFrame()

    # Ensure lat/lon are numeric
    df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
    df['lon'] = pd.to_numeric(df['lon'], errors='coerce')
    df = df.dropna(subset=['lat', 'lon'])

    # Calculate distances
    df['distance_miles'] = df.apply(
        lambda row: haversine_distance(lat, lon, row['lat'], row['lon']),
        axis=1
    )

    # Filter by radius
    df = df[df['distance_miles'] <= radius_miles].copy()

    # Optionally merge HRSA FQHC data
    if include_hrsa:
        hrsa_df = fetch_hrsa_health_centers(lat, lon, radius_miles, refresh=refresh)
        if not hrsa_df.empty:
            # HRSA already has distance_miles from the API
            df = pd.concat([df, hrsa_df], ignore_index=True)

    # Filter by type if specified
    if facility_types:
        df = df[df['type'].isin(facility_types)]

    # Sort by distance
    df = df.sort_values('distance_miles').reset_index(drop=True)

    return df


def find_nearest_ihs_facility(lat: float, lon: float) -> Optional[Dict]:
    """Find the single nearest IHS/Tribal facility."""
    df = find_ihs_facilities_near(lat, lon, radius_miles=500)
    if df.empty:
        return None
    return df.iloc[0].to_dict()


def get_ihs_facilities_by_state(state: str, refresh: bool = False) -> pd.DataFrame:
    """
    Get all IHS/Tribal/Urban facilities in a state.

    Args:
        state: Two-letter state abbreviation or full state name
        refresh: Force data refresh

    Returns:
        DataFrame with facilities in the state
    """
    df = get_ihs_facilities_df(refresh=refresh)

    if df.empty:
        return pd.DataFrame()

    # Normalize state to 2-letter abbreviation and filter
    state_upper = _normalize_state(state)
    state_df = df[df['state'].str.upper() == state_upper].copy()

    return state_df.reset_index(drop=True)


def get_ihs_services_in_area(lat: float, lon: float, radius_miles: float = 50) -> Dict[str, List[str]]:
    """
    Get all services available from IHS/Tribal facilities in area.

    Returns:
        Dict mapping service type to list of facility names offering it
    """
    df = find_ihs_facilities_near(lat, lon, radius_miles)

    services_map = {}
    for _, row in df.iterrows():
        services = row.get('services', [])
        # Handle various types (list, numpy array, None)
        if services is None:
            services = []
        elif hasattr(services, 'tolist'):
            services = services.tolist()
        elif not isinstance(services, list):
            services = []

        for service in services:
            if service not in services_map:
                services_map[service] = []
            services_map[service].append(f"{row['name']} ({row['distance_miles']:.1f} mi)")

    return services_map


# ============================================================================
# SUMMARY & ANALYSIS FUNCTIONS
# ============================================================================

def get_tribal_health_summary(state: str) -> Dict[str, Any]:
    """
    Get summary statistics for tribal health in a state.

    Args:
        state: State abbreviation (e.g. 'CA') or full name (e.g. 'California')

    Returns dict with:
        - ihs_area: IHS Area Office name
        - total_facilities: Count of IHS/Tribal facilities
        - facility_types: Breakdown by type
        - available_services: List of all services in state
    """
    abbr = _normalize_state(state)
    df = get_ihs_facilities_df()
    state_df = df[df['state'] == abbr]

    all_services = set()
    for services in state_df['services'].dropna():
        if isinstance(services, list):
            all_services.update(services)
        elif hasattr(services, 'tolist'):
            all_services.update(services.tolist())

    return {
        'ihs_area': get_ihs_area(abbr),
        'ihs_area_hq': IHS_AREA_HQ.get(get_ihs_area(abbr), {}),
        'total_facilities': len(state_df),
        'facility_types': state_df['type'].value_counts().to_dict() if not state_df.empty else {},
        'available_services': sorted(list(all_services)),
        'facilities': state_df.to_dict('records')
    }


# Service categories for gap analysis
IHS_SERVICE_CATEGORIES = {
    'primary_care': ['Primary Care', 'Family Medicine', 'Internal Medicine', 'Pediatrics'],
    'emergency': ['Emergency', 'Urgent Care'],
    'hospital': ['Hospital', 'Inpatient'],
    'specialty': ['Specialty Care', 'Cardiology', 'Orthopedics', 'Neurology', 'Oncology'],
    'surgical': ['Surgery', 'General Surgery', 'Outpatient Surgery'],
    'behavioral': ['Behavioral Health', 'Mental Health', 'Psychiatry', 'Substance Abuse', 'SUD Treatment'],
    'dental': ['Dental', 'Oral Health', 'Oral Surgery'],
    'pharmacy': ['Pharmacy'],
    'ancillary': ['Laboratory', 'Radiology', 'Imaging', 'X-Ray', 'CT', 'MRI'],
    'maternal': ['Prenatal Care', 'OB/GYN', 'Labor & Delivery', 'Maternal Health'],
    'rehabilitation': ['Physical Therapy', 'Occupational Therapy', 'Speech Therapy', 'Rehabilitation'],
    'vision_hearing': ['Optometry', 'Ophthalmology', 'Audiology'],
    'traditional': ['Traditional Medicine', 'Traditional Healing', 'Cultural Health'],
    'elder_care': ['Elder Care', 'Geriatrics', 'Home Health'],
    'dialysis': ['Dialysis', 'Hemodialysis', 'Nephrology']
}


def analyze_ihs_service_coverage(lat: float, lon: float, radius_miles: float = 50) -> Dict[str, Any]:
    """
    Analyze what IHS/Tribal services are available vs missing.

    Returns:
        Dict with 'coverage_by_category', 'coverage_score', etc.
    """
    services_map = get_ihs_services_in_area(lat, lon, radius_miles)
    available_services = set(services_map.keys())

    coverage = {}
    total_categories = len(IHS_SERVICE_CATEGORIES)
    covered_categories = 0

    for category, service_list in IHS_SERVICE_CATEGORIES.items():
        category_services = set(service_list)
        found = category_services.intersection(available_services)
        if found:
            coverage[category] = {
                'status': 'available',
                'services': list(found),
                'providers': {s: services_map.get(s, []) for s in found}
            }
            covered_categories += 1
        else:
            coverage[category] = {
                'status': 'missing',
                'services': [],
                'providers': {}
            }

    return {
        'coverage_by_category': coverage,
        'coverage_score': round(covered_categories / total_categories * 100, 1),
        'categories_covered': covered_categories,
        'categories_total': total_categories,
        'all_available_services': sorted(list(available_services))
    }


def get_ihs_national_stats() -> Dict[str, Any]:
    """Get national statistics for IHS facilities."""
    df = get_ihs_facilities_df()

    return {
        'total_facilities': len(df),
        'by_type': df['type'].value_counts().to_dict(),
        'by_state': df['state'].value_counts().to_dict(),
        'by_area': {area: len(df[df['state'].isin(states)])
                   for area, states in IHS_AREAS.items()},
        'states_covered': df['state'].nunique()
    }
