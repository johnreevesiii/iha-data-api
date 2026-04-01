from __future__ import annotations
import pandas as pd
from pathlib import Path

def broadband_county(local_path: str|None, county_fips: str):
    paths = [Path('data/community/broadband_county.csv'), Path('HCA_data/community/broadband_county.csv')]
    if local_path:
        paths.insert(0, Path(local_path))
    for p in paths:
        if p.is_file():
            df = pd.read_csv(p)
            for c in df.columns:
                if c.lower() in ('fips','geoid','county_fips'):
                    return df[df[c].astype(str).str.zfill(5) == str(county_fips).zfill(5)]
            return df
    return pd.DataFrame()
