from __future__ import annotations
from pathlib import Path
import json, hashlib, datetime as dt
from typing import Optional
from fetchers.config import settings

# Data freshness tiers (in hours)
TTL_TIERS = {
    "realtime": 1,      # Hospital utilization, bed availability
    "daily": 24,        # Financial markets, some CMS data
    "weekly": 168,      # FDA updates, some quality metrics
    "monthly": 720,     # Economic indicators, employment data
    "quarterly": 2160,  # Hospital financials (HCRIS), Medicare spending
    "annual": 8760,     # Census data, long-term demographics
    "static": 87600     # Geographic boundaries, never changes
}

def _key_hash(key: str) -> str:
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:24]

def _meta_path(key: str) -> Path:
    return settings.cache_dir / f"{_key_hash(key)}.json"

def _data_path(key: str, ext: str = "parquet") -> Path:
    return settings.cache_dir / f"{_key_hash(key)}.{ext}"

def is_fresh(key: str, ttl_hours: Optional[int] = None, tier: str = "monthly") -> bool:
    """
    Check if cached data is still fresh.
    
    Args:
        key: Cache key
        ttl_hours: Override TTL in hours (takes precedence over tier)
        tier: Freshness tier (realtime, daily, weekly, monthly, quarterly, annual, static)
    """
    # Determine TTL
    if ttl_hours is not None:
        ttl = ttl_hours
    elif tier in TTL_TIERS:
        ttl = TTL_TIERS[tier]
    else:
        ttl = settings.cache_ttl_hours
    
    mp = _meta_path(key)
    if not mp.exists():
        return False
    
    try:
        meta = json.loads(mp.read_text())
        ts = dt.datetime.fromisoformat(meta["fetched_at"])
        age_hours = (dt.datetime.utcnow() - ts).total_seconds() / 3600
        
        # Add debug info to metadata
        if age_hours > ttl:
            print(f"[CACHE] Expired: {key[:40]}... (age: {age_hours:.1f}h, ttl: {ttl}h)")

        return age_hours < ttl
    except Exception as e:
        print(f"[WARN] Cache check error: {e}")
        return False

def write_cache_df(key: str, df, ext: str = "parquet", meta: Optional[dict] = None, tier: str = "monthly"):
    """
    Write DataFrame to cache with tier information.
    
    Args:
        key: Cache key
        df: DataFrame to cache
        ext: File extension (parquet or csv)
        meta: Additional metadata
        tier: Freshness tier for this data
    """
    dp = _data_path(key, ext)
    mp = _meta_path(key)
    
    try:
        # Write data
        if ext == "parquet":
            df.to_parquet(dp, index=False)
        else:
            df.to_csv(dp, index=False)
        
        # Write metadata
        metadata = (meta or {}) | {
            "fetched_at": dt.datetime.utcnow().isoformat(),
            "cache_key": key,
            "ext": ext,
            "tier": tier,
            "ttl_hours": TTL_TIERS.get(tier, settings.cache_ttl_hours),
            "rows": len(df),
            "columns": len(df.columns) if hasattr(df, 'columns') else 0
        }
        mp.write_text(json.dumps(metadata, indent=2))
        
        print(f"[CACHE] Saved: {key[:40]}... ({len(df)} rows, tier: {tier})")
    except Exception as e:
        print(f"[WARN] Cache write error: {str(e)[:80]}")
    
    return df  # Always return the DataFrame

def read_cache_df(key: str):
    """Read DataFrame from cache."""
    import pandas as pd
    
    try:
        mp = _meta_path(key)
        meta = json.loads(mp.read_text())
        dp = _data_path(key, meta.get("ext", "parquet"))
        
        if meta.get("ext") == "parquet":
            df = pd.read_parquet(dp)
        else:
            df = pd.read_csv(dp)
        
        print(f"[CACHE] Hit: {key[:40]}... ({len(df)} rows)")
        return df
    except Exception as e:
        print(f"[WARN] Cache read error: {str(e)[:80]}")
        return pd.DataFrame()

def cache_key(dataset: str, **params) -> str:
    """Generate cache key from dataset name and parameters."""
    bits = [dataset] + [f"{k}={v}" for k, v in sorted(params.items())]
    return "|".join(bits)

def cache_stats() -> dict:
    """Get cache statistics."""
    cache_dir = settings.cache_dir
    
    if not cache_dir.exists():
        return {"total_files": 0, "total_size_mb": 0, "datasets": {}}
    
    json_files = list(cache_dir.glob("*.json"))
    total_size = sum(f.stat().st_size for f in cache_dir.glob("*"))
    
    datasets = {}
    for json_file in json_files:
        try:
            meta = json.loads(json_file.read_text())
            tier = meta.get("tier", "unknown")
            datasets[tier] = datasets.get(tier, 0) + 1
        except:
            pass
    
    return {
        "total_files": len(json_files),
        "total_size_mb": round(total_size / 1024 / 1024, 2),
        "datasets": datasets
    }

def clear_cache(older_than_hours: Optional[int] = None):
    """Clear cache files older than specified hours."""
    cache_dir = settings.cache_dir
    cleared = 0
    
    for json_file in cache_dir.glob("*.json"):
        try:
            meta = json.loads(json_file.read_text())
            ts = dt.datetime.fromisoformat(meta["fetched_at"])
            age_hours = (dt.datetime.utcnow() - ts).total_seconds() / 3600
            
            if older_than_hours is None or age_hours > older_than_hours:
                # Remove data file
                hash_val = json_file.stem
                for ext in ["parquet", "csv"]:
                    data_file = cache_dir / f"{hash_val}.{ext}"
                    if data_file.exists():
                        data_file.unlink()
                
                # Remove metadata
                json_file.unlink()
                cleared += 1
        except:
            pass
    
    return cleared


def clear_dataset_cache(dataset_prefix: str) -> int:
    """Clear file cache entries whose key starts with dataset_prefix.

    Args:
        dataset_prefix: Dataset name prefix (e.g., 'hospital_info').

    Returns:
        Number of cache entries cleared.
    """
    cache_dir = settings.cache_dir
    cleared = 0
    for json_file in cache_dir.glob("*.json"):
        try:
            meta = json.loads(json_file.read_text())
            stored_key = meta.get("cache_key", "")
            if stored_key.startswith(dataset_prefix):
                hash_val = json_file.stem
                for ext in ["parquet", "csv"]:
                    data_file = cache_dir / f"{hash_val}.{ext}"
                    if data_file.exists():
                        data_file.unlink()
                json_file.unlink()
                cleared += 1
        except Exception:
            pass
    return cleared