# tools/utils.py
"""Utility helpers for tools/*.py

Small helpers to keep get_kml.py clean and testable.
"""

from datetime import datetime
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd


def to_unix_ts(iso_str: str) -> float:
    """Convert ISO8601 string to unix timestamp."""
    return datetime.fromisoformat(iso_str.replace("Z", "+00:00")).timestamp()


def calculate_pierce_range(meta_json: dict, factor: float = 1.05) -> float:
    """Calculate dynamic slant range from reference positions with a buffer factor."""
    img_meta = meta_json.get("collect", {}).get("image", {})
    ref_ant = img_meta.get("reference_antenna_position")
    ref_tgt = img_meta.get("reference_target_position")

    if not ref_ant or not ref_tgt:
        return 900000.0  # Fallback to ~900km if missing

    dist = np.linalg.norm(np.array(ref_ant) - np.array(ref_tgt))
    return float(dist * factor)


def discover_parquet_paths(
    parquet_root: Path,
    provider: str,
    *,
    dtype: str | None = None,
) -> list[Path]:
    """
    Discover parquet files for a provider.

    Current behavior:
      - capella + dtype -> ARD parquet:
          parquets/ard/capella/capella_<DTYPE>.parquet

    TODO:
      - iceye ARD layout
      - umbra ARD layout
    """
    provider = provider.lower()

    # --- Capella ARD ---
    if provider == "capella":
        return Path(parquet_root / "ard" / "capella" / f"capella_{dtype}.parquet")

    # --- Future providers ---
    # TODO: implement iceye parquet discovery
    # TODO: implement umbra parquet discovery

    return []


def load_provider_gdf(
    parquet_root: Path,
    provider: str,
    dtype: str,
) -> gpd.GeoDataFrame | None:
    """
    Load provider parquet(s) into a GeoDataFrame.

    Discovery logic is delegated to discover_parquet_paths().
    """
    file = discover_parquet_paths(parquet_root, provider, dtype=dtype)
    return gpd.read_parquet(file)
    # files = discover_parquet_paths(parquet_root, provider, dtype=dtype)
    # if not files:
    #     return None

    # frames: list[pd.DataFrame] = []
    # for fpath in files:
    #     try:
    #         df = pd.read_parquet(fpath)
    #     except Exception:
    #         df = pd.read_parquet(fpath, engine="pyarrow")
    #     frames.append(df)

    # if not frames:
    #     return None

    # df_all = pd.concat(frames, ignore_index=True, copy=False)

    # try:
    #     return gpd.GeoDataFrame(df_all, geometry="geometry", crs="EPSG:4326")
    # except Exception:
    #     return gpd.GeoDataFrame(df_all)


def find_row_by_id(gdf: gpd.GeoDataFrame | None, item_id: str) -> pd.Series | None:
    """Return the row (as Series) matching gdf['id'] == item_id, or None."""
    if gdf is None or "id" not in gdf.columns:
        return None
    matches = gdf[gdf["id"].astype(str) == str(item_id)]
    if matches.empty:
        return None
    # Return the first match (ids should be unique)
    return matches.iloc[0]
