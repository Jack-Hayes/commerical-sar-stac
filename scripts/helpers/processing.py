import json
from typing import Any
from urllib.parse import urljoin

import numpy as np
import pandas as pd
from shapely.geometry import mapping


def normalize_value_for_parquet(value: Any) -> Any:
    """
    Convert any value to be parquet-safe.
    - numpy arrays -> lists
    - numpy scalars -> Python scalars
    - dicts/lists -> as-is (parquet can handle them)
    - None -> None
    """
    if value is None:
        return None

    if isinstance(value, np.ndarray):
        return value.tolist()

    if isinstance(value, (np.integer, np.floating)):
        return value.item()

    if isinstance(value, dict):
        # Recursively normalize dict values
        return {k: normalize_value_for_parquet(v) for k, v in value.items()}

    if isinstance(value, (list, tuple)):
        # Recursively normalize list items
        return [normalize_value_for_parquet(v) for v in value]

    # Strings, ints, floats, bools are fine as-is
    return value


def serialize_mixed_type_columns(gdf):
    """
    Serialize columns with mixed types (list + non-list) to JSON strings.
    This prevents "cannot mix list and non-list" parquet errors.
    """
    gdf = gdf.copy()

    for col in gdf.columns:
        if col in [
            "geometry",
            "bbox",
            "id",
            "provider",
            "datetime",
            "start_datetime",
            "end_datetime",
        ]:
            continue

        non_null_values = gdf[col].dropna()
        if len(non_null_values) == 0:
            continue

        # Check if column has mixed types
        has_list = any(isinstance(v, list) for v in non_null_values)
        has_non_list = any(
            not isinstance(v, list) for v in non_null_values if not isinstance(v, (type(None)))
        )

        if has_list and has_non_list:
            # Mixed types - serialize to JSON
            gdf[col] = gdf[col].apply(lambda x: json.dumps(x) if x is not None else None)

    return gdf


def extract_bbox_struct(geometry, item_bbox=None):
    """
    Extract bbox as a dict with xmin, ymin, xmax, ymax.
    Prefers item_bbox if available, otherwise calculates from geometry.
    """
    if item_bbox and len(item_bbox) >= 4:
        return {
            "xmin": item_bbox[0],
            "ymin": item_bbox[1],
            "xmax": item_bbox[2],
            "ymax": item_bbox[3],
        }

    # Calculate from geometry
    bounds = geometry.bounds  # (minx, miny, maxx, maxy)
    return {
        "xmin": bounds[0],
        "ymin": bounds[1],
        "xmax": bounds[2],
        "ymax": bounds[3],
    }


def extract_datetime_fields(properties):
    """
    Extract start_datetime and end_datetime from STAC properties.
    Falls back to 'datetime' if start/end variants not available.
    """
    start_dt = properties.get("start_datetime") or properties.get("datetime")
    end_dt = properties.get("end_datetime") or start_dt

    # Parse to pd.Timestamp with UTC timezone
    if start_dt:
        start_dt = pd.to_datetime(start_dt, utc=True, format="mixed")
    if end_dt:
        end_dt = pd.to_datetime(end_dt, utc=True, format="mixed")

    return start_dt, end_dt


def resolve_link_href(href: str, base_url: str) -> str:
    """
    Resolve relative link hrefs to absolute URLs.

    Examples:
    - '../../../collections/iceye-sar.json' → full URL
    - 'umbra-sar' -> 'https://umbra-open-data-catalog/umbra-sar'
    - Already absolute URLs stay unchanged
    """
    if href.startswith("http://") or href.startswith("https://"):
        return href  # Already absolute

    if href.startswith("s3://"):
        return href  # S3 URI - leave as-is

    # Relative path - resolve against base URL
    return urljoin(base_url, href)


def resolve_links(links_list, item_url: str):
    """
    Resolve all relative link hrefs to absolute URLs.
    Normalize all values for parquet compatibility.
    """
    if not isinstance(links_list, list):
        return []

    resolved_links = []
    for link in links_list:
        if not isinstance(link, dict):
            continue

        resolved_link = {k: normalize_value_for_parquet(v) for k, v in link.items()}

        # Resolve href if present
        if "href" in resolved_link:
            resolved_link["href"] = resolve_link_href(resolved_link["href"], item_url)

        resolved_links.append(resolved_link)

    return resolved_links


def normalize_umbra_asset_keys(assets_dict: dict) -> dict:
    """
    Transform Umbra's unwieldy machine-generated filenames into human-readable asset keys.

    Umbra uses timestamps + UUIDs + file extensions as keys. This function extracts
    meaningful names based on file type and content.

    Examples:
    - '2025-06-22-23-57-52_UMBRA-10_CSI_MM.tif' → 'CSI_MM'
    - '2025-06-22-23-57-52_UMBRA-10_MM.json' → 'metadata'
    - '2025-06-22-23-57-52_99e75722-4338-484c-a245-1409b6b1b7d1.parquet' → 'raw_data'
    """
    if not isinstance(assets_dict, dict):
        return {}

    normalized = {}

    for key, asset_obj in assets_dict.items():
        if asset_obj is None or not isinstance(asset_obj, dict):
            continue

        # Extract meaningful name from the filename
        new_key = _extract_umbra_asset_name(key)
        normalized[new_key] = asset_obj

    return normalized


def _extract_umbra_asset_name(filename: str) -> str:
    """
    Extract a human-readable name from Umbra's filename-based asset key.

    Strategy:
    1. Remove timestamp prefix (YYYY-MM-DD-HH-MM-SS_)
    2. Remove satellite identifier and redundant parts
    3. Extract product/file type descriptor
    4. Map common patterns to semantic names
    """
    if not isinstance(filename, str):
        return filename

    # Remove timestamp prefix (e.g., "2025-06-22-23-57-52_")
    if "_" in filename:
        parts = filename.split("_", 1)
        if len(parts[0]) == 19 and parts[0].replace("-", "").isdigit():
            # It's a timestamp prefix
            filename = parts[1]

    # Remove satellite identifier (e.g., "UMBRA-10_")
    if filename.startswith("UMBRA-"):
        parts = filename.split("_", 1)
        if len(parts) > 1:
            filename = parts[1]

    # Remove file extension and get the descriptor part
    name_without_ext = filename.rsplit(".", 1)[0] if "." in filename else filename

    # TODO: fix this as it is not best practice
    # common patterns
    if "STAC" in filename:
        return "stac_metadata"
    elif "ANALYSIS" in filename or "ANALYSIS" in name_without_ext:
        return "analysis"
    elif "CSI" in name_without_ext:
        return (
            "csi_" + name_without_ext.split("CSI_")[1].lower()
            if "CSI_" in name_without_ext
            else "csi"
        )
    elif "SICD" in name_without_ext:
        return (
            "sicd_" + name_without_ext.split("SICD_")[1].lower()
            if "SICD_" in name_without_ext
            else "sicd"
        )
    elif "SIDD" in name_without_ext:
        return (
            "sidd_" + name_without_ext.split("SIDD_")[1].lower()
            if "SIDD_" in name_without_ext
            else "sidd"
        )
    elif name_without_ext.endswith("MM") or "_MM" in name_without_ext:
        return "mm_data"
    elif filename.endswith(".json"):
        return "metadata"
    elif filename.endswith(".parquet"):
        return "raw_data"
    elif filename.endswith(".cphd"):
        return "cphd"
    elif filename.endswith(".zip"):
        return "archive"
    elif filename.endswith((".tif", ".tiff")):
        return "geotiff"
    elif filename.endswith((".nitf", ".ntf")):
        return "nitf"
    elif filename.endswith(".xml"):
        return "metadata_xml"

    # Fallback: use cleaned filename
    return name_without_ext.lower().replace("-", "_").replace(" ", "_")[:50]


def compact_assets_dict(assets_dict: dict, provider: str = None) -> dict:
    """
    Compact assets to ONLY store href, type, and roles.
    Normalize asset keys for better UX (especially for Umbra).
    Filters out None values.
    """
    if not isinstance(assets_dict, dict):
        return {}

    # For Umbra, normalize the unwieldy asset keys first
    if provider == "umbra":
        assets_dict = normalize_umbra_asset_keys(assets_dict)

    compacted = {}
    for key, asset_obj in assets_dict.items():
        if asset_obj is None or not isinstance(asset_obj, dict):
            continue

        # Store href + type + roles only (essential for stac-map)
        href = asset_obj.get("href")
        if href:
            compacted[key] = {
                "href": href,
                "type": asset_obj.get("type"),
                "roles": normalize_value_for_parquet(asset_obj.get("roles")),
            }

    return compacted


def flatten_stac_properties(stac_item, item_url: str = "", provider: str = None):
    """
    Flatten STAC item properties for stac-map compatibility.

    Returns a dict with:
    - Flattened properties from the STAC item
    - assets: Compacted assets dict (with provider-specific normalization)
    - links: Resolved links array with absolute URLs
    """
    properties = stac_item.get("properties", {}).copy()

    # Normalize all property values
    properties = normalize_value_for_parquet(properties)

    # Add compacted assets and resolved links as top-level properties
    properties["assets"] = compact_assets_dict(stac_item.get("assets", {}), provider=provider)
    properties["links"] = resolve_links(stac_item.get("links", []), item_url)

    return properties


def add_geometry_geojson(gdf):
    """
    Add a GeoJSON-serialized geometry column for stac-map JavaScript parsing.

    stac-map's query returns the geometry column as WKB binary (geoarrow.wkb extension).
    When JavaScript receives this through DuckDB WASM, it gets a DuckDB struct proxy,
    not a GeoJSON object. Calling .find() on it causes '_a6.find is not a function' error.

    This pre-serializes geometry as valid GeoJSON strings so stac-map can parse them directly.
    Keeps the original geometry column intact for GeoPandas operations.
    """
    gdf = gdf.copy()
    gdf["geometry_geojson"] = gdf.geometry.apply(lambda geom: json.dumps(mapping(geom)))
    return gdf


def serialize_complex_columns(gdf):
    """
    Selectively serialize only problematic columns to JSON strings.

    Strategy:
    - assets: Already compacted in flatten_stac_properties
    - bbox: Keep as dict (stac-map needs it for spatial queries)
    - geometry: Keep as Shapely for GeoPandas
    - geometry_geojson: Keep as string (already serialized for stac-map)
    - links: Already resolved in flatten_stac_properties
    - start_datetime, end_datetime: Keep as pd.Timestamp for TIMESTAMP type in Parquet
    - Other complex columns with mixed types: Serialize to avoid parquet issues
    """
    gdf = gdf.copy()

    # Columns to NEVER serialize (keep as structured or native types)
    never_serialize = {
        "geometry",
        "bbox",
        "links",
        "assets",
        "geometry_geojson",
        "start_datetime",
        "end_datetime",
    }

    for col in gdf.columns:
        if col in never_serialize:
            continue

        # Check if column contains complex types
        non_null_values = gdf[col].dropna()
        if len(non_null_values) == 0:
            continue

        has_complex = any(isinstance(v, (list, dict)) for v in non_null_values)

        # Serialize if contains complex types
        if has_complex:
            gdf[col] = gdf[col].apply(
                lambda x: json.dumps(normalize_value_for_parquet(x))
                if isinstance(x, (list, dict))
                else x
            )

    # Handle mixed-type columns (like 'providers' in Umbra)
    gdf = serialize_mixed_type_columns(gdf)

    return gdf


def clean_capella_gdf(gdf):
    """Applies cleaning rules specific to Capella GeoDataFrames."""
    print("Applying cleaning for Capella (2D geometry + geometry_geojson).")
    gdf = gdf.copy()
    gdf.geometry = gdf.geometry.force_2d()
    gdf = add_geometry_geojson(gdf)
    return gdf


def clean_iceye_gdf(gdf):
    """Applies cleaning rules specific to ICEYE GeoDataFrames."""
    print("Applying cleaning for ICEYE (2D geometry + geometry_geojson).")
    gdf = gdf.copy()
    gdf.geometry = gdf.geometry.force_2d()
    gdf = add_geometry_geojson(gdf)
    return gdf


def clean_umbra_gdf(gdf):
    """Applies cleaning rules specific to Umbra GeoDataFrames."""
    print("Applying cleaning for Umbra (2D geometry + geometry_geojson).")
    gdf = gdf.copy()
    gdf.geometry = gdf.geometry.force_2d()
    gdf = add_geometry_geojson(gdf)
    return gdf
