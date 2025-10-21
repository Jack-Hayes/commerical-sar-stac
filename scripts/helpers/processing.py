import json

import numpy as np


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
    return start_dt, end_dt


def flatten_stac_properties(stac_item):
    """
    Flatten STAC item properties for stac-map compatibility.

    Returns a dict with:
    - Flattened properties from the STAC item
    - assets: The assets object (for stac-map AssetsSection)
    - links: The links array (for stac-map LinksSection)
    """
    properties = stac_item.get("properties", {}).copy()

    # Add assets and links as top-level properties for stac-map
    properties["assets"] = stac_item.get("assets", {})
    properties["links"] = stac_item.get("links", [])

    return properties


def clean_numpy_arrays(obj):
    """
    Recursively convert numpy arrays to Python lists and numpy types to native Python types.
    This prevents bloated Parquet files with unnecessary numpy wrappers.
    """
    if isinstance(obj, dict):
        return {k: clean_numpy_arrays(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [clean_numpy_arrays(item) for item in obj]
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, (np.integer, np.floating)):
        return obj.item()
    elif isinstance(obj, np.bool_):
        return bool(obj)
    else:
        return obj


def compact_assets(assets_dict):
    """
    Compact assets by:
    1. Removing None values (Umbra has 23k keys, only ~10 are non-None)
    2. Converting numpy arrays to lists
    3. Returning as JSON string for efficient storage
    """
    if not assets_dict:
        return "{}"

    # Filter out None values
    compacted = {k: v for k, v in assets_dict.items() if v is not None}

    # Clean numpy arrays
    compacted = clean_numpy_arrays(compacted)

    # Return as JSON string
    return json.dumps(compacted)


def serialize_complex_columns(gdf):
    """
    Selectively serialize only problematic columns to JSON strings.

    Strategy:
    - assets: Serialize to JSON (remove None values, convert numpy arrays)
    - links: Keep as list (stac-map needs array)
    - Other complex columns with mixed types: Serialize
    - Everything else: Keep as-is
    """
    gdf = gdf.copy()

    # Columns to NEVER serialize (keep as structured)
    never_serialize = {"geometry", "bbox", "links"}

    # Columns to ALWAYS serialize
    always_serialize = {"assets"}

    for col in gdf.columns:
        if col in never_serialize:
            continue

        if col in always_serialize:
            # Special handling for assets
            if col == "assets":
                gdf[col] = gdf[col].apply(compact_assets)
            continue

        # Check if column contains complex types
        non_null_values = gdf[col].dropna()
        if len(non_null_values) == 0:
            continue

        has_complex = any(isinstance(v, (list, dict)) for v in non_null_values)

        # Serialize if contains complex types
        if has_complex:
            gdf[col] = gdf[col].apply(
                lambda x: json.dumps(clean_numpy_arrays(x)) if isinstance(x, (list, dict)) else x
            )

    return gdf


def clean_capella_gdf(gdf):
    """Applies cleaning rules specific to Capella GeoDataFrames."""
    print("Applying cleaning for Capella (2D geometry).")
    gdf = gdf.copy()
    gdf.geometry = gdf.geometry.force_2d()
    return gdf


def clean_iceye_gdf(gdf):
    """Applies cleaning rules specific to ICEYE GeoDataFrames."""
    print("Applying cleaning for ICEYE (2D geometry).")
    gdf = gdf.copy()
    gdf.geometry = gdf.geometry.force_2d()
    return gdf


def clean_umbra_gdf(gdf):
    """Applies cleaning rules specific to Umbra GeoDataFrames."""
    print("Applying cleaning for Umbra (2D geometry).")
    gdf = gdf.copy()
    gdf.geometry = gdf.geometry.force_2d()
    return gdf
