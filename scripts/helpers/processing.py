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
            not isinstance(v, list) for v in non_null_values if not isinstance(v, type(None))
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


def fix_umbra_asset_hrefs(assets_dict: dict, item_url: str) -> dict:
    """
    Replace private S3 hrefs with public URLs for Umbra assets.

    Umbra's STAC metadata points to a private bucket (prod-prod-processed-sar-data),
    but the public bucket (umbra-open-data-catalog) contains the actual data files
    with slightly different naming (no _MM suffix).

    Only returns assets that have valid public URLs.

    Args:
        assets_dict: Original assets dict with private s3:// hrefs
        item_url: The public STAC item URL (used to construct public asset URLs)

    Returns:
        Assets dict with corrected public HTTPS URLs (only includes assets with valid hrefs)
    """
    if not isinstance(assets_dict, dict):
        return {}

    base_url = item_url.rsplit("/", 1)[0] + "/"
    pattern = item_url.split("/")[-1].replace(".stac.v2.json", "")

    # Mapping of asset title patterns to public file suffixes
    # Order matters! Check more specific patterns first
    title_to_suffix = {
        "CSI-SIDD": "_CSI-SIDD.nitf",
        "CSI_SIDD": "_CSI-SIDD.nitf",
        "CSI_TIFF": "_CSI.tif",
        "SICD_XML": "_SICD.nitf",
        "SICD": "_SICD.nitf",
        "SIDD": "_SIDD.nitf",
        "CPHD": "_CPHD.cphd",
        "TIFF": "_GEC.tif",
    }

    fixed_assets = {}
    for key, asset_obj in assets_dict.items():
        if not isinstance(asset_obj, dict):
            continue

        title = asset_obj.get("title", "")

        # Try to find matching public file
        public_href = None
        for title_pattern, suffix in title_to_suffix.items():
            if title_pattern in title:
                public_href = f"{base_url}{pattern}{suffix}"
                break

        # Only include assets with valid public hrefs
        if public_href:
            fixed_assets[key] = {
                "href": public_href,
                "type": asset_obj.get("type"),
                "roles": normalize_value_for_parquet(asset_obj.get("roles")),
                "title": title,
            }

    return fixed_assets


def fix_umbra_links(links_list: list, item_url: str) -> list:
    """
    Fix Umbra STAC links to point to valid public endpoints.

    Umbra items only have 'collection' links which don't resolve to valid endpoints.
    This function filters those out and adds a 'self' link pointing to the item.

    Args:
        links_list: Original links from STAC item
        item_url: The public STAC item URL

    Returns:
        Fixed links list with 'self' link added and broken links removed
    """
    if not isinstance(links_list, list):
        links_list = []

    fixed_links = []

    # Process existing links
    for link in links_list:
        if not isinstance(link, dict):
            continue

        rel = link.get("rel")

        # Skip broken collection/parent links
        if rel in ["collection", "parent"]:
            continue

        # Update self link if present
        if rel == "self":
            fixed_links.append(
                {
                    "rel": "self",
                    "href": item_url,
                    "type": link.get("type", "application/geo+json"),
                }
            )
            continue

        # Keep any other valid links
        fixed_link = {k: normalize_value_for_parquet(v) for k, v in link.items()}
        fixed_links.append(fixed_link)

    # Always ensure we have a self link (add if not present)
    has_self = any(link.get("rel") == "self" for link in fixed_links)
    if not has_self:
        fixed_links.append(
            {
                "rel": "self",
                "href": item_url,
                "type": "application/geo+json",
            }
        )

    return fixed_links


def resolve_links(links_list, item_url: str, provider: str = None):
    """
    Resolve all relative link hrefs to absolute URLs.
    Normalize all values for parquet compatibility.
    Apply provider-specific fixes for broken links.

    Args:
        links_list: List of link objects from STAC item
        item_url: The STAC item URL (for resolving relative paths)
        provider: Provider name ('umbra', 'capella', 'iceye')

    Returns:
        List of resolved and normalized link objects
    """
    if not isinstance(links_list, list):
        return []

    # Apply provider-specific link fixes
    if provider == "umbra":
        return fix_umbra_links(links_list, item_url)

    # Default behavior for other providers
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
    2. Remove satellite identifier (UMBRA-XX_)
    3. Map file type patterns to semantic names
    """
    if not isinstance(filename, str):
        return filename

    # Remove timestamp and satellite identifier prefix
    # Format: "2025-06-22-23-57-52_UMBRA-10_..."
    # Since satellite ID (e.g. Umbra-10) always follows timestamp, we can remove both at once
    if filename.startswith(("20", "19")) and filename.count("_") >= 2:
        # Skip timestamp_satellite_ prefix
        parts = filename.split("_", 2)
        if len(parts) >= 3:
            filename = parts[2]

    # Remove file extension
    name_without_ext = filename.rsplit(".", 1)[0] if "." in filename else filename

    # Map common patterns
    if "STAC" in filename:
        return "stac_metadata"
    if "ANALYSIS" in name_without_ext:
        return "analysis"
    if "CSI" in name_without_ext:
        suffix = name_without_ext.split("CSI_")[1].lower() if "CSI_" in name_without_ext else ""
        return f"csi_{suffix}" if suffix else "csi"
    if "SICD" in name_without_ext:
        suffix = name_without_ext.split("SICD_")[1].lower() if "SICD_" in name_without_ext else ""
        return f"sicd_{suffix}" if suffix else "sicd"
    if "SIDD" in name_without_ext:
        suffix = name_without_ext.split("SIDD_")[1].lower() if "SIDD_" in name_without_ext else ""
        return f"sidd_{suffix}" if suffix else "sidd"
    if name_without_ext.endswith("MM") or "_MM" in name_without_ext:
        return "mm_data"
    if filename.endswith(".json"):
        return "metadata"
    if filename.endswith(".parquet"):
        return "raw_data"
    if filename.endswith(".cphd"):
        return "cphd"
    if filename.endswith(".zip"):
        return "archive"
    if filename.endswith((".tif", ".tiff")):
        return "geotiff"
    if filename.endswith((".nitf", ".ntf")):
        return "nitf"
    if filename.endswith(".xml"):
        return "metadata_xml"

    # Fallback: use cleaned filename
    return name_without_ext.lower().replace("-", "_").replace(" ", "_")[:50]


def compact_assets_dict(assets_dict: dict, item_url: str = "", provider: str = None) -> dict:
    """
    Compact assets to ONLY store href, type, and roles.
    Normalize asset keys for better UX (especially for Umbra).
    Apply provider-specific fixes for broken hrefs.
    Filters out None values.

    Args:
        assets_dict: Original assets dict from STAC item
        item_url: The STAC item URL (for constructing public URLs for Umbra)
        provider: Provider name ('umbra', 'capella', 'iceye')

    Returns:
        Compacted assets dict with only essential fields
    """
    if not isinstance(assets_dict, dict):
        return {}

    # Apply provider-specific asset fixes
    if provider == "umbra":
        # Umbra needs special handling to fix private S3 hrefs
        assets_dict = fix_umbra_asset_hrefs(assets_dict, item_url)
        # Also normalize the unwieldy asset keys
        assets_dict = normalize_umbra_asset_keys(assets_dict)
        # Assets are already compacted by fix_umbra_asset_hrefs
        return assets_dict

    # Default behavior for other providers
    if provider in ["capella", "iceye"]:
        # Normalize Umbra keys only (no-op for other providers)
        pass

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
    - assets: Compacted assets dict (with provider-specific normalization and fixes)
    - links: Resolved links array with absolute URLs (with provider-specific fixes)
    """
    properties = stac_item.get("properties", {}).copy()

    # Normalize all property values
    properties = normalize_value_for_parquet(properties)

    # Add compacted assets and resolved links as top-level properties
    properties["assets"] = compact_assets_dict(
        stac_item.get("assets", {}), item_url=item_url, provider=provider
    )
    properties["links"] = resolve_links(stac_item.get("links", []), item_url, provider=provider)

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
