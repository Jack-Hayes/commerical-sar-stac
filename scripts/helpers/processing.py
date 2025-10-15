

def process_stac_item(item_json, url, provider):
    """Generic pre-processing for a raw STAC item JSON."""
    if not item_json or not item_json.get("geometry"):
        return None

    record = item_json.get("properties", {})
    record["geometry"] = item_json.get("geometry")
    record["id"] = item_json.get("id")
    record["stac_item_url"] = url

    assets = item_json.get("assets", {})
    if not isinstance(assets, dict):
        return record

    if provider == "umbra":
        for asset_key, asset_data in assets.items():
            title = asset_data.get("title")
            if title:
                col_name = f"asset_{title.replace('-', '_').replace('.', '_').lower()}"
                record[col_name] = url.replace(url.split("/")[-1], asset_key)
    else:
        for key, asset_data in assets.items():
            col_name = f"asset_{key.replace('-', '_')}"
            record[col_name] = asset_data.get("href")

    return record


def _sanitize_for_parquet(gdf):
    """
    A robust helper function to ensure all columns are Parquet-compatible.
    It converts any column containing lists or dicts into strings.
    """
    for col in gdf.columns:
        if gdf[col].dtype == "object" and col != "geometry":
            # Check if any non-null cell in the column is a list or dict
            is_complex = gdf[col].dropna().apply(lambda x: isinstance(x, (list, dict))).any()
            if is_complex:
                print(f"    - Normalizing mixed-type column '{col}' to string.")
                # Convert entire column to string to ensure type consistency
                gdf[col] = gdf[col].astype(str)
    return gdf


def clean_capella_gdf(gdf):
    """Applies cleaning rules specific to Capella GeoDataFrames."""
    gdf = gdf.copy()
    if "proj:centroid" in gdf.columns:
        gdf = gdf.drop(columns=["proj:centroid"])
    if "proj:shape" in gdf.columns:
        gdf["rows"] = gdf["proj:shape"].apply(
            lambda x: x[0] if isinstance(x, list) and len(x) > 0 else None
        )
        gdf["cols"] = gdf["proj:shape"].apply(
            lambda x: x[1] if isinstance(x, list) and len(x) > 1 else None
        )
        gdf = gdf.drop(columns=["proj:shape"])

    gdf = _sanitize_for_parquet(gdf)
    return gdf


def clean_iceye_gdf(gdf):
    """Applies cleaning rules specific to ICEYE GeoDataFrames."""
    gdf = gdf.copy()
    cols_to_drop = ["proj:centroid", "raster:bands"]
    existing_cols = [col for col in cols_to_drop if col in gdf.columns]
    if existing_cols:
        gdf = gdf.drop(columns=existing_cols)

    if "proj:shape" in gdf.columns:
        gdf["rows"] = gdf["proj:shape"].apply(
            lambda x: x[0] if isinstance(x, list) and len(x) > 0 else None
        )
        gdf["cols"] = gdf["proj:shape"].apply(
            lambda x: x[1] if isinstance(x, list) and len(x) > 1 else None
        )
        gdf = gdf.drop(columns=["proj:shape"])

    if "processing:software" in gdf.columns:
        gdf["processing:software"] = gdf["processing:software"].apply(
            lambda x: x.get("processor") if isinstance(x, dict) else x
        )
        gdf = _sanitize_for_parquet(gdf)
    return gdf


def clean_umbra_gdf(gdf):
    """Applies cleaning rules specific to Umbra GeoDataFrames."""
    gdf = gdf.copy()
    gdf.geometry = gdf.geometry.force_2d()
    gdf = _sanitize_for_parquet(gdf)
    return gdf
