import argparse
import asyncio
import os
from urllib.parse import urljoin

import aiohttp
import geopandas as gpd
import pandas as pd
import requests
import s3fs
from shapely.geometry import shape

from scripts.helpers.common import discover_child_links, fetch_json_tolerant
from scripts.helpers.processing import (
    clean_capella_gdf,
    clean_iceye_gdf,
    clean_umbra_gdf,
    extract_bbox_struct,
    extract_datetime_fields,
    flatten_stac_properties,
    serialize_complex_columns,
)

# --- Constants ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(BASE_DIR, "parquets")

CATALOG_URLS = {
    "capella": "https://capella-open-data.s3.us-west-2.amazonaws.com/stac/capella-open-data-by-product-type/catalog.json",
    "iceye": "https://iceye-open-data-catalog.s3-us-west-2.amazonaws.com/collections/iceye-sar.json",
    "umbra": "umbra-open-data-catalog",
}


async def _discover_umbra_items_s3(bucket):
    """Uses s3fs to discover all Umbra STAC item files."""
    fs = s3fs.S3FileSystem(anon=True)
    s3_paths = fs.glob(f"{bucket}/sar-data/**/*.stac.v2.json")
    return [fs.url(path) for path in s3_paths]


async def process_provider(provider, session):
    """Main processing pipeline for a single provider."""
    print(f"\n--- Starting provider: {provider.upper()} ---")

    # --- 1. Discover Item URLs ---
    item_urls = []
    if provider == "umbra":
        item_urls = await _discover_umbra_items_s3(CATALOG_URLS[provider])
    elif provider == "capella":
        collection_urls = discover_child_links(CATALOG_URLS[provider]).values()
        for url in collection_urls:
            try:
                resp = requests.get(url)
                resp.raise_for_status()
                links = [link for link in resp.json().get("links", []) if link.get("rel") == "item"]
                item_urls.extend([urljoin(url, link["href"]) for link in links])
            except requests.RequestException:
                print(f"Warning: Could not process collection {url}")
    elif provider == "iceye":
        try:
            resp = requests.get(CATALOG_URLS[provider])
            resp.raise_for_status()
            links = [link for link in resp.json().get("links", []) if link.get("rel") == "item"]
            item_urls.extend([link["href"] for link in links])
        except requests.RequestException:
            print(f"Warning: Could not process collection {CATALOG_URLS[provider]}")

    if not item_urls:
        print(f"No items found for {provider}. Skipping.")
        return
    print(f"Found {len(item_urls)} items for {provider}.")

    # --- 2. Fetch all JSONs concurrently ---
    tasks = [fetch_json_tolerant(session, url) for url in item_urls]
    item_jsons = await asyncio.gather(*tasks)

    # --- 3. Pre-process into records with stac-map schema ---
    records = []
    for item_url, item_json in zip(item_urls, item_jsons):
        if item_json and item_json.get("geometry"):
            try:
                properties = item_json.get("properties", {})
                start_dt, end_dt = extract_datetime_fields(properties)
                bbox_val = item_json.get("bbox")
                geom = shape(item_json.get("geometry"))
                bbox_struct = extract_bbox_struct(geom, bbox_val)

                # Flatten STAC properties (includes compacted assets + resolved links)
                flattened_props = flatten_stac_properties(item_json, item_url, provider=provider)

                # Remove the string datetime fields from flattened_props
                # so our parsed pd.Timestamp objects take precedence
                flattened_props.pop("start_datetime", None)
                flattened_props.pop("end_datetime", None)
                flattened_props.pop("datetime", None)

                record = {
                    "id": item_json.get("id"),
                    "geometry": geom,
                    "bbox": bbox_struct,
                    "start_datetime": start_dt,
                    "end_datetime": end_dt,
                    "provider": provider,
                    **flattened_props,
                }
                records.append(record)
            except Exception as e:
                print(f"Warning: Could not process item {item_json.get('id')}. Error: {e}")

    if not records:
        print(f"No valid records processed for {provider}. Skipping.")
        return

    # --- 4. Create GeoDataFrame ---
    df = pd.DataFrame(records)
    gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")

    # --- 5. Clean (includes 2D geometry + geometry_geojson for stac-map) ---
    cleaner_func = {
        "capella": clean_capella_gdf,
        "iceye": clean_iceye_gdf,
        "umbra": clean_umbra_gdf,
    }.get(provider)

    if cleaner_func:
        gdf = cleaner_func(gdf)

    # Serialize complex columns (handles mixed types, assets, links, etc.)
    gdf = serialize_complex_columns(gdf)

    # --- 6. Save ---
    provider_output_dir = os.path.join(OUTPUT_DIR, provider)
    os.makedirs(provider_output_dir, exist_ok=True)

    if provider == "capella":
        # Split Capella by sar:product_type
        if "sar:product_type" in gdf.columns:
            product_types = gdf["sar:product_type"].dropna().unique()
            print(f"\n{provider.upper()} found {len(product_types)} product types:")

            for product_type in product_types:
                subset = gdf[gdf["sar:product_type"] == product_type].copy()
                subset = subset.drop(columns=["sar:product_type"])

                path = os.path.join(provider_output_dir, f"capella_{product_type}.parquet")
                try:
                    subset.to_parquet(path, compression="snappy")
                    file_size_mb = os.path.getsize(path) / 1024 / 1024
                    print(f"capella_{product_type}: {len(subset)} rows, {file_size_mb:.2f} MB")
                except Exception as e:
                    print(f"capella_{product_type}: {e}")
        else:
            # Fallback if no product_type
            path = os.path.join(provider_output_dir, f"{provider}.parquet")
            gdf.to_parquet(path, compression="snappy")
            print(f"{provider.upper()}: {len(gdf)} rows")
    else:
        # ICEYE and Umbra - save as single file
        path = os.path.join(provider_output_dir, f"{provider}.parquet")
        try:
            gdf.to_parquet(path, compression="snappy")
            file_size_mb = os.path.getsize(path) / 1024 / 1024
            print(f"\n{provider.upper()}:")
            print(f"  File: {path}")
            print(f"  Size: {file_size_mb:.2f} MB")
            print(f"  Rows: {len(gdf)}")
        except Exception as e:
            print(f"\n{provider.upper()}: Failed to save")
            print(f"  Error: {e}")

    print(f"--- Finished provider: {provider.upper()} ---")


async def main(providers_to_process):
    """Main entry point to run processing for specified providers."""
    async with aiohttp.ClientSession() as session:
        tasks = [process_provider(provider, session) for provider in providers_to_process]
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process commercial SAR STAC catalogs.")
    parser.add_argument(
        "providers",
        nargs="+",
        choices=["capella", "iceye", "umbra"],
        help="A list of providers to process.",
    )
    args = parser.parse_args()

    asyncio.run(main(args.providers))
