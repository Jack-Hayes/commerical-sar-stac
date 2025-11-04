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
from scripts.helpers.processing_ard import (
    clean_capella_gdf_ard,
    clean_iceye_gdf_ard,
    clean_umbra_gdf_ard,
    process_stac_item_ard,
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


def _save_gdf(gdf, provider, output_subdir):
    """Helper to save GeoDataFrame with provider-specific logic."""
    provider_output_dir = os.path.join(OUTPUT_DIR, output_subdir, provider)
    os.makedirs(provider_output_dir, exist_ok=True)

    if provider == "capella" and "sar:product_type" in gdf.columns:
        product_types = gdf["sar:product_type"].dropna().unique()
        msg_prov = f"\n{provider} ({output_subdir}) found {len(product_types)} product types:"
        print(msg_prov)

        for product_type in product_types:
            subset = gdf[gdf["sar:product_type"] == product_type].copy()
            subset = subset.drop(columns=["sar:product_type"])

            path = os.path.join(provider_output_dir, f"capella_{product_type}.parquet")
            try:
                subset.to_parquet(path, compression="snappy")
                file_size_mb = os.path.getsize(path) / 1024 / 1024
                print(f"  capella_{product_type}: {len(subset)} rows, {file_size_mb:.2f} MB")
            except Exception as e:
                print(f"  capella_{product_type}: {e}")
    else:
        path = os.path.join(provider_output_dir, f"{provider}.parquet")
        try:
            gdf.to_parquet(path, compression="snappy")
            file_size_mb = os.path.getsize(path) / 1024 / 1024
            print(f"\n{provider.upper()} ({output_subdir.upper()}):")
            print(f"  File: {path}")
            print(f"  Size: {file_size_mb:.2f} MB")
            print(f"  Rows: {len(gdf)}")
        except Exception as e:
            print(f"\n{provider.upper()} ({output_subdir.upper()}): Failed to save - {e}")


async def process_provider(provider, session, output_format="both"):
    """Main processing pipeline for a single provider (fetch once, process into both formats)."""
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

    # --- 2. Fetch all JSONs concurrently (once!) ---
    tasks = [fetch_json_tolerant(session, url) for url in item_urls]
    item_jsons = await asyncio.gather(*tasks)

    # --- 3. Process into VIZ format (stac-map) ---
    if output_format in ["both", "viz"]:
        records_viz = []
        for item_url, item_json in zip(item_urls, item_jsons):
            if item_json and item_json.get("geometry"):
                try:
                    properties = item_json.get("properties", {})
                    start_dt, end_dt = extract_datetime_fields(properties)
                    bbox_val = item_json.get("bbox")
                    geom = shape(item_json.get("geometry"))
                    bbox_struct = extract_bbox_struct(geom, bbox_val)

                    flattened_props = flatten_stac_properties(item_json, item_url, provider=provider)

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
                    records_viz.append(record)
                except Exception as e:
                    print(
                        f"Warning: Could not process item {item_json.get('id')} for VIZ. Error: {e}"
                    )

        if records_viz:
            df_viz = pd.DataFrame(records_viz)
            gdf_viz = gpd.GeoDataFrame(df_viz, geometry="geometry", crs="EPSG:4326")

            cleaner_func = {
                "capella": clean_capella_gdf,
                "iceye": clean_iceye_gdf,
                "umbra": clean_umbra_gdf,
            }.get(provider)

            if cleaner_func:
                gdf_viz = cleaner_func(gdf_viz)

            gdf_viz = serialize_complex_columns(gdf_viz)
            _save_gdf(gdf_viz, provider, "viz")

    # --- 4. Process into ARD format (analysis-ready) ---
    if output_format in ["both", "ard"]:
        records_ard = []
        for url, item_json in zip(item_urls, item_jsons):
            record = process_stac_item_ard(item_json, url, provider)
            if record:
                records_ard.append(record)

        if records_ard:
            df_ard = pd.DataFrame(records_ard)
            geometries = [shape(geom) for geom in df_ard["geometry"]]
            gdf_ard = gpd.GeoDataFrame(
                df_ard.drop(columns=["geometry"]), geometry=geometries, crs="EPSG:4326"
            )
            gdf_ard["provider"] = provider

            cleaner_func_ard = {
                "capella": clean_capella_gdf_ard,
                "iceye": clean_iceye_gdf_ard,
                "umbra": clean_umbra_gdf_ard,
            }.get(provider)

            if cleaner_func_ard:
                gdf_ard = cleaner_func_ard(gdf_ard)

            _save_gdf(gdf_ard, provider, "ard")

    print(f"--- Finished provider: {provider.upper()} ---")


async def main(providers_to_process, output_format="both"):
    """Main entry point to run processing for specified providers."""
    async with aiohttp.ClientSession() as session:
        tasks = [
            process_provider(provider, session, output_format) for provider in providers_to_process
        ]
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process commercial SAR STAC catalogs.")
    parser.add_argument(
        "providers",
        nargs="+",
        choices=["capella", "iceye", "umbra"],
        help="A list of providers to process.",
    )
    parser.add_argument(
        "--format",
        choices=["viz", "ard", "both"],
        default="both",
        help="Output format: 'viz' (stac-map), 'ard' (analysis-ready), or 'both'.",
    )
    args = parser.parse_args()

    asyncio.run(main(args.providers, args.format))
