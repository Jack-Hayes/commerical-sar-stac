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

    # --- 3. Pre-process into records ---
    records = []
    for url, item_json in zip(item_urls, item_jsons):
        if item_json and item_json.get("geometry"):
            record = item_json.get("properties", {})
            record["geometry"] = item_json.get("geometry")  # Keep as dict for now
            record["id"] = item_json.get("id")
            record["stac_item_url"] = url

            assets = item_json.get("assets", {})
            if isinstance(assets, dict):
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
            records.append(record)

    if not records:
        print(f"No valid records processed for {provider}. Skipping.")
        return

    # --- 4. Create GeoDataFrame using the robust multi-step pattern ---
    # a) Create a standard DataFrame first.
    df = pd.DataFrame(records)

    # b) Manually create the GeoSeries from the dictionary column.
    geometries = [shape(geom) for geom in df["geometry"]]

    # c) Assemble the final, valid GeoDataFrame.
    gdf = gpd.GeoDataFrame(df.drop(columns=["geometry"]), geometry=geometries, crs="EPSG:4326")

    gdf["provider"] = provider

    # --- 5. Clean and Save ---
    cleaner_func = {
        "capella": clean_capella_gdf,
        "iceye": clean_iceye_gdf,
        "umbra": clean_umbra_gdf,
    }.get(provider)

    if cleaner_func:
        gdf = cleaner_func(gdf)

    provider_output_dir = os.path.join(OUTPUT_DIR, provider)
    os.makedirs(provider_output_dir, exist_ok=True)

    if provider == "capella":
        for product_type, group in gdf.groupby("sar:product_type"):
            path = os.path.join(provider_output_dir, f"capella_{product_type}.parquet")
            print(f"Saving {len(group)} items to {path}...")
            group.to_parquet(path)
    else:
        path = os.path.join(provider_output_dir, f"{provider}.parquet")
        print(f"Saving {len(gdf)} items to {path}...")
        gdf.to_parquet(path)


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
