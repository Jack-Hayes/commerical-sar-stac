import argparse
import asyncio
import os
from urllib.parse import urljoin

import aiohttp
import requests
import s3fs

from src.helpers.common import discover_child_links, fetch_json_tolerant

# from src.helpers.processing import item_to_record, records_to_gdf, write_geoparquet
from src.helpers.processing import (
    to_stac_item,
    write_stac_geoparquet,
    densify_item_assets,
)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(BASE_DIR, "parquets")

UMBRA_BUCKET = "umbra-open-data-catalog"
UMBRA_HTTPS_BASE = "https://umbra-open-data-catalog.s3.us-west-2.amazonaws.com/"

CATALOG_URLS = {
    "capella": "https://capella-open-data.s3.us-west-2.amazonaws.com/stac/capella-open-data-by-product-type/catalog.json",
    "iceye": "https://iceye-open-data-catalog.s3-us-west-2.amazonaws.com/collections/iceye-sar.json",
    "umbra": UMBRA_BUCKET,
}


async def _discover_umbra_items_s3(bucket):
    """Discover Umbra STAC items and return canonical public HTTPS URLs (not fs.url(),
    which can hand back presigned/region-less forms that would poison synthesized assets)."""
    fs = s3fs.S3FileSystem(anon=True)
    s3_paths = fs.glob(f"{bucket}/sar-data/**/*.stac.v2.json")
    return [UMBRA_HTTPS_BASE + p.split(f"{bucket}/", 1)[-1] for p in s3_paths]


def _save_items(items, provider):
    out_dir = os.path.join(OUTPUT_DIR, provider)
    os.makedirs(out_dir, exist_ok=True)

    if provider == "capella":
        buckets = {}
        for it in items:
            pt = it.get("properties", {}).get("sar:product_type", "NA")
            buckets.setdefault(pt, []).append(it)
        print(f"\ncapella: {len(buckets)} product types")
        for pt, group in buckets.items():
            path = os.path.join(out_dir, f"capella_{pt}.parquet")
            try:
                write_stac_geoparquet(densify_item_assets(group), path)
                print(
                    f"  capella_{pt}: {len(group)} items, {os.path.getsize(path) / 1024 / 1024:.2f} MB"
                )
            except Exception as e:
                print(f"  capella_{pt}: {e}")
    else:
        path = os.path.join(out_dir, f"{provider}.parquet")
        try:
            write_stac_geoparquet(densify_item_assets(items), path)
            print(
                f"\n{provider.upper()}: {len(items)} items, {os.path.getsize(path) / 1024 / 1024:.2f} MB -> {path}"
            )
        except Exception as e:
            print(f"\n{provider.upper()}: Failed to save - {e}")


async def process_provider(provider, session):
    """Fetch a provider's items and write its parquet(s)."""
    print(f"\n--- Starting provider: {provider.upper()} ---")

    # Discover item URLs
    item_urls = []
    if provider == "umbra":
        item_urls = await _discover_umbra_items_s3(CATALOG_URLS[provider])
    elif provider == "capella":
        for url in discover_child_links(CATALOG_URLS[provider]).values():
            try:
                resp = requests.get(url)
                resp.raise_for_status()
                links = [
                    link
                    for link in resp.json().get("links", [])
                    if link.get("rel") == "item"
                ]
                item_urls.extend(urljoin(url, link["href"]) for link in links)
            except requests.RequestException:
                print(f"Warning: Could not process collection {url}")
    elif provider == "iceye":
        try:
            resp = requests.get(CATALOG_URLS[provider])
            resp.raise_for_status()
            links = [
                link
                for link in resp.json().get("links", [])
                if link.get("rel") == "item"
            ]
            item_urls.extend(link["href"] for link in links)
        except requests.RequestException:
            print(f"Warning: Could not process collection {CATALOG_URLS[provider]}")

    if not item_urls:
        print(f"No items found for {provider}. Skipping.")
        return
    print(f"Found {len(item_urls)} items for {provider}.")

    # Fetch all item JSONs once
    item_jsons = await asyncio.gather(
        *(fetch_json_tolerant(session, u) for u in item_urls)
    )

    # Build valid STAC items (assets/links fixed)
    items = []
    for url, item in zip(item_urls, item_jsons):
        try:
            stac_item = to_stac_item(item, url, provider)
            if stac_item:
                items.append(stac_item)
        except Exception as e:
            print(f"Warning: skipped {(item or {}).get('id')} ({provider}): {e}")

    if not items:
        print(f"No usable items for {provider}. Skipping.")
        return

    # Write stac-geoparquet
    _save_items(items, provider)
    print(f"--- Finished provider: {provider.upper()} ---")


async def main(providers_to_process):
    async with aiohttp.ClientSession() as session:
        await asyncio.gather(
            *(process_provider(p, session) for p in providers_to_process)
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process commercial SAR STAC catalogs."
    )
    parser.add_argument(
        "providers",
        nargs="+",
        choices=["capella", "iceye", "umbra"],
        help="A list of providers to process.",
    )
    args = parser.parse_args()
    asyncio.run(main(args.providers))
