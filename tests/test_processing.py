"""
Tests for STAC item processing logic.

Validates that our processing functions correctly handle real STAC items
from each provider, catching any breaking changes in field definitions.
"""

import pytest
import requests
from shapely.geometry import shape

from scripts.helpers.processing import (
    compact_assets_dict,
    extract_bbox_struct,
    extract_datetime_fields,
    fix_umbra_asset_hrefs,
    fix_umbra_links,
    flatten_stac_properties,
    normalize_umbra_asset_keys,
)

# Stable, known-good item URLs for each provider
SAMPLE_ITEMS = {
    "capella": "https://capella-open-data.s3.us-west-2.amazonaws.com/stac/capella-open-data-by-datetime/capella-open-data-2025/capella-open-data-2025-08/capella-open-data-2025-08-26/CAPELLA_C13_SP_GEO_HH_20250826023518_20250826023527/CAPELLA_C13_SP_GEO_HH_20250826023518_20250826023527.json",
    "iceye": "https://iceye-open-data-catalog.s3.amazonaws.com/stac-items/2025/09/ICEYE_ETGCZ1_20250930T115843Z_6360071_X35_SLEDF.json",
    "umbra": "https://umbra-open-data-catalog.s3.amazonaws.com/sar-data/tasks/3e56976b-fa2b-4035-bb71-385efde84c4a/2025-06-22-23-57-52_UMBRA-10/2025-06-22-23-57-52_UMBRA-10.stac.v2.json",
}


@pytest.fixture
def sample_items():
    """Fetch sample STAC items for testing."""
    items = {}
    for provider, url in SAMPLE_ITEMS.items():
        try:
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            items[provider] = (response.json(), url)
        except requests.RequestException as e:
            pytest.skip(f"Could not fetch {provider} sample item: {e}")
    return items


def test_extract_datetime_fields(sample_items):
    """Verify datetime extraction works for all providers."""
    for provider, (item, _) in sample_items.items():
        properties = item.get("properties", {})
        start_dt, end_dt = extract_datetime_fields(properties)

        assert start_dt is not None, f"{provider}: start_datetime is None"
        assert end_dt is not None, f"{provider}: end_datetime is None"
        assert hasattr(start_dt, "isoformat"), f"{provider}: start_dt is not a datetime"
        assert hasattr(end_dt, "isoformat"), f"{provider}: end_dt is not a datetime"


def test_extract_bbox_struct(sample_items):
    """Verify bbox extraction creates valid structure."""
    for provider, (item, _) in sample_items.items():
        geometry = shape(item["geometry"])
        bbox = extract_bbox_struct(geometry, item.get("bbox"))

        assert isinstance(bbox, dict), f"{provider}: bbox is not a dict"
        assert "xmin" in bbox, f"{provider}: missing xmin"
        assert "ymin" in bbox, f"{provider}: missing ymin"
        assert "xmax" in bbox, f"{provider}: missing xmax"
        assert "ymax" in bbox, f"{provider}: missing ymax"
        assert bbox["xmin"] < bbox["xmax"], f"{provider}: invalid bbox bounds"
        assert bbox["ymin"] < bbox["ymax"], f"{provider}: invalid bbox bounds"


def test_compact_assets_dict_capella(sample_items):
    """Verify Capella assets are compacted correctly."""
    item, url = sample_items["capella"]
    assets = compact_assets_dict(item.get("assets", {}), url, provider="capella")

    assert isinstance(assets, dict), "Compacted assets is not a dict"
    assert len(assets) > 0, "No assets returned"

    # Check first asset has required fields
    first_asset = next(iter(assets.values()))
    assert "href" in first_asset, "Asset missing href"
    assert "type" in first_asset, "Asset missing type"
    assert first_asset["href"].startswith("http"), "Asset href is not a URL"


def test_compact_assets_dict_iceye(sample_items):
    """Verify ICEYE assets are compacted correctly."""
    item, url = sample_items["iceye"]
    assets = compact_assets_dict(item.get("assets", {}), url, provider="iceye")

    assert isinstance(assets, dict), "Compacted assets is not a dict"
    assert len(assets) > 0, "No assets returned"

    first_asset = next(iter(assets.values()))
    assert "href" in first_asset, "Asset missing href"
    assert "type" in first_asset, "Asset missing type"
    assert first_asset["href"].startswith("http"), "Asset href is not a URL"


def test_fix_umbra_asset_hrefs(sample_items):
    """Verify Umbra asset hrefs are fixed to public URLs."""
    item, url = sample_items["umbra"]
    fixed_assets = fix_umbra_asset_hrefs(item.get("assets", {}), url)

    assert isinstance(fixed_assets, dict), "Fixed assets is not a dict"
    assert len(fixed_assets) > 0, "No assets with public hrefs"

    # All returned assets should have hrefs
    for key, asset in fixed_assets.items():
        assert "href" in asset, f"Asset {key} missing href"
        assert asset["href"].startswith("https://umbra-open-data-catalog"), (
            f"Asset {key} href not pointing to public bucket"
        )
        assert "type" in asset, f"Asset {key} missing type"
        assert "title" in asset, f"Asset {key} missing title"


def test_normalize_umbra_asset_keys(sample_items):
    """Verify Umbra asset keys are normalized to readable names."""
    item, url = sample_items["umbra"]
    fixed_assets = fix_umbra_asset_hrefs(item.get("assets", {}), url)
    normalized = normalize_umbra_asset_keys(fixed_assets)

    assert isinstance(normalized, dict), "Normalized assets is not a dict"

    # Check that keys are human-readable (not timestamp-based)
    for key in normalized.keys():
        assert not key.startswith("20"), f"Key {key} still has timestamp prefix"
        assert "UMBRA-" not in key, f"Key {key} still has satellite identifier"


def test_fix_umbra_links(sample_items):
    """Verify Umbra links are fixed to include self link."""
    item, url = sample_items["umbra"]
    fixed_links = fix_umbra_links(item.get("links", []), url)

    assert isinstance(fixed_links, list), "Fixed links is not a list"
    assert len(fixed_links) > 0, "No links returned"

    # Should have a self link
    self_links = [link for link in fixed_links if link.get("rel") == "self"]
    assert len(self_links) == 1, "Should have exactly one self link"
    assert self_links[0]["href"] == url, "Self link href doesn't match item URL"

    # Should not have broken collection links
    collection_links = [link for link in fixed_links if link.get("rel") == "collection"]
    assert len(collection_links) == 0, "Should filter out broken collection links"


def test_flatten_stac_properties_all_providers(sample_items):
    """Verify flattened properties have all required fields for stac-map."""
    for provider, (item, url) in sample_items.items():
        flattened = flatten_stac_properties(item, url, provider=provider)

        assert isinstance(flattened, dict), f"{provider}: flattened props is not a dict"
        assert "assets" in flattened, f"{provider}: missing assets"
        assert "links" in flattened, f"{provider}: missing links"
        assert isinstance(flattened["assets"], dict), f"{provider}: assets is not a dict"
        assert isinstance(flattened["links"], list), f"{provider}: links is not a list"
        assert len(flattened["assets"]) > 0, f"{provider}: no assets"
        assert len(flattened["links"]) > 0, f"{provider}: no links"
