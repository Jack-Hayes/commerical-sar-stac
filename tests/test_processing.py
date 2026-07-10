import pytest
import requests
import pyarrow.parquet as pq

from src.helpers.processing import (
    compact_assets,
    densify_item_assets,
    resolve_links,
    to_stac_item,
    write_stac_geoparquet,
)

SAMPLE_ITEMS = {
    "capella": "https://capella-open-data.s3.us-west-2.amazonaws.com/stac/capella-open-data-by-datetime/capella-open-data-2025/capella-open-data-2025-08/capella-open-data-2025-08-26/CAPELLA_C13_SP_GEO_HH_20250826023518_20250826023527/CAPELLA_C13_SP_GEO_HH_20250826023518_20250826023527.json",
    "iceye": "https://iceye-open-data-catalog.s3.amazonaws.com/stac-items/2025/09/ICEYE_ETGCZ1_20250930T115843Z_6360071_X35_SLEDF.json",
    "umbra": "https://umbra-open-data-catalog.s3.us-west-2.amazonaws.com/sar-data/task-data/0007445c-5da7-4b33-bc0a-facbd249d603/2025-07-15-06-10-12_UMBRA-08/2025-07-15-06-10-12_UMBRA-08.stac.v2.json",
}


@pytest.fixture(scope="module")
def sample_items():
    items = {}
    for provider, url in SAMPLE_ITEMS.items():
        try:
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            items[provider] = (resp.json(), url)
        except requests.RequestException:
            items[provider] = None
    return items


def _get(sample_items, provider):
    if not sample_items.get(provider):
        pytest.skip(f"{provider} sample item unavailable")
    return sample_items[provider]


def test_to_stac_item_structure(sample_items):
    for provider in SAMPLE_ITEMS:
        item, url = _get(sample_items, provider)
        rec = to_stac_item(item, url, provider)
        assert rec is not None
        for field in (
            "id",
            "type",
            "stac_version",
            "geometry",
            "bbox",
            "properties",
            "assets",
            "links",
        ):
            assert field in rec, f"{provider}: missing {field}"
        assert isinstance(rec["assets"], dict) and rec["assets"]
        assert isinstance(rec["links"], list) and rec["links"]


def test_bbox_bounds(sample_items):
    for provider in SAMPLE_ITEMS:
        item, url = _get(sample_items, provider)
        bbox = to_stac_item(item, url, provider)["bbox"]
        assert isinstance(bbox, (list, tuple))
        assert len(bbox) == 4
        xmin, ymin, xmax, ymax = bbox
        assert xmin < xmax and ymin < ymax


@pytest.mark.parametrize("provider", ["capella", "iceye"])
def test_compact_assets_native_providers(sample_items, provider):
    item, url = _get(sample_items, provider)
    assets = compact_assets(item, url, provider)
    assert assets
    first = next(iter(assets.values()))
    assert first["href"].startswith("http") and "type" in first


def test_umbra_assets_synthesized_public(sample_items):
    item, url = _get(sample_items, "umbra")
    assets = compact_assets(item, url, "umbra")
    assert set(assets) == {"gec", "csi", "sicd", "sidd", "cphd"}
    for key, asset in assets.items():
        assert asset["href"].startswith("https://umbra-open-data-catalog"), key
        assert "prod-prod-processed-sar-data" not in asset["href"], key
    assert assets["gec"]["href"].endswith("_GEC.tif")


def test_umbra_links_self_only(sample_items):
    item, url = _get(sample_items, "umbra")
    links = resolve_links(item, url, "umbra")
    assert len(links) == 1 and links[0]["rel"] == "self" and links[0]["href"] == url


def test_densify_item_assets(sample_items):
    item, url = _get(sample_items, "capella")
    stac_item = to_stac_item(item, url, "capella")
    dense = densify_item_assets([stac_item])
    assert dense[0]["assets"]
    assert all(isinstance(v, dict) for v in dense[0]["assets"].values())


def test_write_stac_geoparquet_roundtrip(sample_items, tmp_path):
    item, url = _get(sample_items, "capella")
    stac_item = to_stac_item(item, url, "capella")
    path = tmp_path / "capella.parquet"

    write_stac_geoparquet([stac_item], str(path))
    pf = pq.ParquetFile(path)

    assert pf.schema_arrow.field("bbox").type
    assert "assets" in pf.schema_arrow.names
    assert "links" in pf.schema_arrow.names
