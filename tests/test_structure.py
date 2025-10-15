import pytest
import requests

# Stable, known-good item URLs for each provider
SAMPLE_ITEMS = {
    "capella": "https://capella-open-data.s3.us-west-2.amazonaws.com/stac/capella-open-data-by-datetime/capella-open-data-2025/capella-open-data-2025-08/capella-open-data-2025-08-26/CAPELLA_C13_SP_GEO_HH_20250826023518_20250826023527/CAPELLA_C13_SP_GEO_HH_20250826023518_20250826023527.json",
    "iceye": "https://iceye-open-data-catalog.s3.amazonaws.com/stac-items/2025/09/ICEYE_ETGCZ1_20250930T115843Z_6360071_X35_SLEDF.json",
    "umbra": "https://s3.us-west-2.amazonaws.com/umbra-open-data-catalog/stac/2025/2025-06/2025-06-22/01e9fe39-3a6e-4458-a5d1-cf92eb21b961/01e9fe39-3a6e-4458-a5d1-cf92eb21b961.json",
}


@pytest.mark.parametrize("provider, url", SAMPLE_ITEMS.items())
def test_stac_item_structure(provider, url):
    """Fetches a sample STAC item and verifies its core structure."""
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        item = response.json()
    except (requests.RequestException, ValueError) as e:
        pytest.fail(f"Could not fetch or parse sample item for {provider}. Error: {e}")

    assert isinstance(item, dict), "STAC item is not a dictionary"
    assert "id" in item, "STAC item is missing 'id' field"
    assert "geometry" in item, "STAC item is missing 'geometry' field"
    assert "properties" in item, "STAC item is missing 'properties' field"
    assert "assets" in item, "STAC item is missing 'assets' field"
    assert isinstance(item["properties"], dict), "'properties' is not a dictionary"
    assert isinstance(item["assets"], dict), "'assets' is not a dictionary"
