import pytest
import requests

# Import URLs from the main script to avoid duplication
from scripts.main import CATALOG_URLS


@pytest.mark.parametrize(
    "provider, url",
    [
        ("capella", CATALOG_URLS["capella"]),
        ("iceye", CATALOG_URLS["iceye"]),
    ],
)
def test_http_endpoints_are_reachable(provider, url):
    """Tests if the HTTP-based STAC endpoints are online and accessible."""
    try:
        response = requests.head(url, timeout=10)
        txt_status = f"{provider} endpoint returned status {response.status_code}"
        assert response.status_code == 200, txt_status
    except requests.RequestException as e:
        pytest.fail(f"Failed to connect to {provider} endpoint at {url}. Error: {e}")


def test_s3_bucket_is_accessible():
    """Tests if the Umbra S3 bucket is accessible via a simple HEAD request on its root."""
    bucket_name = CATALOG_URLS["umbra"]
    url = f"https://{bucket_name}.s3.us-west-2.amazonaws.com/"
    try:
        response = requests.head(url, timeout=10)
        assert response.status_code == 200, f"Umbra bucket returned status {response.status_code}"
    except requests.RequestException as e:
        pytest.fail(f"Failed to connect to Umbra bucket at {url}. Error: {e}")
