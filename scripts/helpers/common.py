from urllib.parse import urljoin

import aiohttp
import requests


async def fetch_json_tolerant(session, url):
    """Coroutine to fetch JSON, tolerating incorrect content-type headers."""
    try:
        async with session.get(url) as response:
            response.raise_for_status()
            return await response.json(content_type=None)
    except aiohttp.ClientError as e:
        print(f"\nWarning: Failed to fetch {url}. Error: {e}")
        return None


def discover_child_links(catalog_url):
    """Fetches a catalog and returns a dictionary of its child links."""
    try:
        response = requests.get(catalog_url)
        response.raise_for_status()
        child_links = [
            link for link in response.json().get("links", []) if link.get("rel") == "child"
        ]
        return {link.get("title"): urljoin(catalog_url, link["href"]) for link in child_links}
    except requests.RequestException as e:
        print(f"FATAL: Could not fetch entry catalog {catalog_url}. Error: {e}")
        return {}
