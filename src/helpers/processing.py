from __future__ import annotations

from urllib.parse import urljoin

from shapely import force_2d
from shapely.geometry import mapping, shape

from stac_geoparquet.arrow import parse_stac_items_to_arrow, to_parquet

# Umbra's STAC hrefs point at a private bucket; key -> (filename suffix, media type, roles)
UMBRA_HTTPS_BASE = "https://umbra-open-data-catalog.s3.us-west-2.amazonaws.com/"
_COG = "image/tiff; application=geotiff; profile=cloud-optimized"
UMBRA_ASSETS = {
    "gec": ("_GEC.tif", _COG, ["data"]),
    "csi": ("_CSI.tif", _COG, ["data"]),
    "sicd": ("_SICD.nitf", "application/vnd.nitf", ["data"]),
    "sidd": ("_SIDD.nitf", "application/vnd.nitf", ["data"]),
    "cphd": ("_CPHD.cphd", "application/octet-stream", ["data"]),
}

# Free-form properties whose shape varies across items and breaks Arrow schema inference
_DROP_PROPERTIES = ("providers",)


def _umbra_assets(item_url: str) -> dict:
    base, fname = item_url.rsplit("/", 1)
    stem = (
        fname[: -len(".stac.v2.json")]
        if fname.endswith(".stac.v2.json")
        else fname.rsplit(".", 1)[0]
    )
    return {
        key: {"href": f"{base}/{stem}{suffix}", "type": mtype, "roles": roles}
        for key, (suffix, mtype, roles) in UMBRA_ASSETS.items()
    }


def compact_assets(item: dict, item_url: str, provider: str) -> dict:
    """href/type/roles per asset. Umbra is synthesized from the public item URL"""
    if provider == "umbra":
        return _umbra_assets(item_url)
    out = {}
    for key, asset in (item.get("assets") or {}).items():
        if isinstance(asset, dict) and asset.get("href"):
            out[key] = {
                "href": asset["href"],
                "type": asset.get("type"),
                "roles": asset.get("roles") or [],
            }
    return out


def resolve_links(item: dict, item_url: str, provider: str) -> list:
    """Absolute-ise relative hrefs. Umbra's only link ('umbra-sar') is dead, so emit self"""
    if provider == "umbra":
        return [{"rel": "self", "href": item_url, "type": "application/geo+json"}]
    out = []
    for link in item.get("links") or []:
        if not isinstance(link, dict) or "href" not in link:
            continue
        href = link["href"]
        if not href.startswith(("http://", "https://", "s3://")):
            href = urljoin(item_url, href)
        out.append({"rel": link.get("rel"), "href": href, "type": link.get("type")})
    return out


def to_stac_item(item: dict, item_url: str, provider: str) -> dict | None:
    """Turn a raw STAC item into a clean, uniform STAC item dict for stac-geoparquet"""
    if not item or not item.get("geometry"):
        return None
    geom = force_2d(
        shape(item["geometry"])
    )  # Umbra geometries carry a Z coord that breaks stac-map
    out = dict(item)
    out["type"] = "Feature"
    out.setdefault("stac_version", "1.0.0")
    out["id"] = item.get("id")
    out["geometry"] = mapping(geom)
    out["bbox"] = [
        float(v) for v in geom.bounds
    ]  # 2D bbox, consistent with the 2D geometry
    out["assets"] = compact_assets(item, item_url, provider)
    out["links"] = resolve_links(item, item_url, provider)
    props = dict(out.get("properties") or {})
    for key in _DROP_PROPERTIES:
        props.pop(key, None)
    out["properties"] = props
    return out


def densify_item_assets(items: list[dict]) -> list[dict]:
    """
    Give every item the same asset keys so the assets column is a uniform, non-null struct

    Absent assets become a present sub-struct with EMPTY (not null) fields. A null sub-struct is
    what triggers stac-map's DuckDB-WASM "0 child arrays" load error; a null href is what breaks
    the overlay's href.split(...) on click. Empty strings avoid both. No-op for Umbra (uniform)
    """
    empty = {"href": "", "type": "", "roles": []}
    keys = sorted({k for it in items for k in (it.get("assets") or {})})
    for it in items:
        assets = it.get("assets") or {}
        it["assets"] = {k: (assets[k] if assets.get(k) else dict(empty)) for k in keys}
    return items


def write_stac_geoparquet(items: list[dict], path: str) -> None:
    to_parquet(parse_stac_items_to_arrow(densify_item_assets(items)), path)
