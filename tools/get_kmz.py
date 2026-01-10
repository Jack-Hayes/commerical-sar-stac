# tools/get_kmz.py
"""
Build a KMZ visualization (satellite track, look vectors, and preview overlay)
for a single STAC item contained in the repo's parquet viz or ard files.

This tool is intentionally provider-agnostic in interface, but current
implementation only supports capella ARD/VIZ. If a user requests `iceye`
or `umbra` the CLI will reject with a clear message.

Example usage from project root:
  python -m tools.get_kmz --provider capella \
    --id CAPELLA_C13_SP_SLC_HH_... \
    --dtype SLC \
    --output-dir /tmp


Notes:
  - pyproj and simplekml are optional dependencies. The CLI will instruct
    how to install them if missing.
  - For capella, when --dtype is provided the tool will attempt to load:
      parquets/ard/capella/capella_<DTYPE>.parquet
    (DTYPE is uppercased). If --dtype is omitted the tool reads viz parquets:
      parquets/viz/capella/*.parquet
"""

from __future__ import annotations

import argparse
import logging
import zipfile
from collections.abc import Iterable as _Iterable
from pathlib import Path
from typing import Any

import requests

from tools import utils

# Configure module logger
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger("tools.get_kmz")


_SUPPORTED_DTYPES = ("SLC", "SICD", "CPHD", "SIDD", "GEC", "GEO")


def _import_optional_visualization_deps() -> tuple[Any, Any]:
    """
    Import pyproj.Transformer and simplekml on demand. If missing,
    raise ImportError with an actionable message.
    """
    try:
        from pyproj import Transformer  # type: ignore
    except Exception as exc:
        raise ImportError(
            "pyproj is required for coordinate transforms. "
            "Install with conda: `conda install -c conda-forge pyproj==3.7.2` "
            "or via pip: `pip install pyproj==3.7.2`"
        ) from exc

    try:
        import simplekml  # type: ignore
    except Exception as exc:
        raise ImportError(
            "simplekml is required to build KMZ files. "
            "Install with conda: `conda install -c conda-forge simplekml==1.3.2` "
            "or via pip: `pip install simplekml==1.3.2`"
        ) from exc

    return Transformer, simplekml


def fetch_json_safe(url: str) -> dict | None:
    """Fetch JSON from a URL, return None on error (and log)."""
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as exc:
        LOGGER.error("Failed to fetch JSON from %s: %s", url, exc)
        return None


def fetch_bytes_safe(url: str) -> bytes | None:
    """Fetch bytes from a URL, return None on error (and log)."""
    try:
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
        return resp.content
    except requests.RequestException as exc:
        LOGGER.error("Failed to fetch bytes from %s: %s", url, exc)
        return None


# --- helpers used by popup html (unchanged from previous) ---
def _safe_get(dct: dict, *keys, default=None):
    """Nested safe get for dictionaries: _safe_get(d, 'a','b') -> d['a']['b'] or default."""
    cur = dct
    try:
        for k in keys:
            cur = cur.get(k, {})
        # If cur is an empty dict due to missing path, return default
        if cur == {}:
            return default
        return cur
    except Exception:
        return default


def _format_num(value: Any, fmt: str) -> str:
    """
    Format numeric value using a format spec (like '.0f', '.5f', '.2f').
    If value is missing or not convertible to float, return 'n/a'.
    """
    if value is None:
        return "n/a"
    try:
        val = float(value)
        return format(val, fmt)
    except Exception:
        return "n/a"


# --- main KMZ builder and popup html (same approach as before) ---
def build_kmz(
    *,
    transformer_cls: Any,
    simplekml_mod: Any,
    meta_json: dict,
    item_row: dict,
    output_path: Path,
    vector_every_n: int = 5,
) -> None:
    """
    Build and write a KMZ file containing:
      - Track (satellite orbit) as a LineString with altitude absolute
      - Optional look vectors from every Nth state vector to the image center
      - A ground overlay using the geometry bbox and a preview image
      - Popup HTML filled with important metadata
    """
    transformer = transformer_cls.from_crs("EPSG:4978", "EPSG:4326", always_xy=True)

    state_vectors = meta_json.get("collect", {}).get("state", {}).get("state_vectors", [])
    orbit_path_lla: list[tuple[float, float, float]] = []
    for sv in state_vectors:
        pos = sv.get("position")
        if not pos or len(pos) < 3:
            continue
        x, y, z = pos
        lon, lat, alt = transformer.transform(x, y, z)
        orbit_path_lla.append((lon, lat, alt))

    if not orbit_path_lla:
        LOGGER.warning("No orbit state vectors found in metadata. KMZ will omit track/vecs.")

    target_ecef = (
        meta_json.get("collect", {}).get("image", {}).get("center_pixel", {}).get("target_position")
    )
    target_lla = None
    if target_ecef and len(target_ecef) >= 3:
        target_lla = tuple(transformer.transform(*target_ecef))

    thumb_bytes = None
    thumb_url = item_row.get("asset_thumbnail")
    if thumb_url:
        thumb_bytes = fetch_bytes_safe(thumb_url)

    popup_html = _build_popup_html(meta_json, item_row)

    kml = simplekml_mod.Kml()

    if orbit_path_lla:
        track_coords = [(lon, lat, alt) for lon, lat, alt in orbit_path_lla]
        track = kml.newlinestring(name="Orbit Track", coords=track_coords)
        track.altitudemode = simplekml_mod.AltitudeMode.absolute
        track.extrude = 1
        track.style.linestyle.color = simplekml_mod.Color.cyan
        track.style.linestyle.width = 3
        track.style.polystyle.color = simplekml_mod.Color.changealphaint(
            80, simplekml_mod.Color.cyan
        )
        track.description = popup_html

    vec_folder = kml.newfolder(name="Look Vectors")
    for idx, (lon, lat, alt) in enumerate(orbit_path_lla):
        if idx % vector_every_n != 0:
            continue
        if target_lla is None:
            break
        vec = vec_folder.newlinestring(
            coords=[(lon, lat, alt), (target_lla[0], target_lla[1], target_lla[2])]
        )
        vec.altitudemode = simplekml_mod.AltitudeMode.absolute
        vec.style.linestyle.color = "3200ff00"
        vec.style.linestyle.width = 1
        vec.description = popup_html

    try:
        bounds = item_row.get("geometry").bounds
    except Exception:
        bounds = None

    if thumb_bytes and bounds:
        overlay = kml.newgroundoverlay(name="Preview")
        overlay.icon.href = "preview.png"
        overlay.latlonbox.north = bounds[3]
        overlay.latlonbox.south = bounds[1]
        overlay.latlonbox.east = bounds[2]
        overlay.latlonbox.west = bounds[0]
        overlay.description = popup_html

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(output_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("doc.kml", kml.kml())
        if thumb_bytes:
            zf.writestr("preview.png", thumb_bytes)

    LOGGER.info("KMZ written to %s", output_path)


def _build_popup_html(meta_json: dict, row_dict: dict) -> str:
    """Detailed popup HTML; defensive and following original formatting rules."""
    collect = meta_json.get("collect", {}) or {}
    radar = collect.get("radar", {}) or {}
    radar_params = radar.get("time_varying_parameters", [{}])[0] or {}
    img_params = collect.get("image", {}) or {}

    delta_line = _safe_get(img_params, "image_geometry", "delta_line_time", default="n/a")
    range_first_sample = _safe_get(
        img_params, "image_geometry", "range_to_first_sample", default=None
    )
    range_res = _safe_get(img_params, "range_resolution", default=None)
    ground_range_res = _safe_get(img_params, "ground_range_resolution", default=None)
    az_res = _safe_get(img_params, "azimuth_resolution", default=None)
    nesz_peak = _safe_get(img_params, "nesz_peak", default=None)
    az_beam = _safe_get(collect, "transmit_antenna", "azimuth_beamwidth", default=None)
    el_beam = _safe_get(collect, "transmit_antenna", "elevation_beamwidth", default=None)
    sampling_freq = _safe_get(collect, "radar", "sampling_frequency", default=None)
    prf = _safe_get(radar_params, "prf", default=None)
    pulse_bw = _safe_get(radar_params, "pulse_bandwidth", default=None)
    pulse_dur = _safe_get(radar_params, "pulse_duration", default="n/a")

    datetime = row_dict.get("datetime", "n/a")
    mode = row_dict.get("sar:instrument_mode", "n/a")
    orbit_state = row_dict.get("sat:orbit_state", "n/a")
    center_freq = row_dict.get("sar:center_frequency", "n/a")
    polarizations = row_dict.get("sar:polarizations", "n/a")
    look_angle = row_dict.get("capella:look_angle", "n/a")
    squint_angle = row_dict.get("capella:squint_angle", "n/a")
    layover_angle = row_dict.get("capella:layover_angle", "n/a")

    range_first_sample_fmt = _format_num(range_first_sample, ".0f")
    range_res_fmt = _format_num(range_res, ".5f")
    ground_range_res_fmt = _format_num(ground_range_res, ".5f")
    az_res_fmt = _format_num(az_res, ".5f")
    nesz_peak_fmt = _format_num(nesz_peak, ".2f")
    az_beam_fmt = _format_num(az_beam, ".6f")
    el_beam_fmt = _format_num(el_beam, ".6f")
    sampling_freq_fmt = _format_num(sampling_freq, ".0f")
    prf_fmt = _format_num(prf, ".0f")
    pulse_bw_fmt = _format_num(pulse_bw, ".0f")
    pulse_dur_fmt = pulse_dur if pulse_dur is not None else "n/a"

    html = f"""
    <table border="1" cellpadding="2" cellspacing="0" width="400">
      <tr><td colspan="2" bgcolor="#CCCCCC"><b>Metadata (static, not per-vector)</b></td></tr>
      <tr><td colspan="2"><b>GeoPandas / STAC Fields</b></td></tr>
      <tr><td>Datetime</td><td>{datetime}</td></tr>
      <tr><td>Mode</td><td>{mode}</td></tr>
      <tr><td>Orbit State</td><td>{orbit_state}</td></tr>
      <tr><td>Center Freq</td><td>{center_freq} Hz</td></tr>
      <tr><td>Polarization</td><td>{polarizations}</td></tr>
      <tr><td>Look Angle</td><td>{look_angle}</td></tr>
      <tr><td>Squint Angle</td><td>{squint_angle}</td></tr>
      <tr><td>Layover Angle</td><td>{layover_angle}</td></tr>

      <tr><td colspan="2"><b>Extended JSON Fields</b></td></tr>
      <tr><td>Delta Line Time_s</td><td>{delta_line}</td></tr>
      <tr><td>Range to 1st Sample_m</td><td>{range_first_sample_fmt}</td></tr>
      <tr><td>Range Res_m</td><td>{range_res_fmt}</td></tr>
      <tr><td>Ground Range Res_m</td><td>{ground_range_res_fmt}</td></tr>
      <tr><td>Azimuth Res_m</td><td>{az_res_fmt}</td></tr>
      <tr><td>NESZ Peak</td><td>{nesz_peak_fmt}</td></tr>
      <tr><td>Az Beamwidth</td><td>{az_beam_fmt}</td></tr>
      <tr><td>El Beamwidth</td><td>{el_beam_fmt}</td></tr>
      <tr><td>Sampling Freq_Hz</td><td>{sampling_freq_fmt}</td></tr>
      <tr><td>PRF_Hz</td><td>{prf_fmt}</td></tr>
      <tr><td>Pulse BW_Hz</td><td>{pulse_bw_fmt}</td></tr>
      <tr><td>Pulse Dur_s</td><td>{pulse_dur_fmt}</td></tr>
    </table>
    """
    return html


def parse_args(argv: _Iterable[str] | None = None) -> argparse.Namespace:
    """Create and parse CLI arguments."""
    parser = argparse.ArgumentParser(
        prog="get_kmz",
        description="Export a KMZ with satellite orbit, look vectors and preview overlay "
        "for a single STAC item from local parquets.",
    )
    parser.add_argument(
        "--provider",
        "-p",
        required=True,
        choices=["capella", "iceye", "umbra"],
        help="Provider name (capella | iceye | umbra).",
    )
    parser.add_argument(
        "--id",
        "-i",
        required=True,
        help="Item ID to find in the provider GeoDataFrame (matches gdf['id']).",
    )
    parser.add_argument(
        "--dtype",
        "-d",
        required=True,
        help=f"Capella product dtype (one of: {', '.join(_SUPPORTED_DTYPES)}). ",
        choices=list(_SUPPORTED_DTYPES),
    )
    parser.add_argument(
        "--output-dir",
        "-o",
        default=".",
        help="Local output directory to write the KMZ file (default: current dir).",
    )
    parser.add_argument(
        "--parquet-root",
        "-r",
        default="parquets",
        help="Root location of local parquet files (default: ./parquets).",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def _row_to_dict(row) -> dict:
    """Convert a pandas Series (or row-like) to a plain dict with safe gets."""
    try:
        return row.to_dict()
    except Exception:
        return dict(row)


def main(argv: _Iterable[str] | None = None) -> int:
    """Main CLI entry point. Returns an exit code (0 success)."""
    args = parse_args(argv)
    parquet_root = Path(args.parquet_root)
    provider = args.provider
    item_id = args.id
    output_dir = Path(args.output_dir)
    dtype = args.dtype  # may be None

    # --- Provider gating: only capella supported for now ---
    if provider.lower() != "capella":
        LOGGER.error("NO SUPPORT FOR UMBRA AND ICEYE DATA YET. Requested: %s", provider)
        return 1

    LOGGER.info(
        "Loading provider '%s' (dtype=%s) from %s",
        provider,
        dtype,
        parquet_root,
    )

    gdf = utils.load_provider_gdf(
        parquet_root,
        provider,
        dtype=dtype,
    )

    if gdf is None:
        if dtype:
            LOGGER.error(
                "No Capella ARD parquet found for dtype '%s' "
                "(expected: parquets/ard/capella/capella_%s.parquet)",
                dtype,
                dtype.upper(),
            )
        else:
            LOGGER.error(
                "No viz parquet files found for provider '%s' under %s",
                provider,
                parquet_root,
            )
        return 2

    row = utils.find_row_by_id(gdf, item_id)
    if row is None:
        LOGGER.error("Item id '%s' not found for provider '%s' (dtype=%s)", item_id, provider, dtype)
        return 3

    row_dict = _row_to_dict(row)

    # Import optional visualization dependencies lazily
    try:
        TransformerCls, simplekml_mod = _import_optional_visualization_deps()
    except ImportError as exc:
        LOGGER.error(str(exc))
        return 4

    meta_url = row_dict.get("asset_metadata")
    if not meta_url:
        LOGGER.error("Selected item is missing 'asset_metadata' URL.")
        return 5

    meta_json = fetch_json_safe(meta_url)
    if meta_json is None:
        LOGGER.error("Could not fetch asset_metadata from %s", meta_url)
        return 6

    safe_id = str(item_id).replace("/", "_")
    out_fname = f"{provider}_{safe_id}.kmz"
    output_path = (output_dir / out_fname).absolute()

    LOGGER.info("Building KMZ to %s", output_path)
    try:
        build_kmz(
            transformer_cls=TransformerCls,
            simplekml_mod=simplekml_mod,
            meta_json=meta_json,
            item_row=row_dict,
            output_path=output_path,
        )
    except Exception as exc:  # broad try to capture unexpected runtime issues
        LOGGER.exception("Failed to build KMZ: %s", exc)
        return 7

    LOGGER.info("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
