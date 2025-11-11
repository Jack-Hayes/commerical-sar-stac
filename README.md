# Commercial SAR STAC Catalogs

This repository provides tools to discover, consolidate, and visualize metadata from the open data STAC catalogs of major commercial SAR providers: Capella Space, ICEYE, and Umbra.

The primary goal is to create a harmonized GeoDataFrame for each provider, which is then saved in GeoParquet format. The entire process is automated to run weekly via GitHub Actions, ensuring the datasets remain up-to-date.

> **_NOTE:_**  I believe that Synspective only provides open data upon request as of October 15, 2025 https://synspective.com/gallery/

Inspired by [@scottyhq](https://github.com/scottyhq)'s [stac2geojson](https://github.com/uw-cryo/stac2geojson)

## Parquet Formats

### VIZ (Visualization)
Optimized for browser-based visualization with [stac-map](https://developmentseed.org/blog/2025-09-02-stacmap/):
- Datetime fields parsed to `pd.Timestamp` for temporal sliders
- Bbox stored as a nested dict for spatial queries
- Assets compacted to essential fields (href, type, roles)
- GeoJSON geometry serialized for JavaScript compatibility
- Links resolved to absolute URLs

Note that Capella already has a great interactive web map for its open data [https://felt.com/map/Capella-Space-Open-Data-bB24xsH3SuiUlpMdDbVRaA?loc=0,-20.5,1.83z](https://felt.com/map/Capella-Space-Open-Data-bB24xsH3SuiUlpMdDbVRaA?loc=0,-20.5,1.83z) and users should refer to this while it's still maintained.

[Development Seed](https://developmentseed.org/) provides a great open-source tool called [stac-map](https://developmentseed.org/stac-map/) for visualizing these derived geoparquets -- all you need is the GitHub endpoint to the raw geoparquet file of interest. This should match a structure similar to: 

[https://developmentseed.org/stac-map/?href=https://raw.githubusercontent.com/Jack-Hayes/commerical-sar-stac/refs/heads/main/parquets/viz/capella/capella_GEC.parquet](https://developmentseed.org/stac-map/?href=https://raw.githubusercontent.com/Jack-Hayes/commerical-sar-stac/refs/heads/main/parquets/viz/capella/capella_GEC.parquet)

Below are hyperlinks to access the respective parquets on this repo:
* ICEYE: [All ICEYE open data samples](https://developmentseed.org/stac-map/?href=https://raw.githubusercontent.com/Jack-Hayes/commerical-sar-stac/refs/heads/main/parquets/viz/iceye/iceye.parquet)
* Umbra: [All Umbra open data samples](https://developmentseed.org/stac-map/?href=https://raw.githubusercontent.com/Jack-Hayes/commerical-sar-stac/refs/heads/main/parquets/viz/umbra/umbra.parquet)
* Capella:
  [CPHD](https://developmentseed.org/stac-map/?href=https://raw.githubusercontent.com/Jack-Hayes/commerical-sar-stac/refs/heads/main/parquets/viz/capella/capella_CPHD.parquet) |
  [CSI](https://developmentseed.org/stac-map/?href=https://raw.githubusercontent.com/Jack-Hayes/commerical-sar-stac/refs/heads/main/parquets/viz/capella/capella_CSI.parquet) |
  [GEC](https://developmentseed.org/stac-map/?href=https://raw.githubusercontent.com/Jack-Hayes/commerical-sar-stac/refs/heads/main/parquets/viz/capella/capella_GEC.parquet) |
  [GEO](https://developmentseed.org/stac-map/?href=https://raw.githubusercontent.com/Jack-Hayes/commerical-sar-stac/refs/heads/main/parquets/viz/capella/capella_GEO.parquet) |
  [SICD](https://developmentseed.org/stac-map/?href=https://raw.githubusercontent.com/Jack-Hayes/commerical-sar-stac/refs/heads/main/parquets/viz/capella/capella_SICD.parquet) |
  [SIDD](https://developmentseed.org/stac-map/?href=https://raw.githubusercontent.com/Jack-Hayes/commerical-sar-stac/refs/heads/main/parquets/viz/capella/capella_SIDD.parquet) |
  [SLC](https://developmentseed.org/stac-map/?href=https://raw.githubusercontent.com/Jack-Hayes/commerical-sar-stac/refs/heads/main/parquets/viz/capella/capella_SLC.parquet)

### ARD (Analysis-Ready Data)
Optimized for programmatic analysis:
- Asset hrefs expanded as individual columns (e.g., `asset_thumbnail`, `asset_overview`)
- Full STAC properties preserved
- Minimal transformations (e.g. serializing cols with mixed dtypes) for easier filtering/analysis

## Streaming Parquet Files Directly in Python

You can load any of the published GeoParquet files directly into Python using [GeoPandas](https://geopandas.org/) without downloading them first. Simply pass the raw GitHub URL to `gpd.read_file()`:

```python
import geopandas as gpd

# Example: Load Capella CPHD ARD parquet directly from GitHub
url = "https://raw.githubusercontent.com/Jack-Hayes/commerical-sar-stac/main/parquets/ard/capella/capella_CPHD.parquet"
gdf = gpd.read_file(url)
```

This works for any of the Parquet files; just replace the URL with the desired dataset.

> **_NOTE:_** It is important to use the 'ARD' Parquet files for Python streaming and local GIS software, as they are serialized specifically for programmatic use, as opposed to the 'VIZ' files.

## Downloading the Parquet Files

You can download the latest generated Parquet files directly using command-line tools like `curl` (for Linux/macOS) or `Invoke-WebRequest` (for Windows PowerShell)

---

### Bash (Linux/macOS)

```bash
# Download 'ARD' format (for analysis)
curl -L -o iceye_ard.parquet "https://raw.githubusercontent.com/Jack-Hayes/commerical-sar-stac/main/parquets/ard/iceye/iceye.parquet"

curl -L -o umbra_ard.parquet "https://raw.githubusercontent.com/Jack-Hayes/commerical-sar-stac/main/parquets/ard/umbra/umbra.parquet"

curl -L -o capella_GEC_ard.parquet "https://raw.githubusercontent.com/Jack-Hayes/commerical-sar-stac/main/parquets/ard/capella/capella_GEC.parquet"
```

### PowerShell (Windows)

```console
# Download 'ARD' format (for analysis)
Invoke-WebRequest -Uri "https://raw.githubusercontent.com/Jack-Hayes/commerical-sar-stac/main/parquets/ard/iceye/iceye.parquet" -OutFile "iceye_ard.parquet"

Invoke-WebRequest -Uri "https://raw.githubusercontent.com/Jack-Hayes/commerical-sar-stac/main/parquets/ard/umbra/umbra.parquet" -OutFile "umbra_ard.parquet"

Invoke-WebRequest -Uri "https://raw.githubusercontent.com/Jack-Hayes/commerical-sar-stac/main/parquets/ard/capella/capella_GEC.parquet" -OutFile "capella_GEC_ard.parquet"
```

## Data and API Usage Disclaimer

This repository contains open-source code for accessing and processing sample datasets provided by commercial companies including Capella Space, Umbra, and ICEYE.

All datasets and APIs are governed by their respective providers' terms of use. This repository does not redistribute or claim ownership of any proprietary or commercial data.

Users are responsible for ensuring their use of data and APIs complies with the terms set by:
- Capella Space: https://www.capellaspace.com/legal/
- Umbra: https://umbra.space/legal/
- ICEYE: https://www.iceye.com/sar-data/api

## Methodology

The ingestion process follows cloud-optimized best practices:

1.  **Discovery**: For nested catalogs (Umbra, Capella), the script uses `s3fs` or recursive `aiohttp` calls to efficiently discover all STAC Item URLs. For flat catalogs (ICEYE), it directly parses the collection.
2.  **Fetching**: All STAC Item JSON files are fetched concurrently using `aiohttp` for high performance.
3.  **Processing**: The raw JSONs are parsed into a uniform, flattened structure in-memory using Pandas. This includes extracting asset URLs and ensuring correct geometry representation with Shapely.
4.  **Creation**: A GeoDataFrame is created from the processed records.
5.  **Storage**: The final, cleaned GeoDataFrame for each provider (or product type) is saved as a GeoParquet file.

## Repository Structure

-   `.github/workflows/`: Contains GitHub Actions for CI (testing, linting) and weekly data updates.
-   `parquets/`: Stores the output GeoParquet files, organized by format (`/viz` or `/ard`) and provider.
-   `scripts/`: The main Python source code for data ingestion and processing.
-   `tests/`: `pytest` tests to validate endpoints and data structures.
-   `environment.yml`: The Conda environment file to ensure reproducibility.

## Local Setup and Usage

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/Jack-Hayes/commerical-sar-stac.git
    cd commerical-sar-stac
    ```

2.  **Create and activate the Conda environment:**
    ```bash
    mamba env create -f environment.yml
    mamba activate commercial-sar
    ```

3.  **Run the script:**
    You can process specific providers by passing their names as command-line arguments.

    ```bash
    # Process all providers in both formats (default)
    python -m scripts.main capella iceye umbra

    # Process only VIZ format
    python -m scripts.main capella iceye umbra --format viz

    # Process only ARD format
    python -m scripts.main capella iceye umbra --format ard

    # Process specific providers
    python -m scripts.main capella iceye
    ```

## Contributing

Contributions are welcome!  
This repository follows standard GitHub workflows with a protected `main` branch.

### How to contribute

1. **Fork** this repository to your own GitHub account.
2. **Create a feature branch** from `main` in your fork (for example, `feature/my-improvement`).
3. **Commit** your changes using clear, signed commits.
4. **Open a Pull Request (PR)** against the `main` branch of this repository.

All pull requests:
- Must pass automated checks and code quality scans.
- Require at least one review approval (by a repository admin, me :smiley:).
- Cannot be force-pushed or merged directly into `main`.

Once reviewed and approved, your PR will be merged following a linear history (no merge commits).

### Licensing

This project is released under the [MIT License](./LICENSE) 

By contributing, you agree that your contributions will be licensed under the same terms.
