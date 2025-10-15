# Commercial SAR STAC Catalogs

This repository provides tools to discover, consolidate, and visualize metadata from the open data STAC catalogs of major commercial SAR providers: Capella Space, ICEYE, and Umbra. Though note that Capella already has a great interactive web map for its open data https://felt.com/map/Capella-Space-Open-Data-bB24xsH3SuiUlpMdDbVRaA?loc=0,-20.5,1.83z 

The primary goal is to create a unified, harmonized GeoDataFrame for each provider, which is then saved in GeoParquet format. The entire process is automated to run weekly via GitHub Actions, ensuring the datasets remain up-to-date.

> **_NOTE:_**  I beleive that Synspective only provides open data upon request as of October 15, 2025 https://synspective.com/gallery/

Inspired by [@scottyhq](https://github.com/scottyhq)'s [stac2geojson](https://github.com/uw-cryo/stac2geojson)

## Downloading the Parquet Files

You can download the latest generated Parquet files directly using command-line tools like `curl` (for Linux/macOS) or `Invoke-WebRequest` (for Windows PowerShell)

---

### Bash (Linux/macOS)

```bash
# Download the ICEYE parquet file
curl -L -o iceye.parquet "https://raw.githubusercontent.com/Jack-Hayes/commerical-sar-stac/main/parquets/iceye/iceye.parquet"

# Download the Umbra parquet file
curl -L -o umbra.parquet "https://raw.githubusercontent.com/Jack-Hayes/commerical-sar-stac/main/parquets/umbra/umbra.parquet"

# Download a sample Capella parquet file (GEC)
curl -L -o capella_GEC.parquet "https://raw.githubusercontent.com/Jack-Hayes/commerical-sar-stac/main/parquets/capella/capella_GEC.parquet"
```

### PowerShell (Windows)

```console
# Download the ICEYE parquet file
Invoke-WebRequest -Uri "https://raw.githubusercontent.com/Jack-Hayes/commerical-sar-stac/main/parquets/iceye/iceye.parquet" -OutFile "iceye.parquet"

# Download the Umbra parquet file
Invoke-WebRequest -Uri "https://raw.githubusercontent.com/Jack-Hayes/commerical-sar-stac/main/parquets/umbra/umbra.parquet" -OutFile "umbra.parquet"

# Download a sample Capella parquet file (GEC)
Invoke-WebRequest -Uri "https://raw.githubusercontent.com/Jack-Hayes/commerical-sar-stac/main/parquets/capella/capella_GEC.parquet" -OutFile "capella_GEC.parquet"
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
-   `parquets/`: Stores the output GeoParquet files, organized by provider.
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
    # Process all providers
    python -m scripts.main capella iceye umbra

    # Process only Capella and ICEYE
    python -m scripts.main capella iceye
    ```