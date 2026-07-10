# Commercial SAR STAC Catalogs

This repository provides tools to discover, consolidate, and visualize metadata from the open data STAC catalogs of major commercial SAR providers: Capella Space, ICEYE, and Umbra.

The primary goal is to create a harmonized parquet for each provider. The entire process is automated to run weekly via GitHub Actions, ensuring the datasets remain up-to-date.

> **_NOTE:_**  I believe that Synspective only provides open data upon request as of October 15, 2025 https://synspective.com/gallery/

Inspired by [@scottyhq](https://github.com/scottyhq)'s [stac2geojson](https://github.com/uw-cryo/stac2geojson)

[Web browser tool](https://pmuguda.github.io/open-sar-triad/) created by [Pavan Muguda Sanjeevamurthy](https://pmuguda.github.io/) for visualization.

## Parquets

### Visualization

Note that Capella already has a great interactive web map for its open data [https://felt.com/map/Capella-Space-Open-Data-bB24xsH3SuiUlpMdDbVRaA?loc=0,-20.5,1.83z](https://felt.com/map/Capella-Space-Open-Data-bB24xsH3SuiUlpMdDbVRaA?loc=0,-20.5,1.83z) and users should refer to this while it's still maintained.

[Development Seed](https://developmentseed.org/) provides a great open-source tool called [stac-map](https://developmentseed.org/stac-map/) for visualizing these derived geoparquets -- all you need is the GitHub endpoint to the raw geoparquet file of interest. This should match a structure similar to: 

[https://developmentseed.org/stac-map/?href=https://raw.githubusercontent.com/Jack-Hayes/commerical-sar-stac/refs/heads/main/parquets/capella/capella_GEC.parquet](https://developmentseed.org/stac-map/?href=https://raw.githubusercontent.com/Jack-Hayes/commerical-sar-stac/refs/heads/main/parquets/capella/capella_GEC.parquet)

Below are hyperlinks to access the respective parquets on this repo:
* ICEYE: [All ICEYE open data samples](https://developmentseed.org/stac-map/?href=https://raw.githubusercontent.com/Jack-Hayes/commerical-sar-stac/refs/heads/main/parquets/iceye/iceye.parquet)
* Umbra: [All Umbra open data samples](https://developmentseed.org/stac-map/?href=https://raw.githubusercontent.com/Jack-Hayes/commerical-sar-stac/refs/heads/main/parquets/umbra/umbra.parquet)
* Capella:
  [CPHD](https://developmentseed.org/stac-map/?href=https://raw.githubusercontent.com/Jack-Hayes/commerical-sar-stac/refs/heads/main/parquets/capella/capella_CPHD.parquet) |
  [CSI](https://developmentseed.org/stac-map/?href=https://raw.githubusercontent.com/Jack-Hayes/commerical-sar-stac/refs/heads/main/parquets/capella/capella_CSI.parquet) |
  [GEC](https://developmentseed.org/stac-map/?href=https://raw.githubusercontent.com/Jack-Hayes/commerical-sar-stac/refs/heads/main/parquets/capella/capella_GEC.parquet) |
  [GEO](https://developmentseed.org/stac-map/?href=https://raw.githubusercontent.com/Jack-Hayes/commerical-sar-stac/refs/heads/main/parquets/capella/capella_GEO.parquet) |
  [SICD](https://developmentseed.org/stac-map/?href=https://raw.githubusercontent.com/Jack-Hayes/commerical-sar-stac/refs/heads/main/parquets/capella/capella_SICD.parquet) |
  [SIDD](https://developmentseed.org/stac-map/?href=https://raw.githubusercontent.com/Jack-Hayes/commerical-sar-stac/refs/heads/main/parquets/capella/capella_SIDD.parquet) |
  [SLC](https://developmentseed.org/stac-map/?href=https://raw.githubusercontent.com/Jack-Hayes/commerical-sar-stac/refs/heads/main/parquets/capella/capella_SLC.parquet)

[Pavan Muguda Sanjeevamurthy](https://pmuguda.github.io/) created a great web browser tool that points towards the Umbra, Capella, and ICEYE datasets here. This interface is sleeker than the other visualization tools as it was built specifically for the data on the commercial-sar-stac repo and allows users higher fidelity filtering: [https://pmuguda.github.io/open-sar-triad/](https://pmuguda.github.io/open-sar-triad/).

### Streaming Parquet Files Directly in Python

You can load any of the published GeoParquet files directly into Python using [GeoPandas](https://geopandas.org/) without downloading them first. Simply pass the raw GitHub URL to `gpd.read_file()`:

```python
import geopandas as gpd
import fsspec

url = "https://raw.githubusercontent.com/Jack-Hayes/commerical-sar-stac/main/parquets/capella/capella_CPHD.parquet"

with fsspec.open(url, "rb") as f:
    gdf = gpd.read_parquet(f)
```

This works for any of the Parquet files; just replace the URL with the desired dataset.

### Downloading the Parquet Files

You can download the latest generated Parquet files directly using command-line tools like `curl` (for Linux/macOS) or `Invoke-WebRequest` (for Windows PowerShell)

---

### Bash (Linux/macOS)

```bash
curl -L -o iceye.parquet "https://raw.githubusercontent.com/Jack-Hayes/commerical-sar-stac/main/parquets/iceye/iceye.parquet"

curl -L -o umbra.parquet "https://raw.githubusercontent.com/Jack-Hayes/commerical-sar-stac/main/parquets/umbra/umbra.parquet"

curl -L -o capella_GEC.parquet "https://raw.githubusercontent.com/Jack-Hayes/commerical-sar-stac/main/parquets/capella/capella_GEC.parquet"
```

### PowerShell (Windows)

```console
Invoke-WebRequest -Uri "https://raw.githubusercontent.com/Jack-Hayes/commerical-sar-stac/main/parquets/iceye/iceye.parquet" -OutFile "iceye.parquet"

Invoke-WebRequest -Uri "https://raw.githubusercontent.com/Jack-Hayes/commerical-sar-stac/main/parquets/umbra/umbra.parquet" -OutFile "umbra.parquet"

Invoke-WebRequest -Uri "https://raw.githubusercontent.com/Jack-Hayes/commerical-sar-stac/main/parquets/capella/capella_GEC.parquet" -OutFile "capella_GEC.parquet"
```

## Data and API Usage Disclaimer

This repository contains open-source code for accessing and processing sample datasets provided by commercial companies including Capella Space, Umbra, and ICEYE.

All datasets and APIs are governed by their respective providers' terms of use. This repository does not redistribute or claim ownership of any proprietary or commercial data.

Users are responsible for ensuring their use of data and APIs complies with the terms set by:
- Capella Space: https://www.capellaspace.com/legal/
- Umbra: https://umbra.space/legal/
- ICEYE: https://www.iceye.com/sar-data/api


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
