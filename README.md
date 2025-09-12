# SPEI API
[![Ask DeepWiki](https://devin.ai/assets/askdeepwiki.png)](https://deepwiki.com/GabrielT7878/spei_api)

This repository provides a microservice-based solution for calculating the Standardized Precipitation-Evapotranspiration Index (SPEI) from climate data. It comprises an R-based API for the core SPEI calculation and a Python client for batch processing large NetCDF climate datasets in parallel.

## Overview

The system is designed to handle large-scale climate data efficiently. It decouples the data processing logic (Python) from the scientific computation (R), allowing for scalable and parallel execution.

The workflow is as follows:
1.  **Data Preparation**: Helper scripts are used to extract and clean time-series data (Precipitation, Tmin, Tmax) for specific geographical points from large source NetCDF files.
2.  **API Deployment**: The R API is containerized with Docker and can be launched using Docker Compose. It exposes an endpoint to calculate SPEI.
3.  **Batch Processing**: A Python script reads the prepared data, sends parallel requests to the R API for each data point, and aggregates the results.
4.  **Output**: The final SPEI time-series data is saved as a new NetCDF file.

## Core Components

### R API (`api.r`)

A web API built with the Plumber R package.
*   **Functionality**: It exposes an endpoint (`/spei`) that receives monthly climate data (Tmin, Tmax, Precipitation) and calculates SPEI for specified time scales.
*   **Methodology**: It uses the `SPEI` R package for the calculation. Potential Evapotranspiration (PET) is computed using the Hargreaves method.
*   **Dependencies**: `plumber`, `SPEI`, `jsonlite`, `lubridate`. These are installed automatically when the container starts.
*   **Deployment**: Runs as a service defined in `compose.yaml` using the `rocker/r-ver:4.5.1` Docker image.

### Python Client (`main.py`)

A script to orchestrate the batch processing of climate data.
*   **Functionality**:
    *   Reads a pre-processed NetCDF dataset containing time series for multiple points.
    *   Aggregates daily data into monthly averages (for Tmin/Tmax) and sums (for precipitation).
    *   Uses a `ThreadPoolExecutor` to send concurrent requests to the R API, one for each point.
    *   Parses the JSON responses and combines the results from all points.
    *   Saves the final aggregated SPEI data into a single NetCDF file in the `api_output/` directory.

### Helper Scripts (`helpers/`)

*   `cluster.py`: Extracts data for a specific geographic cluster of points from raw, large-scale NetCDF source files.
*   `fix.py`: Cleans the extracted data by correcting temperature inversions (`Tmin` > `Tmax`) via linear interpolation and filling missing precipitation values with zero.
*   `select_points.py`: A utility to find the integer indices of specific latitude/longitude points within a dataset.
*   `verify.py`: A simple script to open and inspect the final output NetCDF file.

## Getting Started

### Prerequisites
*   Docker and Docker Compose
*   Python 3
*   Python libraries: `requests`, `xarray`, `pandas`, `numpy`
*   Input climate data in NetCDF format.

### 1. Data Preparation
Before running the main application, you need to prepare your input data.

1.  Modify `helpers/cluster.py` to point to your raw NetCDF files and define the desired geographical points or basin.
2.  Run the script to generate a clustered dataset (e.g., `data_cluster_AMAZÔNICA_k0.nc`).
    ```bash
    python helpers/cluster.py
    ```
3.  Run `helpers/fix.py` to clean the generated dataset. This will produce the final input file for the main script (e.g., `data_cluster_AMAZÔNICA_k0_corrigido.nc`).
    ```bash
    python helpers/fix.py
    ```

### 2. Run the API
Start the R API service using Docker Compose. This will build the image if necessary and start the container in detached mode.

```bash
docker-compose up -d
```
The API will be available at `http://localhost:8000`. You can check its status by navigating to `http://localhost:8000/status`.

### 3. Run the Processing Client
1.  Open `main.py` and configure the following variables:
    *   `scales`: A list of integer scales for SPEI calculation (e.g., `[3, 12]`).
    *   `date_range`: The start and end dates for the time slice to be processed.
    *   `ds = xr.open_dataset(...)`: Update the filename to your corrected input data file.
    *   `pontos`: A list of integer indices for the points you want to process. You can use `helpers/select_points.py` to identify these.
2.  Execute the script.
    ```bash
    python main.py
    ```
The script will print progress as it processes each point. The final output will be saved in the `api_output/` directory (e.g., `api_output/SPEI_amazonica.nc`).

## API Endpoint Detail

### Calculate SPEI
*   **Endpoint:** `POST /spei`
*   **Description:** Calculates SPEI for a single point's time-series data.
*   **Request Body (JSON):**
    ```json
    {
        "tmin_data": [4.5, 5.1, ...],
        "tmax_data": [15.2, 16.0, ...],
        "pr_data": [30.1, 45.5, ...],
        "lat": -15.78,
        "scales": [3, 12]
    }
    ```
    *   `tmin_data`: `list` of monthly average minimum temperatures.
    *   `tmax_data`: `list` of monthly average maximum temperatures.
    *   `pr_data`: `list` of monthly total precipitation.
    *   `lat`: `float` latitude of the point.
    *   `scales`: `list` of integer scales for the SPEI calculation.

*   **Success Response (200 OK):**
    A JSON object where keys correspond to the requested scales and values are the calculated SPEI time series.
    ```json
    {
        "SPEI_3": [-0.56, 0.12, 1.23, ...],
        "SPEI_12": [-0.89, -0.75, -0.21, ...]
    }
