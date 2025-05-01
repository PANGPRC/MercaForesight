# Batch Processing Documentation (`src/batch`)

## Overview

This directory contains pipelines designed for batch processing large datasets. These pipelines handle tasks like transforming data extracted from sources (potentially GCS where streaming data lands) and loading it into analytical stores like BigQuery.

![image](https://github.com/user-attachments/assets/6f097d1c-bc6e-4e8a-a4b7-601c60066a79)


## Key Pipelines

1.  **GCS Export Pipeline (`gcs_export_pipeline`)**
    *   **Purpose:** Transforms raw data stored in Google Cloud Storage (GCS) into structured dimension tables.
    *   **Process:** Reads Parquet files from a GCS source path, applies transformations (selecting fields, renaming columns) using Spark, and writes the resulting dimension tables back to a GCS destination path as Parquet files.
    *   **Key Files:** `pipeline.py` (main Spark logic), `config.py` (GCS paths, Spark settings), `utils.py` (schema definitions).

2.  **BigQuery Export Pipeline (`bq_export_pipeline`)**
    *   **Purpose:** Loads the structured dimension data from GCS into Google BigQuery.
    *   **Process:** Uses Mage AI to orchestrate the loading process. It defines a pipeline (`pipeline_definition.yaml`) with data loaders to read Parquet files from GCS and data exporters to write the data into specific BigQuery tables.
    *   **Key Files:** `pipeline_definition.yaml` (Mage pipeline structure), `data_loaders/` (scripts to read from GCS), `data_exporters/` (scripts to write to BigQuery).

## Overall Logic

1.  Data (e.g., Parquet files) is available in a GCS location.
2.  The **GCS Export Pipeline** runs, using Spark to transform this data into dimension tables within GCS.
3.  The **BigQuery Export Pipeline** runs, using Mage AI to load these dimension tables from GCS into BigQuery.

## Technologies Used

*   **Python:** Primary programming language.
*   **Apache Spark (PySpark):** Used in `gcs_export_pipeline` for large-scale data transformation.
*   **Mage AI:** Used in `bq_export_pipeline` for orchestrating the ELT process from GCS to BigQuery.
*   **Google Cloud Storage (GCS):** Used as the intermediate storage layer for data processed by Spark and as the source for Mage AI loaders.
*   **Google BigQuery:** The target data warehouse for the final dimension tables.
*   **YAML:** Used for configuration files (e.g., Mage pipeline definitions).
*   **Parquet:** File format used for storing data in GCS.
