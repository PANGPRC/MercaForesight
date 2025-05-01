import os
from typing import ClassVar

class GCSExportSettings:
    """Defines configuration parameters for the GCS batch export process."""

    # --- File System Paths ---
    # Paths are relative to the project root or absolute.
    # Ensure these files exist at the specified locations.
    GCP_CREDENTIALS_FILE: ClassVar[str] = os.path.abspath('./infra/docker/mage/google-cred.json')
    SPARK_GCS_JAR: ClassVar[str] = os.path.abspath("./infra/docker/spark/jar_files/gcs-connector-hadoop3-2.2.5.jar")

    # --- Google Cloud Storage Configuration ---
    TARGET_GCS_BUCKET_URI: ClassVar[str] = "gs://swe5003/" # Base URI for the bucket
    SOURCE_DATA_PATH_PATTERN: ClassVar[str] = os.path.join(TARGET_GCS_BUCKET_URI, "raw_streaming/*") # Pattern for input Parquet files
    DESTINATION_DATA_PATH: ClassVar[str] = os.path.join(TARGET_GCS_BUCKET_URI, "transformed_data/") # Base path for output dimensions

    # --- Spark Execution Environment ---
    SPARK_EXECUTION_MODE: ClassVar[str] = "local[*]" # Spark master URL
    SPARK_APPLICATION_ID: ClassVar[str] = "GCSBatchDimensionExport" # Name for the Spark application
    # Verify this GCP Project ID is correct for the environment
    GOOGLE_PROJECT_ID: ClassVar[str] = "arctic-surf-456413-f0"

    @classmethod
    def verify_paths(cls):
        """Checks if essential configuration files are accessible."""
        print("Verifying essential file paths...")
        if not os.path.isfile(cls.GCP_CREDENTIALS_FILE):
            raise FileNotFoundError(f"GCP credentials file is missing: {cls.GCP_CREDENTIALS_FILE}")
        if not os.path.isfile(cls.SPARK_GCS_JAR):
            raise FileNotFoundError(f"Spark GCS connector JAR is missing: {cls.SPARK_GCS_JAR}")
        print("Essential file paths verified successfully.")

# Usage Example:
# from gcs_export_settings import GCSExportSettings
# print(GCSExportSettings.GOOGLE_PROJECT_ID)
# GCSExportSettings.verify_paths() # Recommended to run at the start

