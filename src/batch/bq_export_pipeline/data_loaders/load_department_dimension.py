import os
import pandas as pd
from google.cloud import storage
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader, ConfigKey
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from typing import List, Dict, Any

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

# --- Configuration Parameters --- 
APP_CONFIG_FILE = 'io_config.yaml'
APP_CONFIG_PROFILE = 'default'
DATA_SOURCE_BUCKET = 'swe5003'
DEPARTMENT_DATA_PREFIX = 'transformed_data/department_dimension.parquet/'

def get_gcs_credentials_location() -> str:
    """Fetches the GCS credentials file path from the Mage configuration."""
    cfg_path = os.path.join(get_repo_path(), APP_CONFIG_FILE)
    cfg_loader = ConfigFileLoader(cfg_path, APP_CONFIG_PROFILE)
    return cfg_loader[ConfigKey.GOOGLE_SERVICE_ACC_KEY_FILEPATH]

def list_gcs_directory_contents(bucket_name: str, dir_prefix: str, creds_path: str) -> List[str]:
    """Lists Parquet files within a specified GCS directory prefix."""
    try:
        gcs_service = storage.Client.from_service_account_json(creds_path)
        data_bucket = gcs_service.get_bucket(bucket_name)
        blob_iterator = data_bucket.list_blobs(prefix=dir_prefix)
        # Filter for actual parquet files, avoiding potential directory markers
        parquet_files = [blob.name for blob in blob_iterator if blob.name.endswith('.parquet') and blob.size > 0]
        print(f"Identified {len(parquet_files)} Parquet files under prefix '{dir_prefix}' in bucket '{bucket_name}'.")
        return parquet_files
    except Exception as e:
        print(f"Error accessing GCS bucket '{bucket_name}' with prefix '{dir_prefix}': {e}")
        # Depending on requirements, might return empty list or re-raise
        raise

@data_loader
def fetch_department_dimension_data(**context: Dict[str, Any]) -> pd.DataFrame:
    """
    Loads department dimension data from partitioned Parquet files stored in GCS.
    Relies on 'io_config.yaml' for GCS access configuration.
    """
    credentials_file = get_gcs_credentials_location()
    
    # Find all relevant department data files
    department_file_keys = list_gcs_directory_contents(
        DATA_SOURCE_BUCKET, 
        DEPARTMENT_DATA_PREFIX, 
        credentials_file
    )

    if not department_file_keys:
        print(f"Warning: No department dimension files found at gs://{DATA_SOURCE_BUCKET}/{DEPARTMENT_DATA_PREFIX}")
        return pd.DataFrame() # Return an empty DataFrame

    # Configure the GCS reader using Mage settings
    mage_config_path = os.path.join(get_repo_path(), APP_CONFIG_FILE)
    gcs_data_reader = GoogleCloudStorage.with_config(ConfigFileLoader(mage_config_path, APP_CONFIG_PROFILE))
    
    # Load data from each identified file
    department_data_segments = []
    for gcs_object_key in department_file_keys:
        print(f"Loading department data segment: {gcs_object_key}")
        try:
            segment_df = gcs_data_reader.load(DATA_SOURCE_BUCKET, gcs_object_key)
            department_data_segments.append(segment_df)
        except Exception as load_error:
            print(f"Failed to load segment {gcs_object_key}: {load_error}. Skipping.")
            # Decide on error handling: skip, retry, or fail pipeline

    if not department_data_segments:
        print("Error: Failed to load any department data segments.")
        return pd.DataFrame()

    # Combine segments into a single DataFrame
    print(f"Aggregating {len(department_data_segments)} department data segments.")
    aggregated_department_df = pd.concat(department_data_segments, ignore_index=True)
    print(f"Department dimension loaded. Shape: {aggregated_department_df.shape}")
    
    return aggregated_department_df

@test
def verify_data_integrity(loaded_df: pd.DataFrame, *args) -> None:
    """
    Performs basic validation checks on the loaded department DataFrame.
    """
    assert loaded_df is not None, "Resulting DataFrame cannot be None."
    # Add more specific assertions based on expected data characteristics
    # assert not loaded_df.empty, "Department DataFrame should not be empty after loading."
    # assert 'Department Name' in loaded_df.columns, "'Department Name' column is missing."
