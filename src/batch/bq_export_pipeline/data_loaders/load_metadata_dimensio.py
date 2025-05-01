import os
import pandas as pd
from google.cloud import storage
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader, ConfigKey
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from typing import List, Dict, Any, Optional

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

# --- Global Settings --- 
MAGE_CONFIG_FILENAME = 'io_config.yaml'
MAGE_CONFIG_PROFILE = 'default'
SOURCE_GCS_BUCKET = 'swe5003'
METADATA_GCS_PREFIX = 'transformed_data/metadata_dimension.parquet/'

def locate_gcp_credentials() -> str:
    """Finds the GCP credentials file path using Mage configuration."""
    repo_root = get_repo_path()
    config_file = os.path.join(repo_root, MAGE_CONFIG_FILENAME)
    loader = ConfigFileLoader(config_file, MAGE_CONFIG_PROFILE)
    creds_path = loader.get(ConfigKey.GOOGLE_SERVICE_ACC_KEY_FILEPATH)
    if not creds_path or not os.path.exists(creds_path):
        raise FileNotFoundError(f"GCS credentials not found or invalid path: {creds_path}")
    return creds_path

def discover_gcs_parquet_parts(bucket: str, prefix: str, credentials_file: str) -> List[str]:
    """Identifies Parquet files within a specified GCS location."""
    try:
        gcs = storage.Client.from_service_account_json(credentials_file)
        bucket_obj = gcs.bucket(bucket)
        blobs = bucket_obj.list_blobs(prefix=prefix)
        # Filter strictly for .parquet files, ignoring zero-byte files or directory markers
        parquet_files = [
            blob.name for blob in blobs 
            if blob.name.lower().endswith('.parquet') and blob.size > 0
        ]
        print(f"Located {len(parquet_files)} metadata Parquet parts in gs://{bucket}/{prefix}")
        return parquet_files
    except Exception as e:
        print(f"Failed to list GCS objects under gs://{bucket}/{prefix}. Error: {e}")
        raise

@data_loader
def acquire_metadata_dimension(**runtime_options: Dict[str, Any]) -> Optional[pd.DataFrame]:
    """
    Reads metadata dimension data from partitioned Parquet files on GCS.
    Aggregates the data into a single pandas DataFrame.
    Uses 'io_config.yaml' for necessary GCS credentials and settings.
    Returns the aggregated DataFrame, or None if the process fails.
    """
    try:
        gcp_keyfile = locate_gcp_credentials()
        
        metadata_object_keys = discover_gcs_parquet_parts(
            SOURCE_GCS_BUCKET, 
            METADATA_GCS_PREFIX, 
            gcp_keyfile
        )

        if not metadata_object_keys:
            print("No metadata dimension files were found. Returning an empty DataFrame.")
            return pd.DataFrame() # Return empty DF instead of None if no files found

        # Set up the Mage GCS reader
        repo_root = get_repo_path()
        config_file = os.path.join(repo_root, MAGE_CONFIG_FILENAME)
        gcs_reader_config = ConfigFileLoader(config_file, MAGE_CONFIG_PROFILE)
        gcs_reader = GoogleCloudStorage.with_config(gcs_reader_config)
        
        # Load data from each file
        loaded_segments = []
        for key in metadata_object_keys:
            print(f"Reading metadata segment: {key}")
            try:
                df_segment = gcs_reader.load(SOURCE_GCS_BUCKET, key)
                loaded_segments.append(df_segment)
            except Exception as load_err:
                print(f"Error loading segment {key}: {load_err}. Skipping.")
                # Continue processing other files

        if not loaded_segments:
            print("Failed to load any metadata segments. Returning None.")
            return None

        # Combine into one DataFrame
        print(f"Combining {len(loaded_segments)} metadata segments...")
        final_metadata_df = pd.concat(loaded_segments, ignore_index=True)
        print(f"Metadata dimension loaded. Shape: {final_metadata_df.shape}")
        
        return final_metadata_df

    except Exception as pipeline_error:
        print(f"An unexpected error occurred during metadata loading: {pipeline_error}")
        return None # Return None on major failure

@test
def check_loaded_metadata(df: Optional[pd.DataFrame], *args) -> None:
    """
    Validates the DataFrame produced by the metadata loader.
    """
    assert df is not None, "Metadata loading failed, resulting DataFrame is None."
    # If no files were found, the DF might be empty, which could be valid.
    # Add checks relevant to metadata if it should never be empty.
    # assert not df.empty, "Metadata DataFrame should not be empty."
    # assert 'key' in df.columns, "Metadata DataFrame missing expected 'key' column."
