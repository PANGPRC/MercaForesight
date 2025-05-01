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

# --- Configuration --- 
IO_CONFIG_YAML = 'io_config.yaml'
IO_PROFILE_NAME = 'default'
GCS_BUCKET_ID = 'swe5003'
SHIPPING_DIM_PREFIX = 'transformed_data/shipping_dimension.parquet/'

def get_gcp_service_key_path() -> str:
    """Retrieves the path to the GCP service account key from Mage config."""
    config_abs_path = os.path.join(get_repo_path(), IO_CONFIG_YAML)
    loader = ConfigFileLoader(config_abs_path, IO_PROFILE_NAME)
    key_path = loader.get(ConfigKey.GOOGLE_SERVICE_ACC_KEY_FILEPATH)
    if not key_path or not os.path.isfile(key_path):
        raise FileNotFoundError(f"GCP service account key file not found at: {key_path}")
    return key_path

def find_gcs_parquet_objects(bucket: str, prefix: str, key_path: str) -> List[str]:
    """Lists Parquet objects in a GCS bucket matching a given prefix."""
    try:
        client = storage.Client.from_service_account_json(key_path)
        bucket_ref = client.bucket(bucket)
        blobs = list(bucket_ref.list_blobs(prefix=prefix))
        # Filter for actual Parquet files
        parquet_object_names = [
            blob.name for blob in blobs 
            if blob.name.lower().endswith('.parquet') and blob.size > 0
        ]
        print(f"Found {len(parquet_object_names)} shipping Parquet objects in gs://{bucket}/{prefix}")
        return parquet_object_names
    except Exception as e:
        print(f"Error listing objects in gs://{bucket}/{prefix}: {e}")
        raise

@data_loader
def load_shipping_dimension_data(**context: Dict[str, Any]) -> Optional[pd.DataFrame]:
    """
    Loads shipping dimension data from GCS, combining partitioned Parquet files.
    Uses 'io_config.yaml' for GCS access.
    Returns a pandas DataFrame or None on failure.
    """
    try:
        service_key_file = get_gcp_service_key_path()
        
        shipping_file_keys = find_gcs_parquet_objects(
            GCS_BUCKET_ID, 
            SHIPPING_DIM_PREFIX, 
            service_key_file
        )

        if not shipping_file_keys:
            print(f"No shipping dimension files found under gs://{GCS_BUCKET_ID}/{SHIPPING_DIM_PREFIX}. Returning empty DataFrame.")
            return pd.DataFrame() # Consistent return type

        # Prepare Mage GCS reader
        config_abs_path = os.path.join(get_repo_path(), IO_CONFIG_YAML)
        gcs_reader = GoogleCloudStorage.with_config(ConfigFileLoader(config_abs_path, IO_PROFILE_NAME))
        
        # Load each file part
        shipping_data_frames = []
        for key in shipping_file_keys:
            print(f"Loading shipping data part: {key}")
            try:
                df_part = gcs_reader.load(GCS_BUCKET_ID, key)
                shipping_data_frames.append(df_part)
            except Exception as load_error:
                print(f"Error loading {key}: {load_error}. Skipping this part.")

        if not shipping_data_frames:
            print("Failed to load any shipping data parts. Returning None.")
            return None

        # Combine parts
        print(f"Combining {len(shipping_data_frames)} shipping data parts...")
        full_shipping_df = pd.concat(shipping_data_frames, ignore_index=True)
        print(f"Shipping dimension loaded. Shape: {full_shipping_df.shape}")
        
        return full_shipping_df

    except FileNotFoundError as fnf_error:
        print(f"Configuration error: {fnf_error}")
        return None
    except Exception as general_error:
        print(f"An unexpected error occurred during shipping data loading: {general_error}")
        return None

@test
def validate_shipping_data(output_df: Optional[pd.DataFrame], *args) -> None:
    """
    Test the output of the shipping dimension loader.
    """
    assert output_df is not None, "Shipping data loading failed, DataFrame is None."
    # If no files found is acceptable, check for empty DF instead of None
    # assert not output_df.empty, "Loaded shipping DataFrame should not be empty (unless source is empty)."
    # Example column check:
    # assert 'Shipping Mode' in output_df.columns, "Missing 'Shipping Mode' column."
