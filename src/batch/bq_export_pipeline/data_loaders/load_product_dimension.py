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

# --- Configuration --- 
IO_CONFIG_FILE = 'io_config.yaml'
IO_CONFIG_PROFILE = 'default'
SOURCE_BUCKET = 'swe5003'
PRODUCT_DIM_GCS_PREFIX = 'transformed_data/product_dimension.parquet/'

def fetch_gcs_credentials_path() -> str:
    """Retrieves the GCS credential file path from Mage io_config."""
    config_full_path = os.path.join(get_repo_path(), IO_CONFIG_FILE)
    config_loader = ConfigFileLoader(config_full_path, IO_CONFIG_PROFILE)
    return config_loader[ConfigKey.GOOGLE_SERVICE_ACC_KEY_FILEPATH]

def find_gcs_objects(bucket_id: str, object_prefix: str, credentials_path: str) -> List[str]:
    """Fetches a list of object names from GCS matching a prefix."""
    try:
        gcs_client = storage.Client.from_service_account_json(credentials_path)
        target_bucket = gcs_client.get_bucket(bucket_id)
        matching_blobs = target_bucket.list_blobs(prefix=object_prefix)
        object_names = [blob.name for blob in matching_blobs if blob.name.endswith('.parquet')]
        print(f"Found {len(object_names)} objects with prefix '{object_prefix}' in bucket '{bucket_id}'.")
        return object_names
    except Exception as err:
        print(f"Failed to list GCS objects: {err}")
        raise # Re-raise the exception after logging

@data_loader
def retrieve_product_dimension_from_gcs(**kwargs: Dict[str, Any]) -> pd.DataFrame:
    """
    Loads product dimension data, partitioned as Parquet files, from GCS.
    Uses configuration specified in 'io_config.yaml'.
    """
    gcs_creds_path = fetch_gcs_credentials_path()
    
    # Identify the Parquet files for the product dimension
    product_parquet_keys = find_gcs_objects(
        SOURCE_BUCKET, 
        PRODUCT_DIM_GCS_PREFIX, 
        gcs_creds_path
    )

    if not product_parquet_keys:
        print("No product dimension Parquet files found. Returning empty DataFrame.")
        return pd.DataFrame()

    # Set up GCS data loader
    config_full_path = os.path.join(get_repo_path(), IO_CONFIG_FILE)
    gcs_reader = GoogleCloudStorage.with_config(ConfigFileLoader(config_full_path, IO_CONFIG_PROFILE))
    
    # Read each part file and store in a list
    dataframe_parts = []
    for object_key in product_parquet_keys:
        print(f"Reading GCS object: gs://{SOURCE_BUCKET}/{object_key}")
        try:
            df_slice = gcs_reader.load(SOURCE_BUCKET, object_key)
            dataframe_parts.append(df_slice)
        except Exception as err:
            print(f"Error reading object {object_key}: {err}. Skipping this file.")
            # Consider whether to fail entirely or just skip problematic files

    if not dataframe_parts:
        print("Failed to load any data parts. Returning empty DataFrame.")
        return pd.DataFrame()

    # Assemble the final DataFrame
    print(f"Combining {len(dataframe_parts)} loaded DataFrame parts.")
    product_dimension_df = pd.concat(dataframe_parts, ignore_index=True)
    print(f"Final product dimension DataFrame shape: {product_dimension_df.shape}")
    
    return product_dimension_df

@test
def validate_loaded_data(df_result: pd.DataFrame, *args) -> None:
    """
    Test function to validate the loaded product dimension data.
    """
    assert df_result is not None, "The loaded DataFrame must not be None."
    # Example additional checks (uncomment and adapt as needed):
    # assert not df_result.empty, "The loaded DataFrame should contain data."
    # assert 'Product Name' in df_result.columns, "Expected 'Product Name' column is missing."
