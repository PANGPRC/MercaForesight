from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader, ConfigKey
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from os import path
import pandas as pd
from google.cloud import storage
from typing import List, Dict, Any

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

# Configuration constants
CONFIG_FILE_NAME = 'io_config.yaml'
CONFIG_PROFILE = 'default'
GCS_BUCKET_NAME = 'swe5003'
CUSTOMER_DIM_PREFIX = 'transformed_data/customer_dimension.parquet/part-'

def get_gcs_service_account_key_path() -> str:
    """Loads the GCS service account key path from the configuration file."""
    config_path = path.join(get_repo_path(), CONFIG_FILE_NAME)
    loader = ConfigFileLoader(config_path, CONFIG_PROFILE)
    return loader[ConfigKey.GOOGLE_SERVICE_ACC_KEY_FILEPATH]

def list_gcs_files(bucket_name: str, prefix: str, service_account_key_path: str) -> List[str]:
    """Lists files in a GCS bucket matching a specific prefix."""
    try:
        storage_client = storage.Client.from_service_account_json(service_account_key_path)
        bucket = storage_client.get_bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=prefix)
        return [blob.name for blob in blobs]
    except Exception as e:
        print(f"Error listing GCS files: {e}")
        return []

@data_loader
def load_customer_data_from_gcs(**kwargs: Dict[str, Any]) -> pd.DataFrame:
    """
    Loads partitioned customer dimension Parquet files from GCS into a single DataFrame.
    Configuration is sourced from 'io_config.yaml'.
    """
    service_account_key_path = get_gcs_service_account_key_path()
    
    # Retrieve list of relevant Parquet parts
    customer_parquet_files = list_gcs_files(
        GCS_BUCKET_NAME, 
        CUSTOMER_DIM_PREFIX, 
        service_account_key_path
    )

    if not customer_parquet_files:
        print(f"No files found matching prefix '{CUSTOMER_DIM_PREFIX}' in bucket '{GCS_BUCKET_NAME}'.")
        return pd.DataFrame() # Return empty DataFrame if no files found

    print(f"Found {len(customer_parquet_files)} customer dimension files to load.")

    # Prepare GCS loader configuration
    config_path = path.join(get_repo_path(), CONFIG_FILE_NAME)
    gcs_loader = GoogleCloudStorage.with_config(ConfigFileLoader(config_path, CONFIG_PROFILE))
    
    loaded_dataframes = []
    # Load each Parquet part into a DataFrame
    for file_key in customer_parquet_files:
        try:
            print(f"Loading file: {file_key}")
            df_part = gcs_loader.load(GCS_BUCKET_NAME, file_key)
            loaded_dataframes.append(df_part)
        except Exception as e:
            print(f"Error loading file {file_key}: {e}")
            # Decide if you want to continue or raise the error
            # continue 

    if not loaded_dataframes:
        print("No dataframes were successfully loaded.")
        return pd.DataFrame()

    # Combine all parts into one DataFrame
    print("Concatenating loaded dataframes...")
    final_customer_df = pd.concat(loaded_dataframes, ignore_index=True)
    print(f"Successfully concatenated dataframes. Final shape: {final_customer_df.shape}")
    
    return final_customer_df

@test
def check_output_validity(output_df: pd.DataFrame, *args) -> None:
    """
    Validates the output DataFrame of the data loader block.
    """
    assert output_df is not None, 'Output DataFrame should not be None.'
    # Add more specific tests if needed, e.g., checking columns or non-empty
    # assert not output_df.empty, 'Output DataFrame should not be empty.' 
    # assert 'Customer Id' in output_df.columns, 'Customer Id column is missing.'
