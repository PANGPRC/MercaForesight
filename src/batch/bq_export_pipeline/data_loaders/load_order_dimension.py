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

# --- Configuration Variables --- 
YAML_CONFIG_FILE = 'io_config.yaml'
YAML_PROFILE = 'default'
GCS_STORAGE_BUCKET = 'swe5003'
ORDER_DIM_GCS_FOLDER = 'transformed_data/order_dimension.parquet/'

def get_service_account_path_from_config() -> str:
    """Retrieves the GCS service account key file path via Mage config."""
    absolute_config_path = os.path.join(get_repo_path(), YAML_CONFIG_FILE)
    config_instance = ConfigFileLoader(absolute_config_path, YAML_PROFILE)
    return config_instance[ConfigKey.GOOGLE_SERVICE_ACC_KEY_FILEPATH]

def list_parquet_files_in_gcs(bucket_name: str, folder_prefix: str, service_account_path: str) -> List[str]:
    """Lists all Parquet files found under a specific GCS prefix."""
    try:
        storage_service = storage.Client.from_service_account_json(service_account_path)
        bucket_handle = storage_service.get_bucket(bucket_name)
        blob_list = list(bucket_handle.list_blobs(prefix=folder_prefix))
        # Filter for .parquet files and exclude potential directory markers
        parquet_keys = [
            blob.name for blob in blob_list 
            if blob.name.endswith('.parquet') and not blob.name.endswith('/')
        ]
        print(f"Found {len(parquet_keys)} Parquet files in gs://{bucket_name}/{folder_prefix}")
        return parquet_keys
    except Exception as error:
        print(f"Error listing GCS folder gs://{bucket_name}/{folder_prefix}: {error}")
        raise

@data_loader
def ingest_order_dimension_from_gcs(**execution_params: Dict[str, Any]) -> Optional[pd.DataFrame]:
    """
    Loads the order dimension data from partitioned Parquet files in GCS.
    Combines the partitions into a single pandas DataFrame.
    GCS connection details are managed via 'io_config.yaml'.
    Returns the combined DataFrame or None on failure.
    """
    gcp_credentials_path = get_service_account_path_from_config()
    
    order_parquet_objects = list_parquet_files_in_gcs(
        GCS_STORAGE_BUCKET, 
        ORDER_DIM_GCS_FOLDER, 
        gcp_credentials_path
    )

    if not order_parquet_objects:
        print(f"No order dimension Parquet files detected in gs://{GCS_STORAGE_BUCKET}/{ORDER_DIM_GCS_FOLDER}. Aborting load.")
        return None

    # Configure Mage GCS reader
    absolute_config_path = os.path.join(get_repo_path(), YAML_CONFIG_FILE)
    gcs_file_reader = GoogleCloudStorage.with_config(ConfigFileLoader(absolute_config_path, YAML_PROFILE))
    
    # Read each Parquet file part
    order_data_parts = []
    for object_path in order_parquet_objects:
        print(f"Loading order data part: {object_path}")
        try:
            data_chunk = gcs_file_reader.load(GCS_STORAGE_BUCKET, object_path)
            order_data_parts.append(data_chunk)
        except Exception as read_error:
            print(f"Failed to read {object_path}: {read_error}. Skipping this part.")
            # Consider error handling strategy: skip, retry, or fail

    if not order_data_parts:
        print("Critical error: No order data parts could be loaded successfully. Returning None.")
        return None

    # Assemble the full DataFrame
    print(f"Stitching together {len(order_data_parts)} order data parts...")
    complete_order_df = pd.concat(order_data_parts, ignore_index=True)
    print(f"Order dimension successfully loaded. Total records: {len(complete_order_df)}")
    
    return complete_order_df

@test
def validate_output_dataframe(result_df: Optional[pd.DataFrame], *args) -> None:
    """
    Test suite for the order dimension data loader.
    """
    assert result_df is not None, "Loader returned None, indicating a failure."
    assert not result_df.empty, "Loaded DataFrame should contain order data."
    # Example: Check for a key column
    # assert 'Order Id' in result_df.columns, "The 'Order Id' column is missing from the loaded data."
