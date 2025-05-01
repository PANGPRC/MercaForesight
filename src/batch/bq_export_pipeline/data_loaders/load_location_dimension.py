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

# --- Constants --- 
CONFIG_FILENAME = 'io_config.yaml'
CONFIG_PROFILE_KEY = 'default'
GCS_DATA_BUCKET = 'swe5003'
LOCATION_DIM_PATH_PREFIX = 'transformed_data/location_dimension.parquet/'

def retrieve_gcs_credentials_filepath() -> str:
    """Gets the path to the GCS credentials file from Mage config."""
    config_abs_path = os.path.join(get_repo_path(), CONFIG_FILENAME)
    loader = ConfigFileLoader(config_abs_path, CONFIG_PROFILE_KEY)
    return loader[ConfigKey.GOOGLE_SERVICE_ACC_KEY_FILEPATH]

def get_gcs_object_list(bucket_id: str, prefix: str, creds_filepath: str) -> List[str]:
    """Returns a list of GCS object names matching the prefix."""
    try:
        client = storage.Client.from_service_account_json(creds_filepath)
        bucket_obj = client.get_bucket(bucket_id)
        blobs = list(bucket_obj.list_blobs(prefix=prefix))
        # Ensure we only get files, not potential directory markers
        file_keys = [blob.name for blob in blobs if blob.name.endswith('.parquet') and not blob.name.endswith('/')]
        print(f"Discovered {len(file_keys)} Parquet files for prefix '{prefix}' in bucket '{bucket_id}'.")
        return file_keys
    except Exception as e:
        print(f"Error listing objects in GCS bucket '{bucket_id}' with prefix '{prefix}': {e}")
        raise

@data_loader
def load_location_dimension_from_storage(**kwargs: Dict[str, Any]) -> Optional[pd.DataFrame]:
    """
    Fetches and combines partitioned location dimension Parquet files from GCS.
    Uses 'io_config.yaml' for GCS connection details.
    Returns a pandas DataFrame or None if loading fails.
    """
    gcs_key_path = retrieve_gcs_credentials_filepath()
    
    location_files = get_gcs_object_list(
        GCS_DATA_BUCKET, 
        LOCATION_DIM_PATH_PREFIX, 
        gcs_key_path
    )

    if not location_files:
        print(f"No location dimension files found at gs://{GCS_DATA_BUCKET}/{LOCATION_DIM_PATH_PREFIX}. Cannot proceed.")
        return None

    # Initialize GCS reader via Mage configuration
    config_abs_path = os.path.join(get_repo_path(), CONFIG_FILENAME)
    gcs_reader_instance = GoogleCloudStorage.with_config(ConfigFileLoader(config_abs_path, CONFIG_PROFILE_KEY))
    
    # Load individual Parquet files
    df_list = []
    for file_key in location_files:
        print(f"Attempting to load: gs://{GCS_DATA_BUCKET}/{file_key}")
        try:
            df_part = gcs_reader_instance.load(GCS_DATA_BUCKET, file_key)
            df_list.append(df_part)
        except Exception as e:
            print(f"Could not load file {file_key}: {e}. This part will be skipped.")

    if not df_list:
        print("No location data parts were successfully loaded. Returning None.")
        return None

    # Concatenate into a single DataFrame
    print(f"Combining {len(df_list)} location data parts...")
    location_dimension_full = pd.concat(df_list, ignore_index=True)
    print(f"Location dimension data loaded successfully. Final dimensions: {location_dimension_full.shape}")
    
    return location_dimension_full

@test
def test_data_loading(output_data: Optional[pd.DataFrame], *args) -> None:
    """
    Basic validation for the loaded location dimension DataFrame.
    """
    assert output_data is not None, "The data loading process resulted in None."
    assert not output_data.empty, "The loaded location DataFrame should not be empty."
    # Example: Check for essential columns
    # required_cols = ['Order City', 'Latitude', 'Longitude']
    # for col in required_cols:
    #     assert col in output_data.columns, f"Missing required column: {col}"
