import os
from pandas import DataFrame
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from typing import Dict, Any

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

# --- Parameters for Metadata Export --- 
APP_CONFIG_FILE = 'io_config.yaml'
APP_PROFILE = 'default'
BQ_METADATA_TABLE_ID = 'arctic-surf-456413-f0.terraform_bigquery.dim_metadata'
BQ_TABLE_WRITE_ACTION = 'replace' # Action if table exists: 'replace', 'append', 'fail'

@data_exporter
def store_metadata_in_bq(metadata_input_df: DataFrame, **kwargs: Dict[str, Any]) -> None:
    """
    Transfers the metadata dimension DataFrame to its designated BigQuery table.
    Leverages Mage's configuration system ('io_config.yaml') for BigQuery access.
    Configured to replace the target table upon execution.
    """
    
    if metadata_input_df is None:
        print("Metadata DataFrame is None. BigQuery export will be skipped.")
        return
    if metadata_input_df.empty:
        print("Metadata DataFrame contains no data. Skipping export to BigQuery.")
        return

    print(f"Beginning export process for metadata to BigQuery table: {BQ_METADATA_TABLE_ID}")
    print(f"Action for existing table: {BQ_TABLE_WRITE_ACTION}")

    # Define the full path to the Mage IO configuration
    io_config_location = os.path.join(get_repo_path(), APP_CONFIG_FILE)
    
    # Initialize the BigQuery configuration loader
    bq_loader_settings = ConfigFileLoader(io_config_location, APP_PROFILE)

    # Attempt the BigQuery export operation
    try:
        # Instantiate the BigQuery client via Mage
        bigquery_client = BigQuery.with_config(bq_loader_settings)
        
        # Execute the export
        bigquery_client.export(
            df=metadata_input_df,           # The DataFrame holding metadata
            table_id=BQ_METADATA_TABLE_ID,  # Target table identifier
            if_exists=BQ_TABLE_WRITE_ACTION, # How to handle pre-existing table
        )
        print(f"Metadata successfully written to BigQuery table {BQ_METADATA_TABLE_ID}.")
    except Exception as bq_error:
        print(f"An error occurred while writing metadata to BigQuery: {bq_error}")
        # Consider re-raising the exception for pipeline failure notification
        # raise bq_error
