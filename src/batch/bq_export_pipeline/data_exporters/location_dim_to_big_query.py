import os
from pandas import DataFrame
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from typing import Dict, Any

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

# --- Export Configuration --- 
IO_CONFIG_PATH = 'io_config.yaml'
IO_PROFILE = 'default'
DESTINATION_TABLE = 'arctic-surf-456413-f0.terraform_bigquery.dim_location'
WRITE_STRATEGY = 'replace' # Strategy for existing table: 'replace', 'append', 'fail'

@data_exporter
def write_location_data_to_bq(location_dataframe: DataFrame, **kwargs: Dict[str, Any]) -> None:
    """
    Exports the location dimension DataFrame to the specified BigQuery table.
    Configuration details are sourced from 'io_config.yaml' within the Mage project.
    The default behavior is to replace the target table if it exists.
    """
    
    if location_dataframe is None:
        print("Input location DataFrame is None. Skipping BigQuery export.")
        return
    if location_dataframe.empty:
        print("Input location DataFrame is empty. No data to export to BigQuery.")
        return

    print(f"Preparing to write location data to BigQuery: {DESTINATION_TABLE}")
    print(f"Write strategy: {WRITE_STRATEGY}")

    # Construct the full path to the IO configuration file
    config_filepath = os.path.join(get_repo_path(), IO_CONFIG_PATH)
    
    # Load the specified configuration profile for BigQuery
    bq_config = ConfigFileLoader(config_filepath, IO_PROFILE)

    # Perform the export operation using Mage's BigQuery connector
    try:
        bq_exporter = BigQuery.with_config(bq_config)
        bq_exporter.export(
            df=location_dataframe,      # DataFrame containing location data
            table_id=DESTINATION_TABLE, # Target BigQuery table (project.dataset.table)
            if_exists=WRITE_STRATEGY,   # Action if table exists
        )
        print(f"Location data successfully exported to {DESTINATION_TABLE}.")
    except Exception as e:
        print(f"Failed to export location data to BigQuery. Error: {e}")
        # Consider raising the exception if pipeline failure is required
        # raise e
