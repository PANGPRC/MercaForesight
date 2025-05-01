from mage_ai.settings.repo import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from pandas import DataFrame
from os import path
from typing import Dict, Any

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

# Configuration constants
CONFIG_FILE_NAME = 'io_config.yaml'
CONFIG_PROFILE = 'default'
TARGET_TABLE_ID = 'arctic-surf-456413-f0.terraform_bigquery.dim_customer'
WRITE_DISPOSITION = 'replace' # Options: 'append', 'replace', 'fail'

@data_exporter
def export_customer_dimension_to_bq(customer_data: DataFrame, **kwargs: Dict[str, Any]) -> None:
    """
    Exports the provided customer dimension DataFrame to a specified BigQuery table.
    Configuration is loaded from 'io_config.yaml'.
    The existing table will be replaced.
    """
    if customer_data.empty:
        print("Input DataFrame is empty. Skipping export to BigQuery.")
        return

    print(f"Preparing to export customer data to BigQuery table: {TARGET_TABLE_ID}")
    print(f"Write disposition set to: {WRITE_DISPOSITION}")

    # Construct the full path to the configuration file
    config_file_path = path.join(get_repo_path(), CONFIG_FILE_NAME)
    
    # Load the BigQuery configuration profile
    bq_config_loader = ConfigFileLoader(config_file_path, CONFIG_PROFILE)

    try:
        # Initialize BigQuery client with the loaded configuration
        bq_client = BigQuery.with_config(bq_config_loader)
        
        # Perform the export operation
        bq_client.export(
            customer_data,          # The DataFrame to export
            TARGET_TABLE_ID,        # The target BigQuery table ID (project.dataset.table)
            if_exists=WRITE_DISPOSITION, # Action to take if the table already exists
        )
        print(f"Successfully exported data to {TARGET_TABLE_ID}.")
    except Exception as e:
        print(f"Error exporting data to BigQuery: {e}")
        # Depending on requirements, you might want to raise the exception
        # raise e
