import os
from pandas import DataFrame
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from typing import Dict, Any

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

# --- BigQuery Export Settings --- 
CONFIG_FILE = 'io_config.yaml'
CONFIG_KEY = 'default'
TARGET_BQ_TABLE = 'arctic-surf-456413-f0.terraform_bigquery.dim_order'
TABLE_WRITE_MODE = 'replace' # Options: 'replace', 'append', 'fail'

@data_exporter
def persist_order_data_to_bq(order_data: DataFrame, **context: Dict[str, Any]) -> None:
    """
    Uploads the provided order dimension DataFrame to its designated BigQuery table.
    Connection and authentication details are managed via Mage's 'io_config.yaml'.
    The existing table will be replaced as per the current configuration.
    """
    
    if order_data is None:
        print("Order data input is None. Cannot export to BigQuery.")
        return
    if order_data.empty:
        print("Order DataFrame is empty. Skipping BigQuery export step.")
        return

    print(f"Initiating export of order data to BigQuery table: {TARGET_BQ_TABLE}")
    print(f"Configured write mode: {TABLE_WRITE_MODE}")

    # Construct the absolute path to the configuration file
    mage_config_filepath = os.path.join(get_repo_path(), CONFIG_FILE)
    
    # Load the BigQuery configuration using Mage helpers
    bq_connection_config = ConfigFileLoader(mage_config_filepath, CONFIG_KEY)

    # Execute the data export
    try:
        bq_interface = BigQuery.with_config(bq_connection_config)
        bq_interface.export(
            df=order_data,              # The DataFrame to be uploaded
            table_id=TARGET_BQ_TABLE,   # The full BigQuery table ID
            if_exists=TABLE_WRITE_MODE, # Behavior if table exists
        )
        print(f"Successfully exported order data to {TARGET_BQ_TABLE}.")
    except Exception as upload_error:
        print(f"Error occurred during BigQuery export for order data: {upload_error}")
        # Depending on pipeline requirements, consider re-raising the error
        # raise upload_error
