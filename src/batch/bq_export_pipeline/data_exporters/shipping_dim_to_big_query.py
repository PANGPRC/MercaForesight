import os
from pandas import DataFrame
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from typing import Dict, Any

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

# --- BigQuery Shipping Dimension Export Configuration ---
CONFIG_FILE_RELATIVE_PATH = 'io_config.yaml'
CONFIG_PROFILE_KEY = 'default'
BQ_SHIPPING_TABLE_FULL_ID = 'arctic-surf-456413-f0.terraform_bigquery.dim_shipping'
# Defines behavior if the target table exists ('replace', 'append', 'fail')
EXISTING_TABLE_POLICY = 'replace' 

@data_exporter
def transfer_shipping_data_to_bq(shipping_details_df: DataFrame, **runtime_params: Dict[str, Any]) -> None:
    """
    Exports the shipping dimension DataFrame to the specified BigQuery table.
    Relies on Mage's configuration management ('io_config.yaml') for credentials.
    Currently set to replace the destination table on each run.
    """
    
    if shipping_details_df is None:
        print("Input shipping DataFrame is None. Halting export to BigQuery.")
        return
    if shipping_details_df.empty:
        print("Input shipping DataFrame is empty. No data will be exported.")
        return

    print(f"Starting export of shipping dimension to BigQuery: {BQ_SHIPPING_TABLE_FULL_ID}")
    print(f"Policy for existing table: {EXISTING_TABLE_POLICY}")

    # Construct the absolute path to the configuration file
    config_absolute_path = os.path.join(get_repo_path(), CONFIG_FILE_RELATIVE_PATH)
    
    # Load the BigQuery configuration profile
    bq_export_config = ConfigFileLoader(config_absolute_path, CONFIG_PROFILE_KEY)

    # Use Mage's BigQuery block for the export operation
    try:
        bq_connector = BigQuery.with_config(bq_export_config)
        bq_connector.export(
            df=shipping_details_df,             # The DataFrame to export
            table_id=BQ_SHIPPING_TABLE_FULL_ID, # Full BigQuery table ID
            if_exists=EXISTING_TABLE_POLICY,    # Action for existing table
        )
        print(f"Successfully exported shipping data to {BQ_SHIPPING_TABLE_FULL_ID}.")
    except Exception as export_err:
        print(f"Error during BigQuery export of shipping data: {export_err}")
        # Consider re-raising the error if this should halt the pipeline
        # raise export_err
