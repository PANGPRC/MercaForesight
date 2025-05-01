import os
from pandas import DataFrame
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from typing import Dict, Any

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

# --- Constants for BigQuery Export --- 
BQ_CONFIG_FILENAME = 'io_config.yaml'
BQ_CONFIG_PROFILE_NAME = 'default'
BQ_TARGET_PRODUCT_TABLE = 'arctic-surf-456413-f0.terraform_bigquery.dim_product'
BQ_WRITE_MODE = 'replace'  # Determines action if table exists: 'replace', 'append', 'fail'

@data_exporter
def send_product_data_to_bigquery(product_dataframe: DataFrame, **execution_context: Dict[str, Any]) -> None:
    """
    Uploads the product dimension DataFrame to the designated BigQuery table.
    Reads connection details from the Mage 'io_config.yaml' file.
    Current configuration replaces the table if it already exists.
    """
    
    if product_dataframe is None or product_dataframe.empty:
        print("Product DataFrame is empty or None. Halting BigQuery export process.")
        return

    print(f"Initiating data export to BigQuery table: {BQ_TARGET_PRODUCT_TABLE}")
    print(f"Table write mode: {BQ_WRITE_MODE}")

    # Determine the absolute path to the configuration file
    mage_config_path = os.path.join(get_repo_path(), BQ_CONFIG_FILENAME)
    
    # Instantiate the configuration loader for BigQuery
    bq_configuration = ConfigFileLoader(mage_config_path, BQ_CONFIG_PROFILE_NAME)

    # Execute the export using the BigQuery Mage block
    try:
        BigQuery.with_config(bq_configuration).export(
            df=product_dataframe,                # Data to upload
            table_id=BQ_TARGET_PRODUCT_TABLE,    # Destination table ID
            if_exists=BQ_WRITE_MODE,             # How to handle existing table
        )
        print(f"Data successfully written to BigQuery table: {BQ_TARGET_PRODUCT_TABLE}")
    except Exception as error:
        print(f"Encountered an error during BigQuery export: {error}")
        # Consider re-raising the error if pipeline failure is desired
        # raise error
