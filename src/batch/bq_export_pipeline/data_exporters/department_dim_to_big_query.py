import os
from pandas import DataFrame
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from typing import Dict, Any

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

# --- Configuration Settings --- 
MAGE_IO_CONFIG = 'io_config.yaml'
MAGE_PROFILE = 'default'
BIGQUERY_DEPARTMENT_TABLE = 'arctic-surf-456413-f0.terraform_bigquery.dim_department'
TABLE_WRITE_BEHAVIOR = 'replace' # Controls how to handle existing table: 'replace', 'append', 'fail'

@data_exporter
def upload_department_data_to_bq(department_df: DataFrame, **runtime_args: Dict[str, Any]) -> None:
    """
    Writes the department dimension DataFrame to its corresponding BigQuery table.
    Utilizes Mage's BigQuery integration and reads settings from 'io_config.yaml'.
    The table is replaced upon execution based on current settings.
    """
    
    if department_df is None:
        print("Received None DataFrame for department data. Aborting BigQuery export.")
        return
    if department_df.empty:
        print("Department DataFrame is empty. Nothing to export to BigQuery.")
        return

    print(f"Starting export of department data to BigQuery: {BIGQUERY_DEPARTMENT_TABLE}")
    print(f"Table creation/update mode: {TABLE_WRITE_BEHAVIOR}")

    # Resolve the full path for the Mage IO config file
    io_config_full_path = os.path.join(get_repo_path(), MAGE_IO_CONFIG)
    
    # Prepare the BigQuery configuration loader
    bq_loader_config = ConfigFileLoader(io_config_full_path, MAGE_PROFILE)

    # Attempt to export the data
    try:
        bq_writer = BigQuery.with_config(bq_loader_config)
        bq_writer.export(
            df=department_df,                   # The DataFrame containing department data
            table_id=BIGQUERY_DEPARTMENT_TABLE, # Target BigQuery table identifier
            if_exists=TABLE_WRITE_BEHAVIOR,     # Action for existing table
        )
        print(f"Successfully uploaded department data to {BIGQUERY_DEPARTMENT_TABLE}.")
    except Exception as export_exception:
        print(f"An error occurred while exporting department data to BigQuery: {export_exception}")
        # Optionally re-raise to signal pipeline failure
        # raise export_exception
