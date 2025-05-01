import pyspark
from pyspark.sql import SparkSession, DataFrame
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.functions import col
from typing import List, Dict

# Import refactored configuration and schema definitions
from config import GCSExportSettings # Renamed class
from utils import (
    CUSTOMER_DIM_SCHEMA, PRODUCT_DIM_SCHEMA, LOCATION_DIM_SCHEMA, ORDER_DIM_SCHEMA, 
    SHIPPING_DIM_SCHEMA, DEPARTMENT_DIM_SCHEMA, METADATA_DIM_SCHEMA # Renamed variables
)

def initialize_spark_session(settings: GCSExportSettings) -> SparkSession:
    """Initializes and returns a SparkSession configured for GCS access."""
    print("Initializing Spark session...")
    spark_configuration = SparkConf() \
        .setMaster(settings.SPARK_EXECUTION_MODE) \
        .setAppName(settings.SPARK_APPLICATION_ID) \
        .set("spark.jars", settings.SPARK_GCS_JAR) \
        .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", settings.GCP_CREDENTIALS_FILE) \
        .set("spark.hadoop.google.cloud.project.id", settings.GOOGLE_PROJECT_ID)

    spark_context = SparkContext.getOrCreate(conf=spark_configuration)

    # Set Hadoop configurations for GCS
    hadoop_config = spark_context._jsc.hadoopConfiguration()
    hadoop_config.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    hadoop_config.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    hadoop_config.set("fs.gs.auth.service.account.json.keyfile", settings.GCP_CREDENTIALS_FILE)
    hadoop_config.set("fs.gs.auth.service.account.enable", "true")

    spark_session = SparkSession.builder \
        .config(conf=spark_context.getConf()) \
        .getOrCreate()
    print("Spark session initialized successfully.")
    return spark_session

def select_data_fields(input_df: DataFrame, fields_to_select: List[str]) -> DataFrame:
    """Selects specified fields from the nested 'data' struct."""
    selected_fields = [col("data").getItem(field).alias(field) for field in fields_to_select]
    return input_df.select(*selected_fields)

def select_metadata_fields(input_df: DataFrame, fields_to_select: List[str]) -> DataFrame:
    """Selects specified fields from the nested 'metadata' struct."""
    selected_fields = [col("metadata").getItem(field).alias(field) for field in fields_to_select]
    return input_df.select(*selected_fields)

def apply_column_renames(input_df: DataFrame, rename_definitions: Dict[str, str]) -> DataFrame:
    """Renames DataFrame columns based on the provided dictionary."""
    output_df = input_df
    for current_name, desired_name in rename_definitions.items():
        if current_name in output_df.columns:
            output_df = output_df.withColumnRenamed(current_name, desired_name)
        else:
            print(f"Warning: Column '{current_name}' not found for renaming.")
    return output_df

def save_dataframes_to_gcs(dataframe_map: Dict[str, DataFrame], base_gcs_path: str):
    """Saves a dictionary of DataFrames to GCS as Parquet files."""
    print(f'Initiating export of {len(dataframe_map)} DataFrames to base path: {base_gcs_path}')
    for df_name, df_instance in dataframe_map.items():
        full_destination_path = f"{base_gcs_path}{df_name}.parquet"
        print(f"Writing DataFrame '{df_name}' to: {full_destination_path}")
        try:
            df_instance.write \
                .mode("overwrite") \
                .option("header", "true") \
                .option("compression", "none") \
                .parquet(full_destination_path)
            print(f"Successfully wrote '{df_name}'.")
        except Exception as e:
            print(f"Error writing DataFrame '{df_name}' to {full_destination_path}: {e}")
            # Depending on requirements, might want to raise the exception
    print("All DataFrame exports completed.")

def execute_dimension_export_pipeline():
    """Coordinates the batch pipeline to extract and export dimension tables to GCS."""
    print("Starting GCS dimension export pipeline...")
    # 1. Validate Configuration
    try:
        GCSExportSettings.verify_paths()
    except FileNotFoundError as e:
        print(f"Configuration error: {e}. Aborting pipeline.")
        return

    # 2. Initialize Spark
    spark = initialize_spark_session(GCSExportSettings)

    # 3. Load Source Data
    print(f'Loading source data from: {GCSExportSettings.SOURCE_DATA_PATH_PATTERN}')
    try:
        source_df = spark.read.parquet(GCSExportSettings.SOURCE_DATA_PATH_PATTERN)
        print('Source data loaded successfully. Schema:')
        source_df.printSchema()
    except Exception as e:
        print(f"Error loading source data from GCS: {e}. Aborting pipeline.")
        spark.stop()
        return

    # 4. Create Dimension DataFrames
    print("Extracting dimension data...")
    
    customer_dim = select_data_fields(source_df, CUSTOMER_DIM_SCHEMA)
    print('Extracted Customer dimension.')

    product_dim = select_data_fields(source_df, PRODUCT_DIM_SCHEMA)
    print('Extracted Product dimension.')

    location_dim = select_data_fields(source_df, LOCATION_DIM_SCHEMA)
    print('Extracted Location dimension.')

    order_dim_intermediate = select_data_fields(source_df, ORDER_DIM_SCHEMA)
    order_dim = apply_column_renames(order_dim_intermediate, {"Order date (DateOrders)": "Order date"})
    print('Extracted and renamed Order dimension.')

    shipping_dim_intermediate = select_data_fields(source_df, SHIPPING_DIM_SCHEMA)
    shipping_dim = apply_column_renames(shipping_dim_intermediate, {
        "Shipping date (DateOrders)": "Shipping date",
        "Days for shipping (real)": "Days for shipping real",
        "Days for shipment (scheduled)": "Days for shipping scheduled"
    })
    print('Extracted and renamed Shipping dimension.')

    department_dim = select_data_fields(source_df, DEPARTMENT_DIM_SCHEMA)
    print('Extracted Department dimension.')

    metadata_dim = select_metadata_fields(source_df, METADATA_DIM_SCHEMA)
    print('Extracted Metadata dimension.')

    # 5. Prepare for Export
    dimensions_to_save = {
        "customer_dimension": customer_dim,
        "product_dimension": product_dim,
        "location_dimension": location_dim,
        "order_dimension": order_dim,
        "shipping_dimension": shipping_dim,
        "department_dimension": department_dim,
        "metadata_dimension": metadata_dim
    }

    # 6. Write to GCS
    save_dataframes_to_gcs(dimensions_to_save, GCSExportSettings.DESTINATION_DATA_PATH)

    # 7. Stop Spark Session
    print("Stopping Spark session...")
    spark.stop()
    print("Pipeline execution finished.")

if __name__ == "__main__":
    execute_dimension_export_pipeline()
