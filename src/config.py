import os
from typing import Dict, Any
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

class Config:
    """Configuration settings for the MercaForesight platform"""
    
    class Kafka:
        """Kafka configuration"""
        BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        TOPIC = os.getenv('KAFKA_TOPIC', 'supply_chain_data')
        GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'merca_foresight')
        AUTO_OFFSET_RESET = os.getenv('KAFKA_AUTO_OFFSET_RESET', 'earliest')

    class Database:
        """Database configuration"""
        HOST = os.getenv('DB_HOST', 'localhost')
        PORT = int(os.getenv('DB_PORT', '3306'))
        DATABASE = os.getenv('DB_NAME', 'supply_chain')
        USERNAME = os.getenv('DB_USER', 'root')
        PASSWORD = os.getenv('DB_PASSWORD', 'password')

    class GCS:
        """Google Cloud Storage configuration"""
        BUCKET_NAME = os.getenv('GCS_BUCKET_NAME', 'merca-foresight-data')
        CREDENTIALS_PATH = os.getenv('GCS_CREDENTIALS_PATH', './credentials/gcs-credentials.json')

    class Spark:
        """Spark configuration"""
        MASTER = os.getenv('SPARK_MASTER', 'local[*]')
        APP_NAME = os.getenv('SPARK_APP_NAME', 'MercaForesight')
        EXECUTOR_MEMORY = os.getenv('SPARK_EXECUTOR_MEMORY', '2g')
        DRIVER_MEMORY = os.getenv('SPARK_DRIVER_MEMORY', '1g')

    @staticmethod
    def get_schema() -> StructType:
        """Get the schema for the supply chain data"""
        return StructType([
            StructField("Type", StringType(), True),
            StructField("Days_for_shipping_real", DoubleType(), True),
            StructField("Days_for_shipment_scheduled", DoubleType(), True),
            StructField("Benefit_per_order", DoubleType(), True),
            StructField("Sales_per_customer", DoubleType(), True),
            StructField("Delivery_Status", StringType(), True),
            StructField("Late_delivery_risk", IntegerType(), True),
            StructField("Category_Id", IntegerType(), True),
            StructField("Category_Name", StringType(), True),
            StructField("Customer_City", StringType(), True),
            StructField("Customer_Country", StringType(), True),
            StructField("Customer_Email", StringType(), True),
            StructField("Customer_Fname", StringType(), True),
            StructField("Customer_Id", IntegerType(), True),
            StructField("Customer_Lname", StringType(), True),
            StructField("Customer_Password", StringType(), True),
            StructField("Customer_Segment", StringType(), True),
            StructField("Customer_State", StringType(), True),
            StructField("Customer_Street", StringType(), True),
            StructField("Customer_Zipcode", StringType(), True),
            StructField("Department_Id", IntegerType(), True),
            StructField("Department_Name", StringType(), True),
            StructField("Latitude", DoubleType(), True),
            StructField("Longitude", DoubleType(), True),
            StructField("Market", StringType(), True),
            StructField("Order_City", StringType(), True),
            StructField("Order_Country", StringType(), True),
            StructField("Order_Customer_Id", IntegerType(), True),
            StructField("Order_date", TimestampType(), True),
            StructField("Order_Id", IntegerType(), True),
            StructField("Order_Item_Cardprod_Id", IntegerType(), True),
            StructField("Order_Item_Discount", DoubleType(), True),
            StructField("Order_Item_Discount_Rate", DoubleType(), True),
            StructField("Order_Item_Id", IntegerType(), True),
            StructField("Order_Item_Product_Price", DoubleType(), True),
            StructField("Order_Item_Profit_Ratio", DoubleType(), True),
            StructField("Order_Item_Quantity", IntegerType(), True),
            StructField("Sales", DoubleType(), True),
            StructField("Order_Item_Total", DoubleType(), True),
            StructField("Order_Profit_Per_Order", DoubleType(), True),
            StructField("Order_Region", StringType(), True),
            StructField("Order_State", StringType(), True),
            StructField("Order_Status", StringType(), True),
            StructField("Product_Card_Id", IntegerType(), True),
            StructField("Product_Category_Id", IntegerType(), True),
            StructField("Product_Description", StringType(), True),
            StructField("Product_Image", StringType(), True),
            StructField("Product_Name", StringType(), True),
            StructField("Product_Price", DoubleType(), True),
            StructField("Product_Status", IntegerType(), True),
            StructField("Shipping_date", TimestampType(), True),
            StructField("Shipping_Mode", StringType(), True)
        ])

    @staticmethod
    def get_kafka_config() -> Dict[str, Any]:
        """Get Kafka configuration as a dictionary"""
        return {
            'bootstrap.servers': Config.Kafka.BOOTSTRAP_SERVERS,
            'group.id': Config.Kafka.GROUP_ID,
            'auto.offset.reset': Config.Kafka.AUTO_OFFSET_RESET
        }

    @staticmethod
    def get_database_config() -> Dict[str, Any]:
        """Get database configuration as a dictionary"""
        return {
            'host': Config.Database.HOST,
            'port': Config.Database.PORT,
            'database': Config.Database.DATABASE,
            'username': Config.Database.USERNAME,
            'password': Config.Database.PASSWORD
        }

    @staticmethod
    def get_spark_config() -> Dict[str, Any]:
        """Get Spark configuration as a dictionary"""
        return {
            'master': Config.Spark.MASTER,
            'appName': Config.Spark.APP_NAME,
            'spark.executor.memory': Config.Spark.EXECUTOR_MEMORY,
            'spark.driver.memory': Config.Spark.DRIVER_MEMORY
        }

# For backward compatibility
kafka = Config.Kafka
database = Config.Database
gcs = Config.GCS
spark = Config.Spark 