import pyspark.sql.types as T
from typing import Dict, Any

class KafkaConfig:
    """Kafka configuration settings"""
    BOOTSTRAP_SERVERS = 'kafka:29092'
    TOPIC = 'supply_chain_data'
    GROUP_ID = 'swe5003'
    AUTO_OFFSET_RESET = 'earliest'

class DatabaseConfig:
    """Database configuration settings"""
    HOST = 'localhost'
    PORT = 3306
    DATABASE = 'supply_chain_dataset'
    USERNAME = 'root'
    PASSWORD = 'password'
    
    @property
    def URL(self) -> str:
        return f"mysql+pymysql://{self.USERNAME}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}"

class SchemaConfig:
    """Data schema configuration"""
    @staticmethod
    def get_schema() -> T.StructType:
        """Get the schema definition for the supply chain data"""
        return T.StructType([
            T.StructField("Type", T.StringType()),
            T.StructField("Days_for_shipping_real", T.FloatType()),
            T.StructField("Days_for_shipment_scheduled", T.FloatType()),
            T.StructField("Benefit_per_order", T.FloatType()),
            T.StructField("Sales_per_customer", T.FloatType()),
            T.StructField("Delivery_Status", T.StringType()),
            T.StructField("Late_delivery_risk", T.IntegerType()),
            T.StructField("Category_Id", T.IntegerType()),
            T.StructField("Category_Name", T.StringType()),
            T.StructField("Customer_City", T.StringType()),
            T.StructField("Customer_Country", T.StringType()),
            T.StructField("Customer_Email", T.StringType()),
            T.StructField("Customer_Fname", T.StringType()),
            T.StructField("Customer_Id", T.IntegerType()),
            T.StructField("Customer_Lname", T.StringType()),
            T.StructField("Customer_Password", T.StringType()),
            T.StructField("Customer_Segment", T.StringType()),
            T.StructField("Customer_State", T.StringType()),
            T.StructField("Customer_Street", T.StringType()),
            T.StructField("Customer_Zipcode", T.StringType()),
            T.StructField("Department_Id", T.IntegerType()),
            T.StructField("Department_Name", T.StringType()),
            T.StructField("Latitude", T.FloatType()),
            T.StructField("Longitude", T.FloatType()),
            T.StructField("Market", T.StringType()),
            T.StructField("Order_City", T.StringType()),
            T.StructField("Order_Country", T.StringType()),
            T.StructField("Order_Customer_Id", T.IntegerType()),
            T.StructField("Order_date", T.TimestampType()),
            T.StructField("Order_Id", T.IntegerType()),
            T.StructField("Order_Item_Cardprod_Id", T.IntegerType()),
            T.StructField("Order_Item_Discount", T.FloatType()),
            T.StructField("Order_Item_Discount_Rate", T.FloatType()),
            T.StructField("Order_Item_Id", T.IntegerType()),
            T.StructField("Order_Item_Product_Price", T.FloatType()),
            T.StructField("Order_Item_Profit_Ratio", T.FloatType()),
            T.StructField("Order_Item_Quantity", T.IntegerType()),
            T.StructField("Sales", T.FloatType()),
            T.StructField("Order_Item_Total", T.FloatType()),
            T.StructField("Order_Profit_Per_Order", T.FloatType()),
            T.StructField("Order_Region", T.StringType()),
            T.StructField("Order_State", T.StringType()),
            T.StructField("Order_Status", T.StringType()),
            T.StructField("Product_Card_Id", T.IntegerType()),
            T.StructField("Product_Category_Id", T.IntegerType()),
            T.StructField("Product_Description", T.StringType()),
            T.StructField("Product_Image", T.StringType()),
            T.StructField("Product_Name", T.StringType()),
            T.StructField("Product_Price", T.FloatType()),
            T.StructField("Product_Status", T.IntegerType()),
            T.StructField("Shipping_date", T.TimestampType()),
            T.StructField("Shipping_Mode", T.StringType())
        ])

class Config:
    """Main configuration class that combines all configs"""
    kafka = KafkaConfig()
    database = DatabaseConfig()
    schema = SchemaConfig()

    @staticmethod
    def get_kafka_config() -> Dict[str, Any]:
        """Get Kafka configuration as a dictionary"""
        return {
            'bootstrap.servers': Config.kafka.BOOTSTRAP_SERVERS,
            'group.id': Config.kafka.GROUP_ID,
            'auto.offset.reset': Config.kafka.AUTO_OFFSET_RESET
        }

    @staticmethod
    def get_database_config() -> Dict[str, Any]:
        """Get database configuration as a dictionary"""
        return {
            'host': Config.database.HOST,
            'port': Config.database.PORT,
            'database': Config.database.DATABASE,
            'username': Config.database.USERNAME,
            'password': Config.database.PASSWORD
        }

# For backward compatibility
BOOTSTRAP_SERVERS = Config.kafka.BOOTSTRAP_SERVERS
TOPIC = Config.kafka.TOPIC
STREAM_SCHEMA = Config.schema.get_schema()
