from pyspark.sql import SparkSession
from pyspark.sql.functions import window, avg, sum, count, col, from_json, to_timestamp, when, countDistinct
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType, StructField, IntegerType
import logging
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
from config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class StreamingProcessor:
    """Processes streaming data with real-time analytics"""
    
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("MercaForesightStreaming") \
            .getOrCreate()
        self.streams = {}
        self._setup_streaming_schema()

    def _setup_streaming_schema(self) -> None:
        """Define schemas for different streaming data types"""
        self.schemas = {
            'products': StructType([
                StructField("product_id", IntegerType(), True),
                StructField("category_id", IntegerType(), True),
                StructField("category_name", StringType(), True),
                StructField("description", StringType(), True),
                StructField("image_url", StringType(), True),
                StructField("product_name", StringType(), True),
                StructField("product_price", DoubleType(), True),
                StructField("product_status", StringType(), True),
                StructField("last_updated", TimestampType(), True)
            ]),
            'customers': StructType([
                StructField("customer_id", IntegerType(), True),
                StructField("email", StringType(), True),
                StructField("first_name", StringType(), True),
                StructField("last_name", StringType(), True),
                StructField("segment", StringType(), True),
                StructField("city", StringType(), True),
                StructField("country", StringType(), True),
                StructField("state", StringType(), True),
                StructField("street", StringType(), True),
                StructField("zipcode", StringType(), True),
                StructField("last_updated", TimestampType(), True)
            ]),
            'access_records': StructType([
                StructField("record_id", IntegerType(), True),
                StructField("customer_id", IntegerType(), True),
                StructField("product_id", IntegerType(), True),
                StructField("access_time", TimestampType(), True),
                StructField("access_type", StringType(), True),
                StructField("session_duration", IntegerType(), True),
                StructField("device_type", StringType(), True)
            ])
        }

    def create_stream(self, topic: str, schema_name: str) -> None:
        """Create a streaming DataFrame from Kafka"""
        try:
            df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", Config.kafka.BOOTSTRAP_SERVERS) \
                .option("subscribe", topic) \
                .option("startingOffsets", "earliest") \
                .load()

            # Parse JSON and apply schema
            df = df.select(
                from_json(col("value").cast("string"), self.schemas[schema_name]).alias("data")
            ).select("data.*")

            self.streams[topic] = df
            logger.info(f"Created stream for topic: {topic}")
        except Exception as e:
            logger.error(f"Error creating stream: {e}")
            raise

    def process_stream(self, topic: str) -> None:
        """Process the streaming data with real-time analytics"""
        try:
            df = self.streams[topic]
            
            if topic == 'products':
                # Product analytics
                df = df.withWatermark("last_updated", "1 minute") \
                    .groupBy("category_id", "category_name") \
                    .agg(
                        count("product_id").alias("product_count"),
                        avg("product_price").alias("avg_price"),
                        count(when(col("product_status") == "active", 1)).alias("active_products")
                    )
            
            elif topic == 'customers':
                # Customer analytics
                df = df.withWatermark("last_updated", "1 minute") \
                    .groupBy("segment") \
                    .agg(
                        count("customer_id").alias("customer_count"),
                        countDistinct("city").alias("cities_covered"),
                        countDistinct("country").alias("countries_covered")
                    )
            
            elif topic == 'access_records':
                # Access record analytics
                df = df.withWatermark("access_time", "1 minute") \
                    .groupBy("product_id", "access_type") \
                    .agg(
                        count("record_id").alias("access_count"),
                        avg("session_duration").alias("avg_session_duration"),
                        countDistinct("customer_id").alias("unique_customers")
                    )
            
            # Write to console for demonstration
            query = df.writeStream \
                .outputMode("update") \
                .format("console") \
                .start()
            
            query.awaitTermination()
            
        except Exception as e:
            logger.error(f"Error processing stream: {e}")
            raise

def main():
    """Example usage of the StreamingProcessor"""
    try:
        processor = StreamingProcessor()
        
        # Create streams
        processor.create_stream("products", "products")
        processor.create_stream("customers", "customers")
        processor.create_stream("access_records", "access_records")
        
        # Start processing
        processor.process_stream("products")
        processor.process_stream("customers")
        processor.process_stream("access_records")
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise

if __name__ == "__main__":
    main() 