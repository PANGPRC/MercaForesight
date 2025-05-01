from pyspark.sql import SparkSession
from pyspark.sql.functions import window, avg, sum, count, col, from_json, to_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType, StructField
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
    
    def __init__(self, spark: Optional[SparkSession] = None):
        self.spark = spark or self._create_spark_session()
        self.queries = {}
        self._setup_streaming_schema()

    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session for streaming"""
        return SparkSession.builder \
            .appName("MercaForesightStreaming") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1") \
            .getOrCreate()

    def _setup_streaming_schema(self) -> None:
        """Define schemas for different streaming data types"""
        self.schemas = {
            'orders': StructType([
                StructField("order_id", StringType(), True),
                StructField("customer_id", StringType(), True),
                StructField("order_date", TimestampType(), True),
                StructField("total_amount", DoubleType(), True),
                StructField("status", StringType(), True)
            ]),
            'inventory': StructType([
                StructField("product_id", StringType(), True),
                StructField("quantity", DoubleType(), True),
                StructField("timestamp", TimestampType(), True),
                StructField("warehouse_id", StringType(), True)
            ]),
            'pricing': StructType([
                StructField("product_id", StringType(), True),
                StructField("price", DoubleType(), True),
                StructField("timestamp", TimestampType(), True),
                StructField("market", StringType(), True)
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

    def process_orders(self) -> None:
        """Process order data with real-time analytics"""
        try:
            orders_df = self.streams['orders']
            
            # Calculate real-time metrics
            metrics = orders_df \
                .withWatermark("order_date", "1 hour") \
                .groupBy(
                    window("order_date", "5 minutes"),
                    "status"
                ) \
                .agg(
                    count("order_id").alias("order_count"),
                    sum("total_amount").alias("total_sales"),
                    avg("total_amount").alias("avg_order_value")
                )

            # Write to GCS
            query = metrics.writeStream \
                .format("parquet") \
                .option("path", f"gs://{Config.gcs.BUCKET_NAME}/streaming/orders/") \
                .option("checkpointLocation", "checkpoints/orders") \
                .outputMode("append") \
                .start()

            self.queries['orders'] = query
            logger.info("Started order processing stream")
        except Exception as e:
            logger.error(f"Error processing orders: {e}")
            raise

    def process_inventory(self) -> None:
        """Process inventory data with real-time analytics"""
        try:
            inventory_df = self.streams['inventory']
            
            # Calculate inventory metrics
            metrics = inventory_df \
                .withWatermark("timestamp", "1 hour") \
                .groupBy(
                    window("timestamp", "15 minutes"),
                    "warehouse_id",
                    "product_id"
                ) \
                .agg(
                    sum("quantity").alias("total_quantity"),
                    count("product_id").alias("update_count")
                )

            # Write to GCS
            query = metrics.writeStream \
                .format("parquet") \
                .option("path", f"gs://{Config.gcs.BUCKET_NAME}/streaming/inventory/") \
                .option("checkpointLocation", "checkpoints/inventory") \
                .outputMode("append") \
                .start()

            self.queries['inventory'] = query
            logger.info("Started inventory processing stream")
        except Exception as e:
            logger.error(f"Error processing inventory: {e}")
            raise

    def process_pricing(self) -> None:
        """Process pricing data with real-time analytics"""
        try:
            pricing_df = self.streams['pricing']
            
            # Calculate pricing metrics
            metrics = pricing_df \
                .withWatermark("timestamp", "1 hour") \
                .groupBy(
                    window("timestamp", "5 minutes"),
                    "product_id",
                    "market"
                ) \
                .agg(
                    avg("price").alias("avg_price"),
                    count("product_id").alias("price_updates")
                )

            # Write to GCS
            query = metrics.writeStream \
                .format("parquet") \
                .option("path", f"gs://{Config.gcs.BUCKET_NAME}/streaming/pricing/") \
                .option("checkpointLocation", "checkpoints/pricing") \
                .outputMode("append") \
                .start()

            self.queries['pricing'] = query
            logger.info("Started pricing processing stream")
        except Exception as e:
            logger.error(f"Error processing pricing: {e}")
            raise

    def start_all_streams(self) -> None:
        """Start all streaming processes"""
        try:
            self.process_orders()
            self.process_inventory()
            self.process_pricing()
            
            # Wait for all streams to terminate
            for query in self.queries.values():
                query.awaitTermination()
        except Exception as e:
            logger.error(f"Error in streaming processes: {e}")
            raise

    def stop_all_streams(self) -> None:
        """Stop all streaming processes"""
        for name, query in self.queries.items():
            query.stop()
            logger.info(f"Stopped stream: {name}")

def main():
    """Example usage of the StreamingProcessor"""
    try:
        processor = StreamingProcessor()
        
        # Create streams
        processor.create_stream("orders", "orders")
        processor.create_stream("inventory", "inventory")
        processor.create_stream("pricing", "pricing")
        
        # Start processing
        processor.start_all_streams()
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise
    finally:
        processor.stop_all_streams()

if __name__ == "__main__":
    main() 