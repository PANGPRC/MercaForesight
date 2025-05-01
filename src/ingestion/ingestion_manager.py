from typing import Optional, Dict, Any
import logging
from datetime import datetime
import os
from pathlib import Path
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from kafka import KafkaProducer
import json
import mysql.connector
from mysql.connector import Error
from config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class IngestionManager:
    """Manages data ingestion from various sources"""
    
    def __init__(self, spark: Optional[SparkSession] = None):
        self.spark = spark or self._create_spark_session()
        self.kafka_producer = self._create_kafka_producer()
        self.mysql_conn = None

    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session"""
        return SparkSession.builder \
            .appName("MercaForesightIngestion") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1") \
            .getOrCreate()

    def _create_kafka_producer(self) -> KafkaProducer:
        """Create Kafka producer for streaming data"""
        return KafkaProducer(
            bootstrap_servers=Config.kafka.BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def _connect_mysql(self) -> None:
        """Establish MySQL connection"""
        try:
            self.mysql_conn = mysql.connector.connect(
                host=Config.database.HOST,
                port=Config.database.PORT,
                database=Config.database.DATABASE,
                user=Config.database.USERNAME,
                password=Config.database.PASSWORD
            )
        except Error as e:
            logger.error(f"Error connecting to MySQL: {e}")
            raise

    def ingest_batch_data(self, file_path: str, schema: StructType) -> None:
        """Ingest batch data from CSV files"""
        try:
            # Read CSV file
            df = self.spark.read \
                .option("header", "true") \
                .schema(schema) \
                .csv(file_path)

            # Write to GCS in Parquet format
            output_path = f"gs://{Config.gcs.BUCKET_NAME}/raw_data/{datetime.now().strftime('%Y%m%d')}/"
            df.write.parquet(output_path, mode="overwrite")
            
            logger.info(f"Successfully ingested batch data to {output_path}")
        except Exception as e:
            logger.error(f"Error ingesting batch data: {e}")
            raise

    def ingest_streaming_data(self, data: Dict[str, Any]) -> None:
        """Ingest streaming data through Kafka"""
        try:
            # Send data to Kafka topic
            self.kafka_producer.send(Config.kafka.TOPIC, value=data)
            self.kafka_producer.flush()
            
            logger.info(f"Successfully sent data to Kafka topic: {Config.kafka.TOPIC}")
        except Exception as e:
            logger.error(f"Error ingesting streaming data: {e}")
            raise

    def ingest_mysql_cdc(self, table_name: str) -> None:
        """Ingest MySQL CDC data using Debezium"""
        try:
            if not self.mysql_conn:
                self._connect_mysql()

            cursor = self.mysql_conn.cursor(dictionary=True)
            cursor.execute(f"SELECT * FROM {table_name}")
            
            for row in cursor:
                # Convert datetime objects to strings
                for key, value in row.items():
                    if isinstance(value, datetime):
                        row[key] = value.isoformat()
                
                # Send to Kafka
                self.ingest_streaming_data(row)
            
            cursor.close()
            logger.info(f"Successfully ingested CDC data from {table_name}")
        except Error as e:
            logger.error(f"Error ingesting MySQL CDC data: {e}")
            raise

    def close(self) -> None:
        """Close all connections"""
        if self.kafka_producer:
            self.kafka_producer.close()
        if self.mysql_conn:
            self.mysql_conn.close()
        if self.spark:
            self.spark.stop()

def main():
    """Example usage of the IngestionManager"""
    try:
        # Initialize ingestion manager
        manager = IngestionManager()

        # Ingest batch data
        batch_file = "data/raw/DataCoSupplyChainDataset.csv"
        manager.ingest_batch_data(batch_file, Config.schema.get_schema())

        # Ingest streaming data
        sample_data = {
            "order_id": "12345",
            "customer_id": "67890",
            "timestamp": datetime.now().isoformat(),
            "amount": 99.99
        }
        manager.ingest_streaming_data(sample_data)

        # Ingest MySQL CDC data
        manager.ingest_mysql_cdc("orders")

    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise
    finally:
        manager.close()

if __name__ == "__main__":
    main() 