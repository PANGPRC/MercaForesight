from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.functions import from_json, col
import os
import logging
from typing import Optional, Callable
from config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SparkConfig:
    """Spark configuration settings"""
    def __init__(self):
        self.credentials_location = './docker/mage/google-cred.json'
        self.gcs_bucket_path = "gs://de-zoomcamp-project-data/"
        
        # Set environment variables
        os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.apache.spark:spark-avro_2.12:3.3.1 pyspark-shell'
        
        # Create Spark configuration
        self.conf = SparkConf() \
            .setMaster('local[*]') \
            .setAppName('supply_chain_streaming') \
            .set("spark.jars", "./data/gcs-connector-hadoop3-2.2.5.jar") \
            .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
            .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", self.credentials_location)

class DataStreamer:
    """Main class for streaming data processing"""
    def __init__(self):
        self.spark_config = SparkConfig()
        self.sc = SparkContext(conf=self.spark_config.conf)
        self._setup_hadoop_config()
        self.spark = self._create_spark_session()
        self.queries = {}

    def _setup_hadoop_config(self):
        """Setup Hadoop configuration for GCS"""
        hadoop_conf = self.sc._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", self.spark_config.credentials_location)
        hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

    def _create_spark_session(self) -> SparkSession:
        """Create and return a Spark session"""
        return SparkSession.builder \
            .config(conf=self.sc.getConf()) \
            .getOrCreate()

    def create_stream(self) -> None:
        """Create the initial data stream from Kafka"""
        self.df_raw_stream = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", Config.kafka.BOOTSTRAP_SERVERS) \
            .option("subscribe", Config.kafka.TOPIC) \
            .option("startingOffsets", "earliest") \
            .option("checkpointLocation", "checkpoint") \
            .load()

        # Convert binary value to string and parse JSON
        self.df_raw_stream = self.df_raw_stream.withColumn(
            "value_str", 
            self.df_raw_stream["value"].cast("string")
        )

        # Apply schema and create structured DataFrame
        self.df_with_json = self.df_raw_stream.select(
            "*",
            from_json(col("value_str"), Config.schema.get_schema()).alias("value_json")
        ).select(
            "*",
            "value_json.*"
        ).drop("value_str", "value_json")

    def write_to_gcs(self, df, batch_id: int, path: str) -> None:
        """Write DataFrame batch to GCS"""
        output_path = f"{self.spark_config.gcs_bucket_path}streaming_data/{path}/batch_{batch_id}/"
        df.write.format("parquet").mode("overwrite").save(output_path)
        logger.info(f"Written batch {batch_id} to {output_path}")

    def start_streaming_query(
        self, 
        name: str, 
        df: Optional[Callable] = None,
        path: str = "default"
    ) -> None:
        """Start a new streaming query"""
        if df is None:
            df = self.df_with_json

        query = df.writeStream \
            .foreachBatch(lambda df, batch_id: self.write_to_gcs(df, batch_id, path)) \
            .start()
        
        self.queries[name] = query
        logger.info(f"Started streaming query: {name}")

    def stop_query(self, name: Optional[str] = None) -> None:
        """Stop one or all streaming queries"""
        if name is None:
            # Stop all queries
            for query_name, query in self.queries.items():
                query.stop()
                logger.info(f"Stopped query: {query_name}")
            self.queries.clear()
        elif name in self.queries:
            # Stop specific query
            self.queries[name].stop()
            del self.queries[name]
            logger.info(f"Stopped query: {name}")
        else:
            logger.warning(f"Query {name} not found")

    def await_termination(self) -> None:
        """Wait for all streaming queries to terminate"""
        try:
            for query in self.queries.values():
                query.awaitTermination()
        except KeyboardInterrupt:
            logger.info("Streaming stopped by user")
            self.stop_query()

def main():
    try:
        # Create and configure streamer
        streamer = DataStreamer()
        streamer.create_stream()

        # Start fact scores query
        streamer.start_streaming_query(
            "fact_scores",
            streamer.df_with_json.select('email', 'module_id', 'score', 'timestamp', 'offset'),
            'fact_score'
        )

        # Start fact time query
        streamer.start_streaming_query(
            "fact_time",
            streamer.df_with_json.select('email', 'module_id', 'time_homework', 'time_lectures', 'timestamp', 'offset'),
            'fact_time'
        )

        # Wait for queries to complete
        streamer.await_termination()

    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise

if __name__ == '__main__':
    main()