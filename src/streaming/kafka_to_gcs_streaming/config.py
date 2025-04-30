from dataclasses import dataclass
from typing import Optional
import os

@dataclass
class KafkaConfig:
    """Kafka configuration settings"""
    connector_type: str = "kafka"
    bootstrap_server: str = "kafka:29092"
    topic: str = "supply_chain_data"
    consumer_group: str = "mage_consumer_group"
    include_metadata: bool = True
    api_version: str = "0.10.2"

@dataclass
class GCSConfig:
    """Google Cloud Storage configuration settings"""
    connector_type: str = "google_cloud_storage"
    bucket: str = "swe5003"
    prefix: str = "raw_streaming"
    file_type: str = "parquet"
    buffer_size_mb: int = 5
    buffer_timeout_seconds: int = 300
    date_partition_format: str = "%Y%m%dT%H"
    
    @property
    def credentials_path(self) -> str:
        """Get the path to the credentials file"""
        return os.path.join(os.path.dirname(__file__), "google-cred.json")

@dataclass
class PipelineConfig:
    """Pipeline configuration settings"""
    name: str = "kafka_to_gcs_streaming"
    type: str = "streaming"
    cache_block_output_in_memory: bool = False
    executor_count: int = 1
    run_pipeline_in_one_process: bool = False

class Config:
    """Main configuration class"""
    kafka = KafkaConfig()
    gcs = GCSConfig()
    pipeline = PipelineConfig()

    @staticmethod
    def get_kafka_config() -> dict:
        """Get Kafka configuration as a dictionary"""
        return {
            "connector_type": Config.kafka.connector_type,
            "bootstrap_server": Config.kafka.bootstrap_server,
            "topic": Config.kafka.topic,
            "consumer_group": Config.kafka.consumer_group,
            "include_metadata": Config.kafka.include_metadata,
            "api_version": Config.kafka.api_version
        }

    @staticmethod
    def get_gcs_config() -> dict:
        """Get GCS configuration as a dictionary"""
        return {
            "connector_type": Config.gcs.connector_type,
            "bucket": Config.gcs.bucket,
            "prefix": Config.gcs.prefix,
            "file_type": Config.gcs.file_type,
            "buffer_size_mb": Config.gcs.buffer_size_mb,
            "buffer_timeout_seconds": Config.gcs.buffer_timeout_seconds,
            "path_to_credentials_json_file": Config.gcs.credentials_path,
            "date_partition_format": Config.gcs.date_partition_format
        }

    @staticmethod
    def get_pipeline_config() -> dict:
        """Get pipeline configuration as a dictionary"""
        return {
            "name": Config.pipeline.name,
            "type": Config.pipeline.type,
            "cache_block_output_in_memory": Config.pipeline.cache_block_output_in_memory,
            "executor_count": Config.pipeline.executor_count,
            "run_pipeline_in_one_process": Config.pipeline.run_pipeline_in_one_process
        } 