import logging
from typing import Optional, Dict, Any
from config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class StreamingPipeline:
    """Main class for handling the streaming pipeline"""
    
    def __init__(self):
        self.kafka_config = Config.get_kafka_config()
        self.gcs_config = Config.get_gcs_config()
        self.pipeline_config = Config.get_pipeline_config()
        self.blocks = {
            "consume_from_kafka": {
                "type": "data_loader",
                "language": "yaml",
                "upstream_blocks": [],
                "downstream_blocks": ["kafka_to_gcs"],
                "configuration": self.kafka_config
            },
            "kafka_to_gcs": {
                "type": "data_exporter",
                "language": "yaml",
                "upstream_blocks": ["consume_from_kafka"],
                "downstream_blocks": [],
                "configuration": self.gcs_config
            }
        }

    def get_block_config(self, block_name: str) -> Dict[str, Any]:
        """Get configuration for a specific block"""
        if block_name not in self.blocks:
            raise ValueError(f"Block {block_name} not found")
        return self.blocks[block_name]

    def get_pipeline_metadata(self) -> Dict[str, Any]:
        """Get the complete pipeline metadata"""
        return {
            "name": self.pipeline_config["name"],
            "type": self.pipeline_config["type"],
            "blocks": [
                {
                    "name": name,
                    "type": block["type"],
                    "language": block["language"],
                    "upstream_blocks": block["upstream_blocks"],
                    "downstream_blocks": block["downstream_blocks"],
                    "configuration": block["configuration"]
                }
                for name, block in self.blocks.items()
            ],
            "cache_block_output_in_memory": self.pipeline_config["cache_block_output_in_memory"],
            "executor_count": self.pipeline_config["executor_count"],
            "run_pipeline_in_one_process": self.pipeline_config["run_pipeline_in_one_process"]
        }

    def update_block_config(self, block_name: str, config: Dict[str, Any]) -> None:
        """Update configuration for a specific block"""
        if block_name not in self.blocks:
            raise ValueError(f"Block {block_name} not found")
        self.blocks[block_name]["configuration"].update(config)
        logger.info(f"Updated configuration for block {block_name}")

    def validate_pipeline(self) -> bool:
        """Validate the pipeline configuration"""
        try:
            # Validate Kafka configuration
            required_kafka_fields = ["bootstrap_server", "topic", "consumer_group"]
            for field in required_kafka_fields:
                if not self.kafka_config.get(field):
                    raise ValueError(f"Missing required Kafka configuration: {field}")

            # Validate GCS configuration
            required_gcs_fields = ["bucket", "prefix", "file_type"]
            for field in required_gcs_fields:
                if not self.gcs_config.get(field):
                    raise ValueError(f"Missing required GCS configuration: {field}")

            # Validate block dependencies
            for block_name, block in self.blocks.items():
                for upstream in block["upstream_blocks"]:
                    if upstream not in self.blocks:
                        raise ValueError(f"Upstream block {upstream} not found for block {block_name}")

            return True
        except Exception as e:
            logger.error(f"Pipeline validation failed: {e}")
            return False

def main():
    try:
        # Create and validate pipeline
        pipeline = StreamingPipeline()
        if not pipeline.validate_pipeline():
            raise ValueError("Pipeline validation failed")

        # Get pipeline metadata
        metadata = pipeline.get_pipeline_metadata()
        logger.info("Pipeline configuration is valid")
        logger.info(f"Pipeline metadata: {metadata}")

    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise

if __name__ == '__main__':
    main() 