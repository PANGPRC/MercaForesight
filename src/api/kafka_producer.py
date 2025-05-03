from kafka import KafkaProducer
import json
import logging
from typing import Dict, Any, Generator
from config import Config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaDataProducer:
    """Handles sending data to Kafka topics"""
    
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=Config.kafka.BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topics = {
            'products': 'products',
            'customers': 'customers',
            'access_records': 'access_records'
        }
    
    def send_data(self, topic: str, data_generator: Generator[Dict[str, Any], None, None]) -> None:
        """Send data from a generator to a Kafka topic"""
        try:
            for data in data_generator:
                self.producer.send(self.topics[topic], value=data)
                logger.info(f"Sent data to {topic}: {data}")
        except Exception as e:
            logger.error(f"Error sending data to {topic}: {e}")
            raise
    
    def close(self) -> None:
        """Close the Kafka producer"""
        self.producer.close() 