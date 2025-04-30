from confluent_kafka import Consumer, KafkaError
import json
import logging
from typing import Callable, Optional
from config import BOOTSTRAP_SERVERS, TOPIC

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataConsumer:
    def __init__(
        self,
        bootstrap_servers: str = BOOTSTRAP_SERVERS,
        topic: str = TOPIC,
        group_id: str = "swe5003",
        auto_offset_reset: str = "earliest"
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.auto_offset_reset = auto_offset_reset
        
        # Kafka consumer configuration
        self.conf = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group_id,
            'auto.offset.reset': self.auto_offset_reset
        }
        
        self.consumer = Consumer(self.conf)
        self.consumer.subscribe([self.topic])

    def process_message(self, msg: bytes) -> None:
        """Process a single message. Override this method for custom processing."""
        try:
            message = msg.decode('utf-8')
            logger.info(f'Received message: {message}')
        except Exception as e:
            logger.error(f'Error processing message: {e}')

    def start_consuming(self, message_handler: Optional[Callable[[bytes], None]] = None):
        """Start consuming messages from Kafka."""
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                    
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition
                        continue
                    else:
                        logger.error(f'Consumer error: {msg.error()}')
                        break
                
                # Use custom handler if provided, otherwise use default
                if message_handler:
                    message_handler(msg.value())
                else:
                    self.process_message(msg.value())

        except KeyboardInterrupt:
            logger.info("Consumer stopped by user")
        except Exception as e:
            logger.error(f"Error in consumer: {e}")
        finally:
            self.consumer.close()

def main():
    # Create consumer with default configuration
    consumer = DataConsumer()
    
    # Example of custom message handler
    def custom_handler(msg: bytes):
        try:
            data = json.loads(msg.decode('utf-8'))
            logger.info(f'Processed data: {data}')
        except json.JSONDecodeError:
            logger.error('Invalid JSON message')
        except Exception as e:
            logger.error(f'Error in custom handler: {e}')

    try:
        # Start consuming with custom handler
        consumer.start_consuming(message_handler=custom_handler)
        
        # Or use default handler
        # consumer.start_consuming()
        
    except KeyboardInterrupt:
        logger.info("Consumer stopped by user")
    except Exception as e:
        logger.error(f"Error in main: {e}")

if __name__ == '__main__':
    main()
