from typing import Dict, Any
import logging
from google.cloud import bigquery
from google.cloud import storage
from datetime import datetime
import json
from kafka import KafkaConsumer, KafkaProducer
from config import Config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ProductCatalogProcessor:
    """Handles processing and synchronization of product catalog data"""
    
    def __init__(self):
        self.bq_client = bigquery.Client()
        self.storage_client = storage.Client()
        self.kafka_consumer = self._create_kafka_consumer()
        self.kafka_producer = self._create_kafka_producer()
        
    def _create_kafka_consumer(self) -> KafkaConsumer:
        """Create Kafka consumer for product catalog updates"""
        return KafkaConsumer(
            'product_catalog_updates',
            bootstrap_servers=Config.kafka.BOOTSTRAP_SERVERS,
            group_id='product_catalog_processor',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest'
        )
        
    def _create_kafka_producer(self) -> KafkaProducer:
        """Create Kafka producer for processed catalog events"""
        return KafkaProducer(
            bootstrap_servers=Config.kafka.BOOTSTRAP_SERVERS,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        
    def start_streaming(self):
        """Start streaming process for product catalog updates"""
        try:
            logger.info("Starting product catalog stream processing")
            for message in self.kafka_consumer:
                try:
                    catalog_data = message.value
                    
                    # Process the catalog update
                    self.process_catalog_update(catalog_data)
                    
                    # Publish processed event
                    self.publish_catalog_event({
                        'product_id': catalog_data['product_id'],
                        'event_type': 'catalog_updated',
                        'timestamp': datetime.now().isoformat(),
                        'changes': catalog_data.get('changes', {})
                    })
                    
                except Exception as e:
                    logger.error(f"Error processing catalog message: {e}")
                    # Continue processing next message despite error
                    continue
                    
        except Exception as e:
            logger.error(f"Fatal error in catalog stream processing: {e}")
            raise
            
    def publish_catalog_event(self, event: Dict[str, Any]):
        """Publish processed catalog event to Kafka"""
        try:
            self.kafka_producer.send(
                'processed_catalog_events',
                value=event
            )
            logger.info(f"Published catalog event for product {event['product_id']}")
        except Exception as e:
            logger.error(f"Error publishing catalog event: {e}")
            raise
            
    def process_catalog_update(self, catalog_data: Dict[str, Any]) -> None:
        """Process updates to the product catalog"""
        try:
            # Store raw data in GCS
            bucket = self.storage_client.bucket(Config.gcs.BUCKET_NAME)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            blob = bucket.blob(f'product_catalog/raw/catalog_update_{timestamp}.json')
            blob.upload_from_string(json.dumps(catalog_data))
            
            # Insert into BigQuery
            table_id = f"{Config.bigquery.PROJECT}.{Config.bigquery.DATASET}.product_catalog"
            
            rows_to_insert = [{
                'product_id': item['product_id'],
                'name': item['name'],
                'description': item['description'],
                'category': item['category'],
                'price': item['price'],
                'inventory_level': item['inventory_level'],
                'last_updated': datetime.now().isoformat()
            } for item in catalog_data['products']]
            
            errors = self.bq_client.insert_rows_json(table_id, rows_to_insert)
            if errors:
                logger.error(f"Encountered errors while inserting into BigQuery: {errors}")
            
            logger.info(f"Successfully processed {len(rows_to_insert)} product catalog updates")
            
        except Exception as e:
            logger.error(f"Error processing catalog update: {e}")
            raise
            
    def sync_to_postgres(self, catalog_data: Dict[str, Any]) -> None:
        """Sync catalog data to PostgreSQL for operational use"""
        try:
            # Implementation for PostgreSQL sync
            # This would use psycopg2 or SQLAlchemy to sync with PostgreSQL
            pass
            
        except Exception as e:
            logger.error(f"Error syncing to PostgreSQL: {e}")
            raise 