from typing import Dict, Any
import logging
from google.cloud import bigquery
from google.cloud import storage
from datetime import datetime
import json
import psycopg2
from kafka import KafkaConsumer, KafkaProducer
from config import Config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CustomerProfileProcessor:
    """Handles processing and storage of customer profile data"""
    
    def __init__(self):
        self.bq_client = bigquery.Client()
        self.storage_client = storage.Client()
        self.pg_conn = None
        self.kafka_consumer = self._create_kafka_consumer()
        self.kafka_producer = self._create_kafka_producer()
        
    def _create_kafka_consumer(self) -> KafkaConsumer:
        """Create Kafka consumer for customer profile updates"""
        return KafkaConsumer(
            'customer_profile_updates',
            bootstrap_servers=Config.kafka.BOOTSTRAP_SERVERS,
            group_id='customer_profile_processor',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest'
        )
        
    def _create_kafka_producer(self) -> KafkaProducer:
        """Create Kafka producer for processed profile events"""
        return KafkaProducer(
            bootstrap_servers=Config.kafka.BOOTSTRAP_SERVERS,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        
    def start_streaming(self):
        """Start streaming process for customer profile updates"""
        try:
            logger.info("Starting customer profile stream processing")
            for message in self.kafka_consumer:
                try:
                    profile_data = message.value
                    
                    # Process the profile update
                    self.process_profile_update(profile_data)
                    
                    # Publish processed event
                    self.publish_profile_event({
                        'customer_id': profile_data['customer_id'],
                        'event_type': 'profile_updated',
                        'timestamp': datetime.now().isoformat(),
                        'changes': profile_data.get('changes', {}),
                        'segment': profile_data.get('segment')
                    })
                    
                except Exception as e:
                    logger.error(f"Error processing profile message: {e}")
                    # Continue processing next message despite error
                    continue
                    
        except Exception as e:
            logger.error(f"Fatal error in profile stream processing: {e}")
            raise
            
    def publish_profile_event(self, event: Dict[str, Any]):
        """Publish processed profile event to Kafka"""
        try:
            self.kafka_producer.send(
                'processed_profile_events',
                value=event
            )
            logger.info(f"Published profile event for customer {event['customer_id']}")
        except Exception as e:
            logger.error(f"Error publishing profile event: {e}")
            raise

    def _get_postgres_connection(self):
        """Get or create PostgreSQL connection"""
        if not self.pg_conn or self.pg_conn.closed:
            self.pg_conn = psycopg2.connect(
                host=Config.postgres.HOST,
                database=Config.postgres.DATABASE,
                user=Config.postgres.USERNAME,
                password=Config.postgres.PASSWORD
            )
        return self.pg_conn
        
    def process_profile_update(self, profile_data: Dict[str, Any]) -> None:
        """Process updates to customer profiles"""
        try:
            # Store raw data in GCS
            bucket = self.storage_client.bucket(Config.gcs.BUCKET_NAME)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            blob = bucket.blob(f'customer_profiles/raw/profile_update_{timestamp}.json')
            blob.upload_from_string(json.dumps(profile_data))
            
            # Insert into BigQuery
            table_id = f"{Config.bigquery.PROJECT}.{Config.bigquery.DATASET}.customer_profiles"
            
            rows_to_insert = [{
                'customer_id': profile_data['customer_id'],
                'email': profile_data['email'],
                'first_name': profile_data['first_name'],
                'last_name': profile_data['last_name'],
                'segment': profile_data['segment'],
                'location': {
                    'city': profile_data['city'],
                    'state': profile_data['state'],
                    'country': profile_data['country']
                },
                'last_updated': datetime.now().isoformat()
            }]
            
            errors = self.bq_client.insert_rows_json(table_id, rows_to_insert)
            if errors:
                logger.error(f"Encountered errors while inserting into BigQuery: {errors}")
                
            # Sync to PostgreSQL
            self.sync_to_postgres(profile_data)
            
            logger.info(f"Successfully processed profile update for customer {profile_data['customer_id']}")
            
        except Exception as e:
            logger.error(f"Error processing profile update: {e}")
            raise
            
    def sync_to_postgres(self, profile_data: Dict[str, Any]) -> None:
        """Sync profile data to PostgreSQL"""
        try:
            conn = self._get_postgres_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO customer_profiles (
                        customer_id, email, first_name, last_name,
                        segment, city, state, country, last_updated
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (customer_id) 
                    DO UPDATE SET
                        email = EXCLUDED.email,
                        first_name = EXCLUDED.first_name,
                        last_name = EXCLUDED.last_name,
                        segment = EXCLUDED.segment,
                        city = EXCLUDED.city,
                        state = EXCLUDED.state,
                        country = EXCLUDED.country,
                        last_updated = EXCLUDED.last_updated
                """, (
                    profile_data['customer_id'],
                    profile_data['email'],
                    profile_data['first_name'],
                    profile_data['last_name'],
                    profile_data['segment'],
                    profile_data['city'],
                    profile_data['state'],
                    profile_data['country'],
                    datetime.now()
                ))
                conn.commit()
                
        except Exception as e:
            logger.error(f"Error syncing to PostgreSQL: {e}")
            raise
            
    def close(self):
        """Close all connections"""
        if self.kafka_consumer:
            self.kafka_consumer.close()
        if self.kafka_producer:
            self.kafka_producer.close()
        if self.pg_conn:
            self.pg_conn.close() 