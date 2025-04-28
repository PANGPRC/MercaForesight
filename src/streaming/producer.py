from confluent_kafka import Producer
import json
import csv
import time
from typing import Generator, Dict, Any
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
from config import BOOTSTRAP_SERVERS, TOPIC

# Database configuration
DATABASE_URL = 'mysql+pymysql://root:password@localhost:3306/supply_chain_dataset'

def delivery_report(err, msg):
    """Callback for message delivery reports."""
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to topic: {msg.topic()}, partition: {msg.partition()}, offset: {msg.offset()}')

@contextmanager
def get_db_session():
    """Context manager for database sessions."""
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        yield session
    finally:
        session.close()

def read_from_database(query: str) -> Generator[Dict[str, Any], None, None]:
    """Read data from database using the provided query."""
    with get_db_session() as session:
        result = session.execute(text(query))
        for row in result:
            # Convert row to dict and handle any special types
            row_dict = dict(row._mapping)
            # Convert any float values that might be None to 0
            for key, value in row_dict.items():
                if value is None and isinstance(value, (int, float)):
                    row_dict[key] = 0
            yield row_dict

def read_csv(file_path: str) -> Generator[Dict[str, Any], None, None]:
    """Read data from CSV file."""
    with open(file_path, 'r', encoding='utf-8-sig', errors='replace') as file:
        reader = csv.DictReader(file)
        for row in reader:
            yield row

def produce_messages(producer: Producer, topic: str, data_generator: Generator[Dict[str, Any], None, None]):
    """Produce messages to Kafka from a data generator."""
    try:
        for row in data_generator:
            json_row = json.dumps(row)
            producer.produce(topic, value=json_row, callback=delivery_report)
            producer.flush()
    except KeyboardInterrupt:
        print("Producer interrupted by user")
    except Exception as e:
        print(f"Error producing messages: {e}")

def main():
    # Create Kafka producer
    producer = Producer({'bootstrap.servers': BOOTSTRAP_SERVERS})
    
    try:
        # Read from database
        db_query = """
            SELECT 
                Type,
                `Days for shipping (real)`,
                `Days for shipment (scheduled)`,
                `Benefit per order`,
                `Sales per customer`,
                `Delivery Status`,
                `Late_delivery_risk`,
                `Category Id`,
                `Category Name`,
                `Customer City`,
                `Customer Country`,
                `Customer Email`,
                `Customer Fname`,
                `Customer Id`,
                `Customer Lname`,
                `Customer Password`,
                `Customer Segment`,
                `Customer State`,
                `Customer Street`,
                `Customer Zipcode`,
                `Department Id`,
                `Department Name`,
                `Latitude`,
                `Longitude`,
                `Market`,
                `Order City`,
                `Order Country`,
                `Order Customer Id`,
                `order date (DateOrders)`,
                `Order Id`,
                `Order Item Cardprod Id`,
                `Order Item Discount`,
                `Order Item Discount Rate`,
                `Order Item Id`,
                `Order Item Product Price`,
                `Order Item Profit Ratio`,
                `Order Item Quantity`,
                `Sales`,
                `Order Item Total`,
                `Order Profit Per Order`,
                `Order Region`,
                `Order State`,
                `Order Status`,
                `Order Zipcode`,
                `Product Card Id`,
                `Product Category Id`,
                `Product Description`,
                `Product Image`,
                `Product Name`,
                `Product Price`,
                `Product Status`,
                `shipping date (DateOrders)`,
                `Shipping Mode`
            FROM supply_chain_data
            LIMIT 1000
        """
        print("Reading data from database...")
        produce_messages(producer, TOPIC, read_from_database(db_query))
        
    finally:
        producer.flush()

if __name__ == '__main__':
    main()
