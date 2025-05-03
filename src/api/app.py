from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
import threading
import logging
from typing import Dict, Any
from .data_generators.product_generator import ProductGenerator
from .data_generators.customer_generator import CustomerGenerator
from .data_generators.access_generator import AccessRecordGenerator
from .kafka_producer import KafkaDataProducer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="MercaForesight Data Generator API")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize generators and producer
product_generator = ProductGenerator()
customer_generator = CustomerGenerator()
access_generator = AccessRecordGenerator()
kafka_producer = KafkaDataProducer()

# Store active streams
active_streams: Dict[str, bool] = {
    'products': False,
    'customers': False,
    'access_records': False
}

def start_stream(topic: str, generator) -> None:
    """Start streaming data to Kafka"""
    try:
        kafka_producer.send_data(topic, generator)
    except Exception as e:
        logger.error(f"Error in {topic} stream: {e}")
        active_streams[topic] = False

@app.post("/start/{stream_type}")
async def start_data_stream(stream_type: str, background_tasks: BackgroundTasks):
    """Start a specific data stream"""
    if stream_type not in active_streams:
        return {"error": f"Invalid stream type: {stream_type}"}
    
    if active_streams[stream_type]:
        return {"message": f"{stream_type} stream is already running"}
    
    # Start the appropriate stream
    if stream_type == 'products':
        background_tasks.add_task(start_stream, 'products', product_generator.generate_product_updates())
    elif stream_type == 'customers':
        background_tasks.add_task(start_stream, 'customers', customer_generator.generate_customer_updates())
    elif stream_type == 'access_records':
        background_tasks.add_task(start_stream, 'access_records', access_generator.generate_access_records())
    
    active_streams[stream_type] = True
    return {"message": f"Started {stream_type} stream"}

@app.post("/stop/{stream_type}")
async def stop_data_stream(stream_type: str):
    """Stop a specific data stream"""
    if stream_type not in active_streams:
        return {"error": f"Invalid stream type: {stream_type}"}
    
    active_streams[stream_type] = False
    return {"message": f"Stopped {stream_type} stream"}

@app.get("/status")
async def get_stream_status():
    """Get the status of all streams"""
    return active_streams

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    kafka_producer.close() 