# kafka-pub-sub/producer/producer.py
import json
import time
import logging
import signal
import sys
import random
from datetime import datetime
from typing import Dict, Any
import uuid
from faker import Faker
from kafka import KafkaProducer
from kafka.errors import KafkaError

from config import Config

# Setup logging
logger = Config.setup_logging()

class DataProducer:
    
    def __init__(self):
        self.config = Config()
        self.fake = Faker()
        self.running = True
        self.producer = self._create_producer()
        
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        
        logger.info(f"Producer initialized with bootstrap servers: {self.config.KAFKA_BOOTSTRAP_SERVERS}")
        logger.info(f"Target topic: {self.config.KAFKA_TOPIC}")
    
    def _create_producer(self) -> KafkaProducer:
        try:
            return KafkaProducer(
                bootstrap_servers=self.config.KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: str(k).encode('utf-8'),
                acks='all',  
                retries=3,   
            )
        except KafkaError as e:
            logger.error(f"Failed to create Kafka producer: {e}")
            sys.exit(1)
    
    def _generate_data(self) -> Dict[str, Any]:
        user_ids=['user1', 'user2', 'user3', 'user4', 'user5']
        user_id = random.choice(user_ids)
        return {
            "id": str(uuid.uuid4()),
            "timestamp": datetime.now().isoformat(),
            "user_id": user_id,
        }
    
    def _on_send_success(self, record_metadata):
        logger.debug(f"Message delivered to {record_metadata.topic} "
                    f"[partition={record_metadata.partition}, offset={record_metadata.offset}]")
    
    def _on_send_error(self, excp):
        logger.error(f"Message delivery failed: {excp}")
    
    def start(self):
        logger.info("Starting to produce messages...")
        
        try:
            while self.running:
                # Generate and send data
                data = self._generate_data()
                
                # Use user ID as key for consistent partitioning
                key = data["user_id"]
                
                # Send the message asynchronously with callbacks
                self.producer.send(
                    self.config.KAFKA_TOPIC, 
                    key=key,
                    value=data
                ).add_callback(self._on_send_success).add_errback(self._on_send_error)
                
                # Log the produced message
                logger.info(f"Produced message: {data['id']} for user: {data['user_id'][:8]}...")
                
                # Flush to ensure messages are sent
                self.producer.flush(timeout=1.0)
                
                # Wait for the next interval
                time.sleep(self.config.PRODUCE_INTERVAL_SEC)
                
        except Exception as e:
            logger.error(f"Error in producer: {e}")
        finally:
            self._cleanup()
    
    def _handle_shutdown(self, signum, frame):
        logger.info(f"Received signal {signum}, shutting down...")
        self.running = False
    
    def _cleanup(self):
        if hasattr(self, 'producer') and self.producer:
            logger.info("Closing Kafka producer...")
            self.producer.close(timeout=5)
        logger.info("Producer shutdown complete")

if __name__ == "__main__":
    producer = DataProducer()
    producer.start()