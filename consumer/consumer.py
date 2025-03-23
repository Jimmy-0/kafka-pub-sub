#kafka-pub-sub/consumer/consumer.py
import json
import logging
import signal
import sys
from typing import Dict, Any
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from termcolor import colored

from config import Config

# Setup logging
logger = Config.setup_logging()

class DataConsumer:
    
    def __init__(self):
        # Initialize the Kafka consumer
        self.config = Config()
        self.running = True
        self.consumer = self._create_consumer()
        self.user_partitions = {}
        self.partition_users = {0: [], 1: [], 2: []} 
        
        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        
        logger.info(f"Consumer initialized with bootstrap servers: {self.config.KAFKA_BOOTSTRAP_SERVERS}")
        logger.info(f"Subscribing to topic: {self.config.KAFKA_TOPIC}")
        logger.info(f"Consumer group: {self.config.KAFKA_CONSUMER_GROUP}")
    
    def _create_consumer(self) -> KafkaConsumer:
        try:
            return KafkaConsumer(
                self.config.KAFKA_TOPIC,
                bootstrap_servers=self.config.KAFKA_BOOTSTRAP_SERVERS,
                group_id=self.config.KAFKA_CONSUMER_GROUP,
                auto_offset_reset=self.config.AUTO_OFFSET_RESET,
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                key_deserializer=lambda k: k.decode('utf-8') if k else None,
                enable_auto_commit=True,
                auto_commit_interval_ms=5000,  # Commit offset every 5 seconds
            )
        except KafkaError as e:
            logger.error(f"Failed to create Kafka consumer: {e}")
            sys.exit(1)
    
    def _process_message(self, key: str, value: Dict[str, Any], metadata: Dict[str, Any]) -> None:
        message_id = value.get('id')
        timestamp = value.get('timestamp')
        user_id = value.get('user_id', 'unknown')
        partition = metadata['partition']

        if user_id not in self.user_partitions:
            self.user_partitions[user_id] = partition
            self.partition_users[partition].append(user_id)
            print("\n" + "="*50)
            print("UPDATED PARTITION MAPPING:")
            for p in sorted(self.partition_users.keys()):
                users = self.partition_users[p]
                if users:  # Only show non-empty partitions
                    print(f"[Partition {p}]: {', '.join(users)}")
            print("="*50 + "\n")

        print("\n" + "="*80)
        print(f"Received message:")
        print(f"  ID: {message_id}")
        print(f"  User ID: {colored(user_id, 'cyan')}")  # Show shortened UUID
        print(f"  Timestamp: {timestamp}")
        print(f"  Partition: {metadata['partition']}, Offset: {metadata['offset']}")
        print("="*80)
        # logger.info(
        #     f"Processed message", 
        #     extra={
        #         'message_id': message_id,
        #         'user_id': user_id,
        #         'partition': metadata['partition'],
        #         'offset': metadata['offset']
        #     }
        # )
    
    def start(self):
        logger.info("Starting to consume messages...")
        
        try:
            while self.running:
                # Poll for messages with a timeout
                message_batch = self.consumer.poll(timeout_ms=1000)
                
                # Process any received messages
                for tp, messages in message_batch.items():
                    for message in messages:
                        metadata = {
                            'topic': tp.topic,
                            'partition': tp.partition,
                            'offset': message.offset,
                            'timestamp': message.timestamp
                        }
                        self._process_message(message.key, message.value, metadata)
                
        except Exception as e:
            logger.error(f"Error in consumer: {e}")
        finally:
            self._cleanup()
    
    def _handle_shutdown(self, signum, frame):
        logger.info(f"Received signal {signum}, shutting down...")
        self.running = False
    
    def _cleanup(self):
        if hasattr(self, 'consumer') and self.consumer:
            logger.info("Closing Kafka consumer...")
            self.consumer.close()
        logger.info("Consumer shutdown complete")

if __name__ == "__main__":
    consumer = DataConsumer()
    consumer.start()