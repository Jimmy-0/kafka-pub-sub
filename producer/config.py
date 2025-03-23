# kafka-pub-sub/producer/config.py
import os
import logging
from pythonjsonlogger import jsonlogger

class Config:    
    # Kafka settings
    KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'sample-topic')
    
    # Producer settings
    PRODUCE_INTERVAL_SEC = int(os.environ.get('PRODUCE_INTERVAL_SEC', 2))
    
    # Logging
    LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO').upper()
    
    @staticmethod
    def setup_logging():
        log_level = getattr(logging, Config.LOG_LEVEL, logging.INFO)
        logger = logging.getLogger()
        logger.setLevel(log_level)
        
        # Remove existing handlers to avoid duplicates
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        
        # Create JSON log handler
        logHandler = logging.StreamHandler()
        formatter = jsonlogger.JsonFormatter(
            '%(asctime)s %(name)s %(levelname)s %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        logHandler.setFormatter(formatter)
        logger.addHandler(logHandler)
        
        return logger