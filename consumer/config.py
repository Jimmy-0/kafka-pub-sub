#kafka-pub-sub/consumer/config.py
import os
import logging
from pythonjsonlogger import jsonlogger

class Config:    
    # Kafka settings
    KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'sample-topic')
    KAFKA_CONSUMER_GROUP = os.environ.get('KAFKA_CONSUMER_GROUP', 'sample-group')
    AUTO_OFFSET_RESET = os.environ.get('AUTO_OFFSET_RESET', 'earliest')
    
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