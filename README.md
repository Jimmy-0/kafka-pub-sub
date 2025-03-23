# kafka-pub-sub
A containerized publish/subscribe system built with Kafka and Python
## Overview
- Kafka : Message broker
- Zookeeper: Coordination service for Kafka
- Producer: Generate sample data and publishe it to a Kafka topic
- Consumer: Subscribes to the same Kafka topic, processes messages, and displays partition mapping information

## User-based Partition 
- When a consumer receives messages, it maintains and displays:
  - A mapping of users to their assigned partitions
  - A visualization of which users are assigned to each partition
- Sample
```
==================================================
kafka-consumer  | UPDATED PARTITION MAPPING:
kafka-consumer  | [Partition 0]: user1
kafka-consumer  | [Partition 1]: user4, user5
kafka-consumer  | [Partition 2]: user3, user2
==================================================
```
```
================================================================================
kafka-consumer  | Received message:
kafka-consumer  |   ID: 2c3c0397-d5f2-4b08-be62-96bda5ebcc8c
kafka-consumer  |   User ID: user1
kafka-consumer  |   Timestamp: 2025-03-23T09:48:27.892556
kafka-consumer  |   Partition: 0, Offset: 12
================================================================================
```

## Getting Started
- Clone the repository:
```bash 
git clone https://github.com/Jimmy-0/kafka-pub-sub.git
cd kafka-python-pubsub
```
- Start the system using Docker Compose:
```bash
docker-compose up
```

- This will:
  - Start Zookeeper
  - Start Kafka
  - Create the sample-topic with 3 partitions
  - Start the producer container
  - Start the consumer container

## Benefits of the Design
- Scalability: Multiple consumers can process messages in parallel
- Ordering Guarantee: Messages from the same user are processed in order
- Load Balancing: Work is distributed across partitions
- Fault Tolerance: Consumer group rebalancing if a consumer fails
- Monitoring: Visual feedback on partition distribution