# Kafka Stream

This Spring Boot project, built using Gradle, demonstrates how to use Apache Kafka Streams for real-time stream processing and event-driven data transformation.

The data consumed from Kafka topics is produced and stored through REST APIs defined in the `kafka-stream-order` project.

This repository focuses on Kafka Streams concepts and stream processing operations such as filtering, mapping, branching, aggregation, joins, windowing, transformations, and topic-to-topic data flow using Kafka Streams.

The implementation follows concepts and hands-on techniques learned from the Udemy course:
Apache Kafka with Java & Spring Boot — Theory & Hands-On Coding.

Repository: :contentReference[oaicite:0]{index=0}

---

## Features Covered

This project demonstrates multiple Kafka Streams processing techniques including:

- Stateless stream processing
    - `filter`
    - `map`
    - `flatMap`
    - `peek`
    - `foreach`

- Stateful stream processing
    - Aggregations
    - Grouping
    - Counting
    - Reducing

- Branching and routing
    - Stream branching
    - Conditional topic routing

- Stream transformations
    - Value transformation
    - Custom transformers
    - Re-keying streams

- Joins
    - KStream-KStream Join
    - KStream-KTable Join
    - KTable-KTable Join

- Windowing operations
    - Tumbling windows
    - Hopping windows
    - Session windows

- Table processing
    - KTable
    - GlobalKTable

- Serialization and deserialization
    - JSON Serde
    - Avro Serde
    - Schema Registry integration

- Error handling and retries

- Topic-to-topic stream pipelines

- Real-time analytics and aggregation use cases

---

## Tech Stack

- Java
- Spring Boot
- Apache Kafka
- Kafka Streams
- Gradle
- Docker
- Schema Registry
- Avro

---

## Running Kafka with Docker

Build and run Kafka containers using Docker for local development and testing.

### Start Kafka Environment

```bash
docker compose up -d
```

## Project Goal

The goal of this repository is to provide hands-on examples of Kafka Streams using Spring Boot while exploring real-world event streaming patterns and stream processing architectures.

This project is continuously updated as more Kafka Streams concepts and implementations are added.