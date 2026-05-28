# Real-Time Stock Market Data Engineering Pipeline

An end-to-end real-time stock market data engineering pipeline built using Apache Kafka and AWS cloud services.
This project simulates real-time stock market streaming from a static dataset, ingests streaming events into Apache Kafka hosted on AWS EC2, stores the streamed data into Amazon S3, catalogs the schema using AWS Glue Crawlers, and enables serverless SQL querying using Amazon Athena.

---

# Architecture Overview

```text
Static CSV Dataset
        ↓
Python Stock Market Simulator
        ↓
Kafka Producer
        ↓
Apache Kafka Broker (AWS EC2)
        ↓
Kafka Topic (stock_test)
        ↓
Kafka Consumer
        ↓
Amazon S3 Data Lake
        ↓
AWS Glue Crawler
        ↓
Glue Data Catalog
        ↓
Amazon Athena Queries
```

---

# Project Objective

The objective of this project is to build a cloud-native real-time streaming data pipeline capable of:

* Simulating live stock market events
* Streaming data through Apache Kafka
* Consuming streaming events in real time
* Persisting data into an AWS-based data lake
* Automatically discovering schema using AWS Glue
* Querying streaming data using Amazon Athena

This project demonstrates practical implementation of distributed event streaming and serverless analytics workflows commonly used in modern data engineering systems.

---

# Tech Stack

## Languages & Libraries
* Python
* Pandas
* kafka-python
* s3fs
* json

## Streaming & Messaging
* Apache Kafka
* Apache Zookeeper

## AWS Services
* Amazon EC2
* Amazon S3
* AWS Glue
* AWS Glue Crawler
* AWS Glue Data Catalog
* Amazon Athena

---

# Kafka Infrastructure Setup

## Install Kafka on EC2

```bash
wget https://archive.apache.org/dist/kafka/3.3.1/kafka_2.12-3.3.1.tgz

tar -xvf kafka_2.12-3.3.1.tgz
```

---

## Install Java

```bash
sudo yum install java-1.8.0-openjdk

java -version
```

---

## Start Zookeeper

```bash
cd kafka_2.12-3.3.1

bin/zookeeper-server-start.sh config/zookeeper.properties
```

---

## Start Kafka Broker

```bash
export KAFKA_HEAP_OPTS="-Xmx256M -Xms128M"

bin/kafka-server-start.sh config/server.properties
```

---

# Kafka Networking Configuration

Initially, Kafka registered itself using the private EC2 DNS:

```text
PLAINTEXT://ip-172-31-21-28.eu-north-1.compute.internal:9092
```

This prevented external producer and consumer applications from connecting to Kafka outside the EC2 private network.

To solve this:

* Updated `ADVERTISED_LISTENERS` inside `server.properties`
* Changed broker endpoint from private DNS to EC2 public IP
* Restarted Kafka and Zookeeper services

Updated configuration:

```properties
ADVERTISED_LISTENERS=PLAINTEXT://<EC2_PUBLIC_IP>:9092
```

Kafka then successfully registered using the public endpoint:

```text
PLAINTEXT://51.21.152.125:9092
```

---

# Security Group Configuration

Inbound rules were temporarily configured to allow external access for development and testing purposes.

Ports used:

* `9092` → Kafka Broker
* `2181` → Zookeeper

> Note:
> In production environments, inbound rules should be restricted to trusted IP ranges and least-privilege networking practices should be followed.

---

# Kafka Topic Creation

```bash
bin/kafka-topics.sh --create \
--topic stock_test \
--bootstrap-server 51.21.152.125:9092 \
--replication-factor 1 \
--partitions 1
```

---

# Kafka Producer Setup

## Start Producer Console

```bash
bin/kafka-console-producer.sh \
--topic stock_test \
--bootstrap-server 51.21.152.125:9092
```

---

# Kafka Consumer Setup

## Start Consumer Console

```bash
bin/kafka-console-consumer.sh \
--topic stock_test \
--bootstrap-server 51.21.152.125:9092
```

This verified successful end-to-end Kafka message flow between producer and consumer applications.

---

# Real-Time Stock Market Producer

A static stock market dataset was transformed into a simulated real-time event stream using Python.

## Producer Logic

```python
df = pd.read_csv("data/indexProcessed.csv")

while True:
    dict_stock = df.sample(1).to_dict(orient="records")[0]
    producer.send('stock_test', value=dict_stock)
    sleep(1)

producer.flush()
```

## Features

* Randomized stock event generation
* Continuous event streaming
* Kafka producer integration
* Real-time simulation using historical data

---

# Kafka Consumer & Amazon S3 Integration

The Kafka consumer continuously reads streaming events and stores them into Amazon S3 as JSON objects.

## Consumer Logic

```python
consumer = KafkaConsumer(
    'stock_test',
    bootstrap_servers=['51.21.152.125:9092'],
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)
```

---

## Amazon S3 Storage Logic

```python
for count, i in enumerate(consumer):
    with s3.open(
        "s3://kafka-stock-market-aduet/stock_market_{}.json".format(count),
        'w'
    ) as file:
        json.dump(i.value, file)
```

---

# Amazon S3 Data Lake

Created S3 bucket:

```text
kafka-stock-market-aduet
```

The consumer application continuously persisted streaming stock events into the S3 bucket as JSON files.

---

# AWS Glue Integration

## Glue Crawler

Created AWS Glue crawler:

```text
kafka_stock_market_aduet
```

Crawler source:

* Amazon S3 bucket containing streamed JSON stock data

The Glue crawler automatically:

* scanned S3 objects
* inferred schema
* created metadata tables
* updated the Glue Data Catalog

---

# Amazon Athena Querying

Using the Glue Data Catalog, the streamed stock market data was queried directly in Amazon Athena using SQL.

This enabled:

* serverless querying
* schema-based analytics
* data lake exploration
* real-time analytical workflows

---

# Key Concepts Demonstrated

* Real-Time Data Streaming
* Distributed Messaging Systems
* Apache Kafka Producer/Consumer Architecture
* Cloud-Based Data Lakes
* AWS Glue Schema Discovery
* Serverless Analytics using Athena
* Event-Driven Data Pipelines
* Streaming Simulation using Historical Data
* EC2 Networking & Kafka Listener Configuration

---

# Learning Outcomes

Through this project, I gained hands-on experience with:

* Apache Kafka setup and configuration
* Kafka networking and advertised listeners
* Real-time event streaming pipelines
* AWS-based cloud data lake architectures
* Glue metadata cataloging
* Athena-based serverless querying
* Producer-consumer streaming workflows
* Streaming simulation techniques

---

# Conclusion

This project demonstrates a complete end-to-end real-time data engineering workflow using Apache Kafka and AWS analytics services.

It simulates how modern organizations ingest, stream, store, catalog, and analyze high-volume real-time event data using scalable cloud-native architectures.
