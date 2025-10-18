#!/bin/bash

# Setup Flink with Kafka connectors
echo "Setting up Flink with Kafka connectors..."

# Create directories
mkdir -p flink-job/lib
cd flink-job/lib

# Flink version
FLINK_VERSION="1.18.1"
KAFKA_VERSION="3.0.2"

# Download Flink Kafka connector
echo "Downloading Flink Kafka Connector..."
curl -O https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${FLINK_VERSION}/flink-sql-connector-kafka-${FLINK_VERSION}.jar

# Download Kafka clients
echo "Downloading Kafka clients..."
curl -O https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_VERSION}/kafka-clients-${KAFKA_VERSION}.jar

echo "Done! JARs downloaded to flink-job/lib/"
ls -lh

cd ../..
