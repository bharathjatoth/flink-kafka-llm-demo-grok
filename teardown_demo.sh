#!/bin/bash
echo "Stopping everything..."

# Stop Flink
./flink/bin/stop-cluster.sh || true

# Stop Kafka & Zookeeper
./kafka/bin/kafka-server-stop.sh || true
./kafka/bin/zookeeper-server-stop.sh || true

# Stop Ollama
pkill -f "ollama serve" || true

echo "Cleaned up. Data retained in Kafka for replay."
