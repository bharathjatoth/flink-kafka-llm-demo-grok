#!/bin/bash
set -e

echo "=== QUICK DEMO SETUP: Kafka + Flink + Flink-Agents + Ollama ==="

# 1. Kafka (pre-built binary; latest stable 3.8.0 as of Dec 2025, Scala 2.13)
KAFKA_VERSION="3.8.0"
if [ ! -d "kafka" ]; then
  echo "Downloading Kafka $KAFKA_VERSION..."
  curl -L "https://downloads.apache.org/kafka/$KAFKA_VERSION/kafka_2.13-$KAFKA_VERSION.tgz" | tar xz
  mv "kafka_2.13-$KAFKA_VERSION" kafka
fi

# 2. Flink (pre-built binary; latest stable 1.19.0 as of Dec 2025)
FLINK_VERSION="1.19.0"
if [ ! -d "flink" ]; then
  echo "Downloading Flink $FLINK_VERSION..."
  curl -L "https://downloads.apache.org/flink/flink-$FLINK_VERSION/flink-$FLINK_VERSION-bin-scala_2.12.tgz" | tar xz
  mv "flink-$FLINK_VERSION" flink
fi

# 3. Flink-Agents (clone & build; v0.2-SNAPSHOT as of Dec 2025)
if [ ! -d "flink-agents" ]; then
  echo "Cloning apache/flink-agents..."
  git clone https://github.com/apache/flink-agents.git
  cd flink-agents
  ./tools/build.sh  # Builds Java/Python components (requires Maven/Java 11)
  pip install -e . >/dev/null
  cd ..
fi

# 4. Ollama (auto-install if missing)
if ! command -v ollama &> /dev/null; then
  echo "Installing Ollama..."
  curl -fsSL https://ollama.com/install.sh | sh
fi

# 5. Python dependencies + Flink Kafka Connector JAR
pip install -q kafka-python pyflink==1.19.0 ollama pandas tqdm

# Download Kafka connector JAR (for PyFlink; matches Flink 1.19/Kafka 3.8)
mkdir -p flink/lib
cd flink/lib
curl -L "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/3.2.0-1.19/flink-connector-kafka-3.2.0-1.19.jar" -o flink-connector-kafka.jar
curl -L "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar" -o flink-sql-connector-kafka.jar  # For Table API
cd ../..

# 6. Start services
echo "Starting Zookeeper & Kafka..."
./kafka/bin/zookeeper-server-start.sh -daemon ./kafka/config/zookeeper.properties
./kafka/bin/kafka-server-start.sh -daemon ./kafka/config/server.properties
sleep 10

echo "Starting Flink cluster..."
export FLINK_HOME=$(pwd)/flink
./flink/bin/start-cluster.sh
sleep 5

echo "Starting Ollama server..."
ollama serve &
OLLAMA_PID=$!
sleep 5

echo "Pulling llama3.2 (fast for demo)..."
ollama pull llama3.2:latest

# 7. Create topics (7-day retention)
echo "Creating Kafka topics..."
./kafka/bin/kafka-topics.sh --create --topic transactions --bootstrap-server localhost:9092 --partitions 4 --replication-factor 1 --config retention.ms=604800000
./kafka/bin/kafka-topics.sh --create --topic fraud-alerts --bootstrap-server localhost:9092 --partitions 4 --replication-factor 1 --config retention.ms=604800000
./kafka/bin/kafka-topics.sh --create --topic explanations --bootstrap-server localhost:9092 --partitions 4 --replication-factor 1 --config retention.ms=604800000

echo "ALL SYSTEMS GO!"
echo "Flink UI: http://localhost:8081"
echo "Kafka: localhost:9092"
echo "Ollama: llama3.2 ready"
echo "Run: ./run_demo.sh for live demo"
