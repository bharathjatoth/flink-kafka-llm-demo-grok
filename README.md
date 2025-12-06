# flink-kafka-llm-demo-grok
flink-kafka-llm-demo-grok

Producer (kafka-python) --> Kafka 'transactions'
--> Flink Job (PyFlink + Kafka Connector JAR) --> Anomaly Detect --> 'fraud-alerts'
--> Flink-Agents (Ollama LLM) --> 'explanations'
--> Consumer (kafka-python) --> Console Alerts

## Quickstart
1. `chmod +x *.sh`
2. `./setup_demo.sh` (~4 min: downloads/builds/starts)
3. `./run_demo.sh` (live: produces data, processes, prints alerts)
4. `./teardown_demo.sh` (stop)

## Scaling Notes
- Kafka: Partitioned topics for concurrency.
- Flink: Stateful windows for replayability.
- LLM: Local Ollama; swap to API in prod.

Flink UI: http://localhost:8081 (monitor jobs).
