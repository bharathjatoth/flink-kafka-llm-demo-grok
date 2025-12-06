#!/bin/bash
echo "Starting live demo (2-min txn stream)..."

# Background jobs
python producer.py --duration 120 &
sleep 5  # Let producer ramp up

# Submit Flink jobs (use FLINK_HOME)
export FLINK_HOME=$(pwd)/flink
$FLINK_HOME/bin/flink run -py flink_job.py &
sleep 5

python agents_inference.py &  # Runs as Flink job via PyFlink

# Foreground: Consume explanations
python consumer.py
