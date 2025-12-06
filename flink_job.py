import logging
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col

# Setup logging
logging.basicConfig(level=logging.INFO)

# Environment (streaming mode)
env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)

# Add Kafka connector JARs (downloaded in setup)
t_env.get_config().set("pipeline.jars", "file:///path/to/flink/lib/flink-connector-kafka-3.2.0-1.19.jar;file:///path/to/flink/lib/flink-sql-connector-kafka-3.2.0-1.19.jar")

# Note: Replace /path/to/flink with actual $(pwd)/flink in run_demo.sh if needed

# Source DDL: Kafka connector for 'transactions' (FlinkKafkaConsumer under the hood)
source_ddl = """
CREATE TABLE transactions (
    user_id STRING,
    amount DOUBLE,
    device_id STRING,
    location STRING,
    timestamp TIMESTAMP(3),
    WATERMARK FOR timestamp AS timestamp - INTERVAL '2' SECONDS
) WITH (
    'connector' = 'kafka',
    'topic' = 'transactions',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'flink-fraud-group',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
)
"""

# Sink DDL: Output flagged to 'fraud-alerts' (FlinkKafkaProducer)
sink_ddl = """
CREATE TABLE fraud_alerts (
    user_id STRING,
    amount DOUBLE,
    device_id STRING,
    location STRING,
    timestamp TIMESTAMP(3),
    anomaly_score DOUBLE
) WITH (
    'connector' = 'kafka',
    'topic' = 'fraud-alerts',
    'properties.bootstrap.servers' = 'localhost:9092',
    'format' = 'json'
)
"""

t_env.execute_sql(source_ddl)
t_env.execute_sql(sink_ddl)

# Stateful anomaly detection: 5-min tumbling window per user, flag >3σ
t_env.from_path('transactions').group_by(col('user_id')) \
    .select(col('user_id'), col('amount'), col('timestamp'), col('device_id'), col('location')) \
    .window(col('timestamp').interval('5 minutes').every('5 minutes')) \
    .aggregate(  # PyFlink Table API for rolling stats (simplified; in prod use keyed state)
        avg_amount= col('amount').avg(),
        stddev_amount= col('amount').stddev_pop(),
        first(txn= col('amount').first(col('amount')))
    ).select(
        col('user_id'),
        col('txn'),
        col('device_id'),  # Carry over
        col('location'),
        col('window_end').as_('timestamp'),
        (col('txn') - col('avg_amount')) / col('stddev_amount').as_('anomaly_score')
    ).filter(col('anomaly_score') > 3.0) \
    .execute_insert('fraud_alerts').wait()

print("Flink fraud job submitted. Check UI at localhost:8081")
