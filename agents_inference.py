import logging
import json
import ollama
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col

logging.basicConfig(level=logging.INFO)

env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)

# Add JARs (Flink-Agents built JAR + Kafka)
t_env.get_config().set("pipeline.jars", 
    "file:///path/to/flink-agents/dist/flink-agents-0.2-SNAPSHOT.jar;file:///path/to/flink/lib/flink-connector-kafka-3.2.0-1.19.jar")

# Source: Kafka 'fraud-alerts'
source_ddl = """
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
    'properties.group.id' = 'flink-agent-group',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json'
)
"""

# Sink: 'explanations' (for consumer)
sink_ddl = """
CREATE TABLE explanations (
    user_id STRING,
    explanation STRING,
    timestamp TIMESTAMP(3)
) WITH (
    'connector' = 'kafka',
    'topic' = 'explanations',
    'properties.bootstrap.servers' = 'localhost:9092',
    'format' = 'json'
)
"""

t_env.execute_sql(source_ddl)
t_env.execute_sql(sink_ddl)

def llm_explain(row: dict) -> dict:
    """Ollama LLM call (retry on failure)"""
    prompt = f"As a fraud analyst, explain this transaction risk in 1 sentence: {json.dumps(row)}"
    for attempt in range(3):
        try:
            response = ollama.chat(model='llama3.2', messages=[{'role': 'user', 'content': prompt}])
            explanation = response['message']['content']
            return {
                'user_id': row['user_id'],
                'explanation': explanation,
                'timestamp': row['timestamp']
            }
        except Exception as e:
            logging.warning(f"LLM attempt {attempt+1} failed: {e}")
            time.sleep(2 ** attempt)  # Backoff
    return {'user_id': row['user_id'], 'explanation': 'Explanation failed - retry later', 'timestamp': row['timestamp']}

# Agentic processing: Use Flink-Agents for stateful LLM (simplified UDF; in prod, use Agents API)
# Here, we use a Python UDF for demo (Flink-Agents would wrap as agent operator)
from pyflink.table.udf import udf
@udf(result_type='STRING')
def explain_udf(user_id: str, amount: float, device_id: str, location: str, anomaly_score: float) -> str:
    row = {'user_id': user_id, 'amount': amount, 'device_id': device_id, 'location': location, 'anomaly_score': anomaly_score}
    return llm_explain(row)['explanation']

t_env.register_function('explain', explain_udf)

result = t_env.sql_query("""
    SELECT user_id, explain(user_id, amount, device_id, location, anomaly_score) as explanation, timestamp
    FROM fraud_alerts
""").execute_insert('explanations').wait()

print("Flink Agents LLM job submitted.")
