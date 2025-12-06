import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'explanations',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    group_id='alert-consumer',
    auto_offset_reset='earliest'
)

print("Consuming explanations...")
for message in consumer:
    alert = message.value
    print(f"ALERT [{alert['user_id']}]: {alert['explanation']}")
