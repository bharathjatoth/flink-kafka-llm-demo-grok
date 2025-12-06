import json
import time
import argparse
import random
from datetime import datetime
from kafka import KafkaProducer
from tqdm import tqdm

parser = argparse.ArgumentParser()
parser.add_argument('--duration', type=int, default=120, help='Seconds to produce')
args = parser.parse_args()

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=3,
    acks='all'  # Durability
)

locations = ['US', 'UK', 'IN', 'CN']
devices = ['iPhone12', 'AndroidX', 'WebChrome']

def generate_txn(is_fraud=False):
    user_id = f"user_{random.randint(1, 1000)}"
    amount = random.uniform(10, 50) if not is_fraud else random.uniform(500, 2000)
    device_id = random.choice(devices)
    location = random.choice(locations)
    if is_fraud:
        location = random.choice([l for l in locations if l != 'US'])  # Geo-mismatch
    timestamp = datetime.utcnow().isoformat()
    return {
        'user_id': user_id, 'amount': amount, 'device_id': device_id,
        'location': location, 'timestamp': timestamp
    }

print("Producing transactions...")
start = time.time()
with tqdm(total=args.duration) as pbar:
    while time.time() - start < args.duration:
        is_fraud = random.random() < 0.1  # 10% fraud
        txn = generate_txn(is_fraud)
        producer.send('transactions', value=txn)
        time.sleep(0.01)  # ~100/sec
        pbar.update(1)

producer.flush()
print("Production complete.")
