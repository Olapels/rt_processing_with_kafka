import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from kafka import KafkaConsumer
from models import ride_deserializer

server = 'localhost:9092'
topic_name = 'green-trips'

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset='earliest',
    group_id='green-trips-distance-counter',
    consumer_timeout_ms=10000,
    value_deserializer=ride_deserializer
)

print(f"Reading all available messages from {topic_name}...")

count_gt_5 = 0
total = 0
for message in consumer:
    ride = message.value
    total += 1
    if ride.trip_distance > 5.0:
        count_gt_5 += 1

consumer.close()

print(f"Total messages read: {total}")
print(f"Trips with trip_distance > 5.0: {count_gt_5}")
