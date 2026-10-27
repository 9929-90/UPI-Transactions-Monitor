import json
from pathlib import Path

import duckdb
from kafka import KafkaProducer

root = Path(__file__).resolve().parents[1]
parquet_glob = (
    root / "data" / "processed" / "transactions_v3" / "**" / "*.parquet"
).as_posix()

con = duckdb.connect()
row = con.execute(f"""
    SELECT transaction_id, user_id, merchant_id, amount, currency,
           payment_method, status, event_time, event_type, is_synthetic
    FROM read_parquet('{parquet_glob}')
    LIMIT 1
""").fetchone()

if row is None:
    raise SystemExit("No processed transaction found. Nothing was sent.")

columns = [
    "transaction_id", "user_id", "merchant_id", "amount", "currency",
    "payment_method", "status", "event_time", "event_type", "is_synthetic"
]
event = dict(zip(columns, row))

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    acks="all",
    retries=5,
)

try:
    metadata = producer.send(
        "upi-transactions",
        key=event["transaction_id"].encode("utf-8"),
        value=event,
    ).get(timeout=15)

    print("Duplicate test event sent successfully.")
    print(f"Transaction ID: {event['transaction_id']}")
    print(f"Original event time: {event['event_time']}")
    print(f"Kafka partition: {metadata.partition}")
    print(f"Kafka offset: {metadata.offset}")
finally:
    producer.flush()
    producer.close()
