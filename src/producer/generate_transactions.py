import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaProducer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "upi-transactions"
OUTPUT = Path("data/raw/transactions.jsonl")

def generate_transaction():
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": f"user_{random.randint(1, 500):04d}",
        "merchant_id": f"merchant_{random.randint(1, 80):03d}",
        "amount": round(random.uniform(10, 25000), 2),
        "currency": "INR",
        "payment_method": "UPI",
        "status": random.choices(
            ["SUCCESS", "FAILED", "PENDING"],
            weights=[80, 15, 5],
            k=1,
        )[0],
        "event_time": datetime.now(timezone.utc).isoformat(),
        "event_type": "payment_transaction",
        "is_synthetic": True,
    }

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--interval", type=float, default=0.2)
    args = parser.parse_args()

    if args.count < 1 or args.interval < 0:
        parser.error("count must be positive and interval non-negative")

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=5,
    )

    sent = 0
    try:
        with OUTPUT.open("w", encoding="utf-8") as file:
            for _ in range(args.count):
                event = generate_transaction()

                producer.send(
                    TOPIC,
                    key=event["transaction_id"].encode("utf-8"),
                    value=event,
                ).get(timeout=15)

                file.write(json.dumps(event) + "\n")
                file.flush()
                sent += 1

                print(
                    f"Sent {sent}/{args.count}: "
                    f"{event['transaction_id']} | "
                    f"INR {event['amount']:.2f} | {event['status']}"
                )

                if args.interval:
                    time.sleep(args.interval)
    finally:
        producer.flush()
        producer.close()

    print(f"\nSuccessfully sent {sent} events to Kafka topic '{TOPIC}'")
    print(f"Local copy: {OUTPUT.resolve()}")

if __name__ == "__main__":
    main()
