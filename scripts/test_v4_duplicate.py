import json
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

transaction_id = "TEST-DUPLICATE-001"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    key_serializer=lambda value: value.encode("utf-8"),
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    acks="all",
    retries=5,
)

try:
    for attempt in range(2):
        event = {
            "transaction_id": transaction_id,
            "user_id": "TEST-USER-DUPLICATE-001",
            "merchant_id": "TEST-MERCHANT-001",
            "amount": 1200.0,
            "currency": "INR",
            "payment_method": "UPI",
            "status": "SUCCESS",
            "event_time": datetime.now(timezone.utc).isoformat(),
            "event_type": "PAYMENT",
            "is_synthetic": True,
        }

        metadata = producer.send(
            "upi-transactions",
            key=transaction_id,
            value=event,
        ).get(timeout=15)

        print(
            f"Copy {attempt + 1} sent | "
            f"ID={transaction_id} | "
            f"Partition={metadata.partition} | "
            f"Offset={metadata.offset}"
        )

        if attempt == 0:
            time.sleep(2)

finally:
    producer.flush()
    producer.close()

print("Both copies were published. Now verify the processed output.")
