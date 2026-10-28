import json
from datetime import datetime, timezone
from kafka import KafkaProducer

event = {
    "transaction_id": "TEST-INFINITY-20261027-001",
    "user_id": "TEST-USER-INFINITY-001",
    "merchant_id": "TEST-MERCHANT-001",
    "amount": float("inf"),
    "currency": "INR",
    "payment_method": "UPI",
    "status": "SUCCESS",
    "event_time": datetime.now(timezone.utc).isoformat(),
    "event_type": "PAYMENT",
    "is_synthetic": True,
}

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

    print("Infinity test event sent.")
    print(f"Transaction ID: {event['transaction_id']}")
    print(f"Amount: {event['amount']}")
    print(f"Partition: {metadata.partition}")
    print(f"Offset: {metadata.offset}")
finally:
    producer.flush()
    producer.close()
