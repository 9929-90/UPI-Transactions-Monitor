
import json
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

USER_ID = "TEST-VELOCITY-USER-001"
TRANSACTION_COUNT = 5

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    acks="all",
    retries=5,
)

try:
    for i in range(TRANSACTION_COUNT):
        event = {
            "transaction_id": f"TEST-VELOCITY-{uuid.uuid4()}",
            "user_id": USER_ID,
            "merchant_id": "TEST-MERCHANT-VELOCITY",
            "amount": 250.0 + i,
            "currency": "INR",
            "payment_method": "UPI",
            "status": "SUCCESS",
            "event_time": datetime.now(timezone.utc).isoformat(),
            "event_type": "PAYMENT",
            "is_synthetic": True,
        }

        metadata = producer.send(
            "upi-transactions",
            key=USER_ID.encode("utf-8"),
            value=event,
        ).get(timeout=15)

        print(
            f"Sent {i + 1}/{TRANSACTION_COUNT}: "
            f"{event['transaction_id']} | "
            f"amount=INR {event['amount']:.2f} | "
            f"partition={metadata.partition} | "
            f"offset={metadata.offset}"
        )

        time.sleep(1)

    print("\nFive test transactions published successfully.")

finally:
    producer.flush()
    producer.close()
