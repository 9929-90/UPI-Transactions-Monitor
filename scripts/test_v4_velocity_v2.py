import json
import time
import uuid
from datetime import datetime, timezone, timedelta

from kafka import KafkaProducer

TOPIC = "upi-transactions"
USER_ID = "TEST-VELOCITY-V2-USER"
ADVANCE_USER_ID = "TEST-VELOCITY-WATERMARK-ADVANCE"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    acks="all",
    retries=5,
)

def publish(event, key):
    metadata = producer.send(
        TOPIC,
        key=key.encode("utf-8"),
        value=event,
    ).get(timeout=15)

    print(
        f"Published id={event['transaction_id']} "
        f"user={event['user_id']} "
        f"time={event['event_time']} "
        f"partition={metadata.partition} offset={metadata.offset}"
    )

try:
    # Put the five test events safely ahead of the current event-time watermark.
    base_time = (
        datetime.now(timezone.utc) + timedelta(minutes=3)
    ).replace(microsecond=0)

    test_events = []

    for i in range(5):
        event = {
            "transaction_id": f"TEST-VELOCITY-V2-{uuid.uuid4()}",
            "user_id": USER_ID,
            "merchant_id": "TEST-MERCHANT-VELOCITY-V2",
            "amount": 250.0 + i,
            "currency": "INR",
            "payment_method": "UPI",
            "status": "SUCCESS",
            "event_time": (base_time + timedelta(seconds=i)).isoformat(),
            "event_type": "PAYMENT",
            "is_synthetic": True,
        }
        test_events.append(event)
        publish(event, USER_ID)
        time.sleep(0.5)

    # Resend an identical transaction ID to test deduplication.
    print("\nPublishing a duplicate transaction ID...")
    publish(test_events[0], USER_ID)

    # Advance event time so the earlier test windows can be finalized.
    advance_time = (
        datetime.now(timezone.utc) + timedelta(minutes=6)
    ).replace(microsecond=0)

    advance_event = {
        "transaction_id": f"TEST-WATERMARK-ADVANCE-{uuid.uuid4()}",
        "user_id": ADVANCE_USER_ID,
        "merchant_id": "TEST-MERCHANT-WATERMARK",
        "amount": 100.0,
        "currency": "INR",
        "payment_method": "UPI",
        "status": "SUCCESS",
        "event_time": advance_time.isoformat(),
        "event_type": "PAYMENT",
        "is_synthetic": True,
    }

    print("\nPublishing watermark-advance event...")
    publish(advance_event, ADVANCE_USER_ID)

    producer.flush()
    print("\nTest events published. Check the Spark terminal for errors.")

finally:
    producer.close()
