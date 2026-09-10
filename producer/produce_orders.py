"""Realistic order-event producer for orders.raw.

Modes (set via SCENARIO env var, default "normal"):
  normal        - well-formed events, steady rate
  schema_drift  - after N events, starts renaming customer_id -> customerId
                  and changing amount from number to string
  poison        - periodically emits malformed JSON / wrong-typed records
  lag           - emits at a much higher rate to build up consumer lag
"""
import json
import os
import random
import string
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC_ORDERS_RAW", "orders.raw")
SCENARIO = os.environ.get("SCENARIO", "normal")
RATE_PER_SEC = float(os.environ.get("RATE_PER_SEC", "2"))


def random_id(prefix: str) -> str:
    return f"{prefix}_{''.join(random.choices(string.ascii_lowercase + string.digits, k=10))}"


def make_event() -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "order_id": random_id("order"),
        "customer_id": random_id("cust"),
        "amount": round(random.uniform(5.0, 500.0), 2),
        "currency": random.choice(["USD", "EUR", "GBP"]),
        "event_time": datetime.now(timezone.utc).isoformat(),
    }


def apply_schema_drift(event: dict) -> dict:
    drifted = dict(event)
    drifted["customerId"] = drifted.pop("customer_id")
    drifted["amount"] = str(drifted["amount"])  # number -> string, BREAKING
    return drifted


def make_poison_message(i: int) -> bytes:
    variants = [
        b'{"event_id": "not-closed", "order_id": ',  # invalid JSON
        json.dumps({"event_id": str(uuid.uuid4())}).encode(),  # missing required fields
        json.dumps({**make_event(), "amount": "not-a-number"}).encode(),  # type violation
        b"not even json at all " + str(i).encode(),
    ]
    return random.choice(variants)


def connect_with_retry() -> KafkaProducer:
    for attempt in range(30):
        try:
            return KafkaProducer(bootstrap_servers=BOOTSTRAP, value_serializer=lambda v: v if isinstance(v, bytes) else json.dumps(v).encode())
        except Exception as e:
            print(f"[producer] Kafka not ready (attempt {attempt}): {e}")
            time.sleep(2)
    raise RuntimeError("Could not connect to Kafka after retries")


def main():
    producer = connect_with_retry()
    print(f"[producer] starting scenario={SCENARIO} rate={RATE_PER_SEC}/s topic={TOPIC}")
    i = 0
    while True:
        i += 1
        if SCENARIO == "schema_drift" and i > 20:
            event = apply_schema_drift(make_event())
            producer.send(TOPIC, event)
        elif SCENARIO == "poison" and i % 4 == 0:
            producer.send(TOPIC, make_poison_message(i))
        elif SCENARIO == "lag":
            for _ in range(50):
                producer.send(TOPIC, make_event())
        else:
            producer.send(TOPIC, make_event())

        producer.flush()
        time.sleep(1.0 / RATE_PER_SEC if RATE_PER_SEC > 0 else 0.1)


if __name__ == "__main__":
    main()
