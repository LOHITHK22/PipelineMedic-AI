#!/usr/bin/env python
"""Injects a batch of schema-drifted order events directly onto orders.raw.

Usage: python scripts/inject_schema_drift.py [--count 30] [--bootstrap kafka:9092]
"""
import argparse
import json
import os
import random
import string
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer


def random_id(prefix):
    return f"{prefix}_{''.join(random.choices(string.ascii_lowercase + string.digits, k=8))}"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=30)
    parser.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic", default="orders.raw")
    args = parser.parse_args()

    producer = KafkaProducer(bootstrap_servers=args.bootstrap, value_serializer=lambda v: json.dumps(v).encode())
    for _ in range(args.count):
        event = {
            "event_id": str(uuid.uuid4()),
            "order_id": random_id("order"),
            "customerId": random_id("cust"),  # renamed from customer_id -- BREAKING
            "amount": str(round(random.uniform(5, 500), 2)),  # number -> string -- BREAKING
            "currency": "USD",
            "event_time": datetime.now(timezone.utc).isoformat(),
        }
        producer.send(args.topic, event)
    producer.flush()
    print(f"Injected {args.count} schema-drifted events onto {args.topic}. "
          f"Watch GET /incidents on the backend for a SCHEMA_DRIFT incident.")


if __name__ == "__main__":
    main()
