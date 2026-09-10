#!/usr/bin/env python
"""Floods orders.raw with a burst of messages to build up consumer lag."""
import argparse
import json
import os
import random
import string
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer


def make_event():
    return {
        "event_id": str(uuid.uuid4()),
        "order_id": f"order_{''.join(random.choices(string.ascii_lowercase, k=8))}",
        "customer_id": f"cust_{''.join(random.choices(string.ascii_lowercase, k=8))}",
        "amount": round(random.uniform(5, 500), 2),
        "currency": "USD",
        "event_time": datetime.now(timezone.utc).isoformat(),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=10000)
    parser.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic", default="orders.raw")
    args = parser.parse_args()

    producer = KafkaProducer(bootstrap_servers=args.bootstrap, value_serializer=lambda v: json.dumps(v).encode())
    for i in range(args.count):
        producer.send(args.topic, make_event())
        if i % 1000 == 0:
            producer.flush()
            print(f"sent {i}/{args.count}")
    producer.flush()
    print(f"Injected {args.count} events onto {args.topic} to build up consumer lag.")


if __name__ == "__main__":
    main()
