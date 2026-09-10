#!/usr/bin/env python
"""Injects malformed / poison messages directly onto orders.raw."""
import argparse
import json
import os
import uuid

from kafka import KafkaProducer


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=6)
    parser.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic", default="orders.raw")
    args = parser.parse_args()

    producer = KafkaProducer(bootstrap_servers=args.bootstrap)
    variants = [
        b'{"event_id": "broken", "order_id": ',
        json.dumps({"event_id": str(uuid.uuid4())}).encode(),
        json.dumps({"event_id": str(uuid.uuid4()), "order_id": "o1", "customer_id": "c1",
                     "amount": "not-a-number", "currency": "USD", "event_time": "now"}).encode(),
        b"totally not json",
    ]
    for i in range(args.count):
        producer.send(args.topic, variants[i % len(variants)])
    producer.flush()
    print(f"Injected {args.count} poison messages onto {args.topic}.")


if __name__ == "__main__":
    main()
