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


def _maybe_record_deployment_event(component: str, note: str):
    """Optionally records a synthetic DEPLOYMENT SystemEvent in Postgres, to
    stand in for what a real CI/CD pipeline would emit on every deploy. Best
    effort: this script's job is to inject Kafka traffic, so a DB write
    failure here (e.g. backend/Postgres not reachable from wherever this
    script runs) must never fail the injection itself."""
    try:
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker

        database_url = os.environ.get(
            "DATABASE_URL", "postgresql+psycopg2://pipelinemedic:pipelinemedic@localhost:5432/pipelinemedic"
        )
        engine = create_engine(database_url)
        Session = sessionmaker(bind=engine)
        with Session() as session:
            session.execute(
                __import__("sqlalchemy").text(
                    "INSERT INTO system_events (id, event_type, component, payload, created_at) "
                    "VALUES (:id, :event_type, :component, :payload, now())"
                ),
                {
                    "id": str(uuid.uuid4()), "event_type": "DEPLOYMENT", "component": component,
                    "payload": json.dumps({"note": note}),
                },
            )
            session.commit()
        print(f"Recorded synthetic DEPLOYMENT system_event for component={component}.")
    except Exception as e:
        print(f"(non-fatal) could not record synthetic deployment event: {e}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=30)
    parser.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic", default="orders.raw")
    parser.add_argument(
        "--record-deployment-event", action="store_true",
        help="Also write a synthetic DEPLOYMENT system_event to Postgres, simulating what a real "
             "deploy pipeline would emit, so event correlation has a trigger to point at.",
    )
    args = parser.parse_args()

    if args.record_deployment_event:
        _maybe_record_deployment_event(args.topic, "synthetic deploy via inject_schema_drift.py")

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
