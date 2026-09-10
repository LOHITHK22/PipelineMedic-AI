#!/usr/bin/env python
"""Resets demo state: truncates PipelineMedic tables and restores the orders
table's column name if it was renamed by inject_airflow_failure.py."""
import os

import psycopg2

DSN = os.environ.get(
    "PIPELINEMEDIC_DB_DSN", "dbname=pipelinemedic user=pipelinemedic password=pipelinemedic host=localhost port=5432"
)

TABLES = [
    "audit_log", "validation_results", "repair_executions", "approval_requests",
    "repair_plans", "incident_events", "incidents", "schema_versions",
]


def main():
    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    with conn.cursor() as cur:
        for t in TABLES:
            cur.execute(f"TRUNCATE TABLE {t} CASCADE;")
        cur.execute("""
            DO $$
            BEGIN
                IF EXISTS (SELECT 1 FROM information_schema.columns
                           WHERE table_name='orders' AND column_name='customerId') THEN
                    ALTER TABLE orders RENAME COLUMN "customerId" TO customer_id;
                END IF;
            END$$;
        """)
    print("Demo state reset: incident tables truncated, orders table schema restored.")


if __name__ == "__main__":
    main()
