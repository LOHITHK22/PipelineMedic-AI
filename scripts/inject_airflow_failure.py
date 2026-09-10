#!/usr/bin/env python
"""Simulates an Airflow data-quality task failure by dropping (renaming) an
expected column from the orders table that the DAG's schema-check task
depends on, then triggers a DAG run.

This directly exercises airflow/dags/order_data_quality_dag.py's
`check_orders_schema` task, which expects the `customer_id` column to exist.
"""
import argparse
import os

import psycopg2


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", default=os.environ.get(
        "PIPELINEMEDIC_DB_DSN", "dbname=pipelinemedic user=pipelinemedic password=pipelinemedic host=localhost port=5432"
    ))
    args = parser.parse_args()

    conn = psycopg2.connect(args.dsn)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                order_id VARCHAR(64) PRIMARY KEY,
                customer_id VARCHAR(64) NOT NULL,
                amount NUMERIC(10,2) NOT NULL,
                currency VARCHAR(8) NOT NULL,
                event_time TIMESTAMPTZ NOT NULL
            );
        """)
        cur.execute("""
            DO $$
            BEGIN
                IF EXISTS (SELECT 1 FROM information_schema.columns
                           WHERE table_name='orders' AND column_name='customer_id') THEN
                    ALTER TABLE orders RENAME COLUMN customer_id TO customerId;
                END IF;
            END$$;
        """)
    print("Renamed orders.customer_id -> orders.customerId. "
          "Trigger the 'order_data_quality_dag' DAG in Airflow to see the schema-check task fail.")


if __name__ == "__main__":
    main()
