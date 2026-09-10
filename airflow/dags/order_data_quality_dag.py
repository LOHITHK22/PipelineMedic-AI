"""order_data_quality_dag

A realistic ETL / data-quality DAG: validates that the `orders` table in
PostgreSQL has the columns the downstream analytics layer expects, then runs
a basic completeness check (no NULLs in required columns for recent rows).

This DAG is intentionally simple and self-contained so it can be broken by
scripts/inject_airflow_failure.py (which renames `customer_id` to
`customerId`) and repaired by PipelineMedic AI's `rerun_airflow_task` tool
once the underlying issue is fixed.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

EXPECTED_COLUMNS = {"order_id", "customer_id", "amount", "currency", "event_time"}

DB_DSN = os.environ.get(
    "PIPELINEMEDIC_DB_DSN",
    "dbname=pipelinemedic user=pipelinemedic password=pipelinemedic host=postgres port=5432",
)


def check_orders_schema(**context):
    import psycopg2

    conn = psycopg2.connect(DB_DSN)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name = 'orders';"
            )
            actual_columns = {row[0] for row in cur.fetchall()}
    finally:
        conn.close()

    missing = EXPECTED_COLUMNS - actual_columns
    if missing:
        raise ValueError(
            f"orders table is missing expected column(s): {sorted(missing)}. "
            f"Actual columns: {sorted(actual_columns)}. This likely indicates upstream schema drift "
            f"that PipelineMedic AI should detect and repair before this task is rerun."
        )
    context["ti"].xcom_push(key="checked_columns", value=sorted(actual_columns))


def check_orders_completeness(**context):
    import psycopg2

    conn = psycopg2.connect(DB_DSN)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM orders WHERE customer_id IS NULL OR amount IS NULL;")
            null_count = cur.fetchone()[0]
    finally:
        conn.close()

    if null_count > 0:
        raise ValueError(f"{null_count} order row(s) have NULL customer_id or amount.")


default_args = {
    "owner": "pipelinemedic",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="order_data_quality_dag",
    description="Validates orders table schema and completeness for the analytics pipeline.",
    default_args=default_args,
    schedule_interval="*/15 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["pipelinemedic", "data-quality"],
) as dag:
    schema_check = PythonOperator(
        task_id="check_orders_schema",
        python_callable=check_orders_schema,
    )

    completeness_check = PythonOperator(
        task_id="check_orders_completeness",
        python_callable=check_orders_completeness,
    )

    schema_check >> completeness_check
