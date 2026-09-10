-- Table backing airflow/dags/order_data_quality_dag.py's schema + completeness
-- checks. This is a separate, simple analytics-style table from the `orders`
-- semantics implied by orders.raw/orders.validated Kafka topics -- it exists
-- purely so the Airflow DAG has a real Postgres table to validate against.
\connect pipelinemedic

CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(64) PRIMARY KEY,
    customer_id VARCHAR(64) NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    currency VARCHAR(8) NOT NULL,
    event_time TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

INSERT INTO orders (order_id, customer_id, amount, currency, event_time) VALUES
    ('seed-order-1', 'seed-customer-1', 42.50, 'USD', now()),
    ('seed-order-2', 'seed-customer-2', 19.99, 'EUR', now()),
    ('seed-order-3', 'seed-customer-3', 108.00, 'GBP', now())
ON CONFLICT (order_id) DO NOTHING;
