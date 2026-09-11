"""Central application settings, loaded from environment variables."""
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Database
    database_url: str = "postgresql+psycopg2://pipelinemedic:pipelinemedic@postgres:5432/pipelinemedic"

    # Kafka
    kafka_bootstrap_servers: str = "kafka:9092"
    kafka_topic_orders_raw: str = "orders.raw"
    kafka_topic_orders_validated: str = "orders.validated"
    kafka_topic_dlq: str = "pipeline.dlq"
    kafka_topic_incidents: str = "pipeline.incidents"
    kafka_topic_audit: str = "pipeline.audit"
    kafka_consumer_group: str = "pipelinemedic-backend"

    # LLM
    llm_provider: str = "mock"  # mock | openai | azure_openai
    openai_api_key: str | None = None
    openai_model: str = "gpt-4o-mini"
    azure_openai_endpoint: str | None = None
    azure_openai_api_key: str | None = None
    azure_openai_deployment: str | None = None

    # Airflow (optional integration)
    airflow_base_url: str = "http://airflow-webserver:8080"
    airflow_username: str = "admin"
    airflow_password: str = "admin"

    # Flink (optional integration)
    flink_jobmanager_url: str = "http://flink-jobmanager:8081"
    # When a real Flink job is running the orders.raw -> orders.validated/dlq
    # validation (see flink/jobs/order_validator_job.py), set this to false so
    # PipelineMonitor's in-process "stream-validator" thread does not also
    # consume orders.raw and double-write validated/dlq records. Defaults to
    # true (the lightweight fallback path) for constrained environments where
    # the Flink cluster isn't brought up.
    pipeline_monitor_stream_validator_enabled: bool = True

    # App
    app_env: str = "development"
    log_level: str = "INFO"
    lag_warning_threshold: int = 500
    lag_critical_threshold: int = 5000
    consumer_poll_interval_seconds: float = 2.0

    # Notifications (see backend/app/services/notifications.py)
    # console: logs a structured message via the JSON logger (default, no
    # credentials required). smtp: sends real email via smtplib -- requires
    # the user's own SMTP credentials, not demonstrated with a real mailbox
    # in this repo. See docs/safety-model.md for the full write-up.
    notification_channel: str = "console"  # console | smtp
    smtp_host: str | None = None
    smtp_port: int = 587
    smtp_username: str | None = None
    smtp_password: str | None = None
    smtp_from_addr: str | None = None
    smtp_to_addr: str | None = None
    smtp_use_tls: bool = True

    # Approve-link tokens (see backend/app/services/approval_tokens.py)
    # Secret key for signing single-use approve-link tokens (itsdangerous).
    # MUST be overridden with a real secret outside local development.
    approval_token_secret: str = "dev-insecure-secret-change-me"
    approval_token_max_age_seconds: int = 1800  # 30 minutes
    # Base URL of the dashboard the approve-link points at. Vite's default
    # dev server port is 5173 (see dashboard/vite.config.ts); docker-compose
    # publishes the dashboard container separately -- override in .env for
    # non-local deployments.
    dashboard_base_url: str = "http://localhost:5173"


settings = Settings()
