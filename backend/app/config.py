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

    # App
    app_env: str = "development"
    log_level: str = "INFO"
    lag_warning_threshold: int = 500
    lag_critical_threshold: int = 5000
    consumer_poll_interval_seconds: float = 2.0


settings = Settings()
