import os

import pytest

os.environ.setdefault("APP_ENV", "test")
os.environ.setdefault("LLM_PROVIDER", "mock")
# Default to a local Postgres if none is configured; e2e tests are skipped
# automatically (see test_e2e.py) if no database is reachable.
os.environ.setdefault(
    "DATABASE_URL", "postgresql+psycopg2://pipelinemedic:pipelinemedic@localhost:5432/pipelinemedic"
)

from app.db.base import Base, engine  # noqa: E402


def _db_available() -> bool:
    try:
        with engine.connect():
            return True
    except Exception:
        return False


@pytest.fixture(scope="session")
def db_available():
    return _db_available()


@pytest.fixture()
def clean_db(db_available):
    if not db_available:
        pytest.skip("Postgres not reachable; skipping DB-backed test.")
    Base.metadata.create_all(bind=engine)
    yield
    with engine.begin() as conn:
        for table in reversed(Base.metadata.sorted_tables):
            conn.execute(table.delete())
