import os

import pytest

os.environ.setdefault("APP_ENV", "test")
os.environ.setdefault("LLM_PROVIDER", "mock")
# Default to a local Postgres if none is configured; e2e tests are skipped
# automatically (see test_e2e.py) if no database is reachable.
os.environ.setdefault(
    "DATABASE_URL", "postgresql+psycopg2://pipelinemedic:pipelinemedic@localhost:5432/pipelinemedic"
)

from app.db.base import Base, apply_lightweight_schema_patches, engine  # noqa: E402


def _db_available() -> bool:
    try:
        with engine.connect():
            return True
    except Exception:
        return False


@pytest.fixture(scope="session")
def db_available():
    return _db_available()


def _truncate_all():
    with engine.begin() as conn:
        for table in reversed(Base.metadata.sorted_tables):
            conn.execute(table.delete())


@pytest.fixture()
def clean_db(db_available):
    if not db_available:
        pytest.skip("Postgres not reachable; skipping DB-backed test.")
    Base.metadata.create_all(bind=engine)
    apply_lightweight_schema_patches()
    # Truncate BEFORE the test too, not just after: this fixture's teardown
    # only cleans up after tests that ran through it, so any data left by an
    # external process using the same DATABASE_URL between pytest runs (e.g.
    # manual verification against a shared local/dev Postgres instance)
    # would otherwise leak into the first test that uses this fixture.
    _truncate_all()
    yield
    _truncate_all()
