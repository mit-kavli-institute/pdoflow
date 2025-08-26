import os

import pytest
import sqlalchemy as sa
from hypothesis import Verbosity, settings
from sqlalchemy import orm, pool

from pdoflow.io import Session
from pdoflow.models import Base
from pdoflow.utils import register_process_guards

settings.register_profile("ci", max_examples=1000)
settings.register_profile("dev", max_examples=100)
settings.register_profile(
    "debug", max_examples=100, verbosity=Verbosity.verbose
)
settings.load_profile(os.getenv("HYPOTHESIS_PROFILE", "default"))


def get_test_database_name(request):
    """Get a unique database name for the test session.

    Uses pytest-xdist worker ID if available, otherwise uses 'master'.
    """
    # Check if we're running under xdist
    worker_id = getattr(request.config, "workerinput", {}).get(
        "workerid", "master"
    )
    return f"lcdb_test_{worker_id}"


@pytest.fixture(scope="session")
def worker_database(request):
    """Create a database for this test worker for the entire session."""
    url = sa.URL.create(
        "postgresql+psycopg",
        database=os.getenv("POSTGRES_DB", "postgres"),
        username=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "testing"),
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
    )

    db_name = get_test_database_name(request)
    admin_engine = sa.create_engine(url, poolclass=sa.pool.NullPool)

    # Create the test database
    with admin_engine.connect().execution_options(
        isolation_level="AUTOCOMMIT"
    ) as conn:
        # Drop if exists (in case of previous unclean shutdown)
        conn.execute(sa.text(f"DROP DATABASE IF EXISTS {db_name}"))
        conn.execute(sa.text(f"CREATE DATABASE {db_name}"))

    # Yield the database configuration
    yield {
        "name": db_name,
        "host": url.host,
        "port": url.port,
        "user": url.username,
        "password": url.password,
    }

    # Cleanup: Drop the database
    with admin_engine.connect().execution_options(
        isolation_level="AUTOCOMMIT"
    ) as conn:
        # First, terminate all connections to the database
        conn.execute(
            sa.text(
                f"""
            SELECT pg_terminate_backend(pg_stat_activity.pid)
            FROM pg_stat_activity
            WHERE pg_stat_activity.datname = '{db_name}'
            AND pid != pg_backend_pid()
        """
            )
        )
        # Then drop the database
        conn.execute(sa.text(f"DROP DATABASE IF EXISTS {db_name}"))

    admin_engine.dispose()


@pytest.fixture
def db_session(worker_database):
    """
    Create a fixture which spawns a database connection and drops tables upon
    a test function's exit.
    """
    url = sa.URL.create(
        "postgresql+psycopg",
        database=worker_database["name"],
        username=worker_database["user"],
        password=worker_database["password"],
        host=worker_database["host"],
        port=worker_database["port"],
    )

    engine = sa.create_engine(url, poolclass=pool.NullPool)
    register_process_guards(engine)

    try:
        Base.metadata.create_all(bind=engine, checkfirst=True)
        Session.configure(bind=engine)
        with Session() as session:
            yield session
    finally:
        orm.close_all_sessions()
        Base.metadata.drop_all(bind=engine)
    engine.dispose()
