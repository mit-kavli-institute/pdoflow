import pytest
from sqlalchemy import URL, create_engine, pool

from pdoflow.io import Session
from pdoflow.models import Base
from pdoflow.utils import register_process_guards


@pytest.fixture
def db_session(postgresql):
    """
    Create a fixture which spawns a database connection and drops tables upon
    a test function's exit.
    """
    url = URL.create(
        "postgresql+psycopg",
        database=postgresql.info.dbname,
        username=postgresql.info.user,
        password=postgresql.info.password,
        host=postgresql.info.host,
        port=postgresql.info.port,
    )

    engine = create_engine(url, poolclass=pool.NullPool)
    register_process_guards(engine)

    Base.metadata.create_all(bind=engine, checkfirst=True)

    Session.configure(bind=engine)
    yield Session()
