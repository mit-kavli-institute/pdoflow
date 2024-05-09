import pytest
from sqlalchemy import URL, create_engine, pool
from pdoflow.models import Base
from pdoflow.utils import register_process_guards
from pdoflow.io import Session

@pytest.fixture
def db_session(postgresql):
    """
    Create a fixture which spawns a database connection and drops tables upon
    a test function's exit.
    """
    url = URL.create(
        "postgresql",
        database=postgresql.info.db_name,
        username=postgresql.info.user,
        password=postgresql.info.password,
        host=postgresql.info.host,
        port=postgresql.info.port
    )

    engine = create_engine(url, poolclass=pool.NullPool)
    register_process_guards(engine)

    Base.metadata.create_all(bind=engine)

    Session.configure(bind=engine)
    yield Session()

    Base.metadata.drop_all(bind=engine)
