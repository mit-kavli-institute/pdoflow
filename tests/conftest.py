import os

import pytest
from hypothesis import Verbosity, settings
from sqlalchemy import URL, create_engine, pool

from pdoflow.io import Session
from pdoflow.models import Base
from pdoflow.utils import register_process_guards

settings.register_profile("ci", max_examples=1000)
settings.register_profile("dev", max_examples=100)
settings.register_profile(
    "debug", max_examples=100, verbosity=Verbosity.verbose
)
settings.load_profile(os.getenv("HYPOTHESIS_PROFILE", "default"))


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
