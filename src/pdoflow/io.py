import os

import configurables as conf
import sqlalchemy as sa
from sqlalchemy import orm

from pdoflow.utils import register_process_guards

DEFAULT_CONFIG_PATH = "~/.config/pdoflow/db.conf"


def configure_engine_from_env(**engine_kwargs):
    """
    Configure an SQLAlchemy engine from environment variables.
    Used primarily in CI/testing environments.
    """
    url = sa.URL.create(
        "postgresql+psycopg",
        database=os.getenv("POSTGRES_DB", "postgres"),
        username=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", ""),
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
    )

    engine = register_process_guards(
        sa.create_engine(url, poolclass=sa.pool.NullPool, **engine_kwargs)
    )
    return engine


@conf.configurable("pdoflow")
@conf.param("database_name")
@conf.param("username")
@conf.param("password")
@conf.option("host", default="localhost")
@conf.option("port", type=int, default=5432)
def configure_engine(
    database_name, username, password, host, port, **engine_kwargs
):
    """
    Configure an SQLAlchemy engine object from a configuration file.
    """

    url = sa.URL.create(
        "postgresql+psycopg",
        database=database_name,
        username=username,
        password=password,
        host=host,
        port=port,
    )

    engine = register_process_guards(
        sa.create_engine(url, poolclass=sa.pool.NullPool, **engine_kwargs)
    )
    return engine


Session = orm.sessionmaker(expire_on_commit=False)

# Try to configure session in order of precedence:
# 1. Environment variables (for CI/testing)
# 2. Configuration file
# 3. Leave unbound for manual configuration
if os.getenv("POSTGRES_HOST"):
    Session.configure(bind=configure_engine_from_env())
else:
    try:
        Session.configure(bind=configure_engine(DEFAULT_CONFIG_PATH))
    except FileNotFoundError:
        # Keep session unbound for manual configuration later
        pass
