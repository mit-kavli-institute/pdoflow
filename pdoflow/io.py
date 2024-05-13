import configurables as conf
import sqlalchemy as sa
from sqlalchemy import orm

from pdoflow.utils import register_process_guards

DEFAULT_CONFIG_PATH = "~/.config/pdoflow/db.conf"


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


Session = orm.sessionmaker()

try:
    Session.configure(bind=configure_engine(DEFAULT_CONFIG_PATH))
except FileNotFoundError:
    # Keep session unbound for manual configuration later
    pass
