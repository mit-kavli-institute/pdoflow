"""
This module defines functions which are primarily for internal quality
of life utility logic.

Ideally these functions should be made as pure as possible.
"""
import os
import typing
import inspect
import pathlib

import sqlalchemy as sa


def get_module_path(obj: typing.Any) -> pathlib.Path:
    """
    Attempt to resolve the file source of the given object.
    """
    module = inspect.getmodule(obj)
    try:
        if module is None or module.__file__ is None:
            raise ValueError
        return pathlib.Path(module.__file__)
    except (AttributeError, ValueError):
        raise ValueError(
            f"{obj} not tied to a module defined in a file. Object likely"
            "dynamically defined in an interactive shell and cannot be"
            "shared across computation nodes."
        )


def connect(dbapi_connection, connection_record):
    connection_record["pid"] = os.getpid()


def checkout(dbapi_connection, connection_record, connection_proxy):
    pid = os.getpid()
    if connection_record.info["pid"] != pid:
        # Mismatched pid, meaning session has been pickled and sent to
        # another process.

        # Reset connection by making it None
        connection_record.connection = connection_proxy.connection = None
        raise sa.exc.DisconnectionError(
            "Attempting to disassociate database connection "
            "from parent process"
        )


def register_process_guards(engine: sa.Engine):
    """Add SQLAlchemy process guards to the given engine"""

    sa.event.listens_for(engine, "connect")(connect)
    sa.event.listens_for(engine, "checkout")(checkout)

    return engine
