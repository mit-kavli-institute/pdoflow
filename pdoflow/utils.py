"""
This module defines functions which are primarily for internal quality
of life utility logic.

Ideally these functions should be made as pure as possible.
"""
import inspect
import os
import typing
from functools import lru_cache

import deal
import sqlalchemy as sa
from loguru import logger


def get_module_path(obj: typing.Any) -> str:
    """
    Attempt to resolve the file source of the given object.
    """
    module = inspect.getmodule(obj)
    try:
        if module is None or module.__file__ is None:
            raise ValueError
        return f"{module.__name__}.{obj.__name__}"
    except (AttributeError, ValueError):
        raise ValueError(
            f"{obj} not tied to a module defined in a file. Object likely"
            "dynamically defined in an interactive shell and cannot be"
            "shared across computation nodes."
        )


def connect(dbapi_connection, connection_record):
    connection_record.info["pid"] = os.getpid()


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


@deal.pre(
    lambda path: all(ord(c) < 128 for c in path),
    message="Given entry point must be ascii",
)
@lru_cache
def load_function(path: str):
    module_path, func_name = path.rsplit(".", maxsplit=1)
    module = __import__(module_path, fromlist=[func_name])
    function = getattr(module, func_name)
    return function


def make_warning_logger(logging_level: str):
    func = getattr(logger, logging_level, logger.debug)

    def warning_logger(message, category, filename, lineno, **_):
        func(f"{category}: {message} {filename}:{lineno}")

    return warning_logger
