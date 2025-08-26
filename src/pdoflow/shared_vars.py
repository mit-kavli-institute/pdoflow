"""
Shared variables API for JobPosting instances.

This module provides functions to get and set shared variables
associated with JobPosting instances using PostgreSQL row-level locking
to ensure consistency in concurrent environments.
"""

import uuid
from typing import Any, Callable, Optional

import sqlalchemy as sa
from sqlalchemy import orm
from sqlalchemy.dialects.postgresql.json import JSONB
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.orm.session import Session

from pdoflow.models import JobPostingVariable


def get_shared_variable(
    db: orm.Session,
    posting_id: uuid.UUID,
    key: str,
    default: Optional[Any] = None,
    lock: bool = False,
) -> Any:
    """
    Get a shared variable value for a JobPosting.

    Parameters
    ----------
    db : orm.Session
        Database session to use for the query
    posting_id : uuid.UUID
        ID of the JobPosting
    key : str
        Variable key to retrieve
    default : Any, optional
        Default value to return if variable doesn't exist
    lock : bool, default False
        Whether to acquire a row lock (FOR UPDATE)

    Returns
    -------
    Any
        The variable value, or default if not found
    """
    q = JobPostingVariable.select().where(
        JobPostingVariable.posting_id == posting_id,
        JobPostingVariable.key == key,
    )
    if lock:
        q = q.with_for_update()

    var = db.scalar(q)
    return var.value if var else default


def set_shared_variable(
    db: orm.Session, posting_id: uuid.UUID, key: str, value: Any
) -> None:
    """
    Set a shared variable value for a JobPosting.

    This function will update an existing variable or create a new one
    if it doesn't exist. The operation is atomic.

    Parameters
    ----------
    db : orm.Session
        Database session to use for the operation
    posting_id : uuid.UUID
        ID of the JobPosting
    key : str
        Variable key to set
    value : Any
        Value to store (must be JSON-serializable)
    """
    # Try to update existing variable first
    stmt = (
        sa.update(JobPostingVariable)
        .where(
            JobPostingVariable.posting_id == posting_id,
            JobPostingVariable.key == key,
        )
        .values(value=value, updated_on=sa.func.now())
    )

    result = db.execute(stmt)

    if result.rowcount == 0:
        # Variable doesn't exist, create it
        var = JobPostingVariable(
            posting_id=posting_id,
            key=key,
            value=value,
        )
        db.add(var)

    db.flush()


def update_shared_variable(
    db: orm.Session,
    posting_id: uuid.UUID,
    key: str,
    modifying_value: Any,
    updater_func: Callable[[InstrumentedAttribute, Any], Any],
    default: Optional[Any] = None,
    jsonb_path: Optional[str] = None,
) -> Any:
    """
    Atomically update a shared variable using a SQL expression.

    This function provides atomic read-modify-write semantics using
    PostgreSQL's UPDATE...RETURNING statement, ensuring thread-safe
    updates without explicit locking.

    Parameters
    ----------
    db : orm.Session
        Database session to use for the operation
    posting_id : uuid.UUID
        ID of the JobPosting
    key : str
        Variable key to update
    modifying_value : Any
        Value to be used in the update operation
    updater_func : Callable[[InstrumentedAttribute, Any], Any]
        Function that takes the column attribute and modifying value,
        returns a SQLAlchemy expression for the update
    default : Any, optional
        Default value to use if variable doesn't exist
    jsonb_path : str, optional
        PostgreSQL JSONB path for nested updates (e.g., '{key}' or
        '{key,subkey}'). When provided, uses jsonb_set to update a
        specific path within the JSON

    Returns
    -------
    Any
        The new value after update

    Example
    -------
    >>> # Increment a counter atomically
    >>> new_value = update_shared_variable(
    ...     db,
    ...     posting_id,
    ...     "counter",
    ...     1,
    ...     lambda col, val: col + val,
    ...     default=0
    ... )
    >>> # Update nested JSON value
    >>> new_value = update_shared_variable(
    ...     db,
    ...     posting_id,
    ...     "config",
    ...     100,
    ...     lambda col, val: val,
    ...     jsonb_path='{max_retries}'
    ... )
    """
    # Get current value with lock
    base_q = sa.update(JobPostingVariable).where(
        JobPostingVariable.key == key,
        JobPostingVariable.posting_id == posting_id,
    )
    update_clause = updater_func(JobPostingVariable.value, modifying_value)

    if jsonb_path is None:
        stmt = base_q.values(
            value=update_clause, updated_on=sa.func.now()
        ).returning(JobPostingVariable.value)
    else:
        # Ensure path is in PostgreSQL array format
        # (e.g., '{key}' or '{key,subkey}')
        if not jsonb_path.startswith("{"):
            jsonb_path = "{" + jsonb_path + "}"

        # Handle case where updater_func returns a plain value
        # vs SQLAlchemy expression
        if hasattr(update_clause, "cast"):
            # It's a SQLAlchemy expression
            new_value_expr = update_clause.cast(sa.Text).cast(JSONB)
        else:
            # It's a plain value - convert to JSON using func.to_jsonb
            import json

            new_value_expr = sa.cast(
                sa.text(f"'{json.dumps(update_clause)}'"), JSONB
            )

        stmt = base_q.values(
            value=sa.func.jsonb_set(
                JobPostingVariable.value,
                sa.text(
                    f"'{jsonb_path}'"
                ),  # Path must be a quoted text literal
                new_value_expr,
                False,
            ),
            updated_on=sa.func.now(),
        ).returning(JobPostingVariable.value)

    new_value = db.scalar(stmt)

    # If no row was updated (variable doesn't exist),
    # create it with default value
    if new_value is None and default is not None:
        set_shared_variable(db, posting_id, key, default)
        # Now update it
        new_value = db.scalar(stmt)

    db.flush()
    return new_value


def delete_shared_variable(
    db: orm.Session, posting_id: uuid.UUID, key: str
) -> bool:
    """
    Delete a shared variable.

    Parameters
    ----------
    db : orm.Session
        Database session to use for the operation
    posting_id : uuid.UUID
        ID of the JobPosting
    key : str
        Variable key to delete

    Returns
    -------
    bool
        True if variable was deleted, False if it didn't exist
    """
    stmt = sa.delete(JobPostingVariable).where(
        JobPostingVariable.posting_id == posting_id,
        JobPostingVariable.key == key,
    )

    result = db.execute(stmt)
    db.flush()

    return result.rowcount > 0


def list_shared_variables(
    db: orm.Session, posting_id: uuid.UUID
) -> dict[str, Any]:
    """
    List all shared variables for a JobPosting.

    Parameters
    ----------
    db : orm.Session
        Database session to use for the query
    posting_id : uuid.UUID
        ID of the JobPosting

    Returns
    -------
    dict[str, Any]
        Dictionary mapping keys to values
    """
    q = JobPostingVariable.select().where(
        JobPostingVariable.posting_id == posting_id
    )

    variables = db.scalars(q).all()
    return {var.key: var.value for var in variables}


class Value:
    """
    A multiprocess-like shared value for JobPosting variables.

    Similar to Python's multiprocessing.Value, this class provides
    a shared value that can be accessed and modified across multiple
    processes. Unlike multiprocessing.Value, this implementation:
    - Uses PostgreSQL for persistence and coordination
    - Supports any JSON-serializable value (not just primitives)
    - Provides snapshot consistency rather than real-time updates

    This is useful for sharing state between distributed workers
    processing jobs from the same JobPosting.
    """

    def __init__(
        self,
        variable_name: str,
        posting_id: uuid.UUID,
        default_value: Any,
        session_factory: Callable[..., Session],
    ):
        """
        Initialize a shared value.

        Parameters
        ----------
        variable_name : str
            The name/key of the shared variable
        posting_id : uuid.UUID
            The JobPosting ID this variable belongs to
        default_value : Any
            Default value to use if the variable doesn't exist
        session_factory : Callable[..., Session]
            Factory function to create database sessions
        """
        self.session_factory = session_factory
        self.name = variable_name
        self.posting_id = posting_id
        self.default_value = default_value

    @property
    def value(self):
        """
        Get the current value of the shared variable.

        Returns a snapshot of the variable's value at the time of access.
        The value is not dynamically updated, so for rapidly changing
        values, each access will fetch the latest state from the database.

        Returns
        -------
        Any
            The current value of the variable, or default_value if not set
        """
        ...
