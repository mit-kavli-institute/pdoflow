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
    updater_func: Callable[[Any], Any],
    default: Optional[Any] = None,
) -> Any:
    """
    Atomically update a shared variable using a function.

    This function provides atomic read-modify-write semantics by:
    1. Acquiring a row lock
    2. Reading the current value
    3. Applying the updater function
    4. Writing the new value

    Parameters
    ----------
    db : orm.Session
        Database session to use for the operation
    posting_id : uuid.UUID
        ID of the JobPosting
    key : str
        Variable key to update
    updater_func : callable
        Function that takes the current value and returns the new value
    default : Any, optional
        Default value to use if variable doesn't exist

    Returns
    -------
    Any
        The new value after update

    Example
    -------
    >>> def increment(x):
    ...     return (x or 0) + 1
    >>> new_value = update_shared_variable(
    ...     db, posting_id, "counter", increment
    ... )
    """
    # Get current value with lock
    current_value = get_shared_variable(
        db, posting_id, key, default=default, lock=True
    )

    # Apply updater function
    new_value = updater_func(current_value)

    # Set the new value
    set_shared_variable(db, posting_id, key, new_value)

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
