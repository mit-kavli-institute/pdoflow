"""
Tests for JobPosting shared variables functionality.
"""

import sqlalchemy as sa
from hypothesis import HealthCheck, given, settings

from pdoflow.models import JobPostingVariable
from pdoflow.shared_vars import (
    delete_shared_variable,
    get_shared_variable,
    list_shared_variables,
    set_shared_variable,
    update_shared_variable,
)

from . import strategies as pf_st


@given(pf_st.job_postings())
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_set_and_get_variable(db_session, job_posting):
    """Test basic set and get operations."""
    # Set a variable
    set_shared_variable(db_session, job_posting.id, "test_key", {"value": 42})
    # Get the variable
    value = get_shared_variable(db_session, job_posting.id, "test_key")
    assert value == {"value": 42}


@given(pf_st.job_postings())
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_get_nonexistent_variable(db_session, job_posting):
    """Test getting a variable that doesn't exist."""
    value = get_shared_variable(db_session, job_posting.id, "nonexistent")
    assert value is None

    # Test with default
    value = get_shared_variable(
        db_session,
        job_posting.id,
        "nonexistent",
        default="default_value",
    )
    assert value == "default_value"


@given(pf_st.job_postings())
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_update_existing_variable(db_session, job_posting):
    """Test updating an existing variable."""
    # Set initial value
    set_shared_variable(db_session, job_posting.id, "counter", {"count": 1})
    db_session.commit()

    # Update the value
    set_shared_variable(db_session, job_posting.id, "counter", {"count": 2})
    db_session.commit()

    # Verify update
    value = get_shared_variable(db_session, job_posting.id, "counter")
    assert value == {"count": 2}

    # Verify only one record exists
    count = db_session.scalar(
        sa.select(sa.func.count())
        .select_from(JobPostingVariable)
        .where(JobPostingVariable.posting_id == job_posting.id)
    )
    assert count == 1


@given(pf_st.job_postings())
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_delete_variable(db_session, job_posting):
    """Test deleting a variable."""
    # Set a variable
    data = {"data": "test"}
    set_shared_variable(db_session, job_posting.id, "to_delete", data)
    db_session.commit()

    # Delete it
    deleted = delete_shared_variable(db_session, job_posting.id, "to_delete")
    db_session.commit()
    assert deleted is True

    # Verify it's gone
    value = get_shared_variable(db_session, job_posting.id, "to_delete")
    assert value is None

    # Try deleting non-existent variable
    deleted = delete_shared_variable(db_session, job_posting.id, "nonexistent")
    assert deleted is False


@given(pf_st.job_postings())
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_list_variables(db_session, job_posting):
    """Test listing all variables for a posting."""
    # Set multiple variables
    set_shared_variable(db_session, job_posting.id, "var1", {"value": 1})
    set_shared_variable(db_session, job_posting.id, "var2", {"value": 2})
    set_shared_variable(db_session, job_posting.id, "var3", {"value": 3})
    db_session.commit()

    # List all variables
    variables = list_shared_variables(db_session, job_posting.id)
    assert len(variables) == 3
    assert variables["var1"] == {"value": 1}
    assert variables["var2"] == {"value": 2}
    assert variables["var3"] == {"value": 3}


@given(pf_st.job_postings())
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_update_with_function(db_session, job_posting):
    """Test atomic update with a function."""
    # Initialize counter
    set_shared_variable(db_session, job_posting.id, "counter", {"count": 0})
    db_session.commit()

    # Define updater function
    def increment_counter(value):
        value["count"] += 1
        return value

    # Update using function
    new_value = update_shared_variable(
        db_session, job_posting.id, "counter", increment_counter
    )
    db_session.commit()

    assert new_value == {"count": 1}

    # Verify in database
    value = get_shared_variable(db_session, job_posting.id, "counter")
    assert value == {"count": 1}


@given(pf_st.job_postings())
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_update_nonexistent_with_default(db_session, job_posting):
    """Test updating a non-existent variable with default."""

    def set_initial(value):
        return {"initialized": True, "value": 100}

    new_value = update_shared_variable(
        db_session,
        job_posting.id,
        "new_var",
        set_initial,
        default={"initialized": False},
    )
    db_session.commit()

    assert new_value == {"initialized": True, "value": 100}


@given(pf_st.job_postings())
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_cascade_delete(db_session, job_posting):
    """Test that variables are deleted when posting is deleted."""
    # Set some variables
    set_shared_variable(db_session, job_posting.id, "var1", {"data": 1})
    set_shared_variable(db_session, job_posting.id, "var2", {"data": 2})
    db_session.commit()

    # Delete the posting
    db_session.delete(job_posting)
    db_session.commit()

    # Verify variables are gone
    count = db_session.scalar(
        sa.select(sa.func.count()).select_from(JobPostingVariable)
    )
    assert count == 0
