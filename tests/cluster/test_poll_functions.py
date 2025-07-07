"""
Tests for polling functions in the cluster module.
"""

import itertools
from math import isnan

import sqlalchemy as sa
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from pdoflow import cluster, models
from pdoflow.io import Session

from .. import strategies as pdo_st


@given(pdo_st.job_postings())
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_poll_posting_percent_empty(db_session, posting: models.JobPosting):
    """Test poll_posting_percent with a posting that has no jobs."""
    with db_session:
        posting = db_session.merge(posting)
        posting_id = posting.id

    # Take first few values from the generator
    percentages = list(
        itertools.islice(cluster.poll_posting_percent(posting_id), 3)
    )

    assert len(percentages) == 3
    for percent in percentages:
        # Empty postings should yield NaN
        assert isnan(percent)


@given(pdo_st.posting_with_records())
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_poll_posting_percent_with_records(
    db_session, posting: models.JobPosting
):
    """Test poll_posting_percent with various job completion states."""
    with db_session:
        posting = db_session.merge(posting)
        posting_id = posting.id
        expected_percent = posting.percent_done

    # Take first few values from the generator
    percentages = list(
        itertools.islice(cluster.poll_posting_percent(posting_id), 3)
    )

    assert len(percentages) == 3
    for percent in percentages:
        if isnan(expected_percent):
            assert isnan(percent)
        else:
            assert percent == expected_percent


@given(st.uuids(version=4))
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_poll_posting_percent_nonexistent(db_session, nonexistent_id):
    """Test poll_posting_percent with a non-existent posting ID."""
    # Take first few values from the generator
    percentages = list(
        itertools.islice(cluster.poll_posting_percent(nonexistent_id), 3)
    )

    assert len(percentages) == 3
    for percent in percentages:
        # Non-existent postings should yield 0.0
        assert percent == 0.0


@given(pdo_st.posting_with_records(min_size=1, max_size=10))
@settings(
    suppress_health_check=[HealthCheck.function_scoped_fixture], deadline=None
)
def test_poll_posting_percent_consistency(
    db_session, posting: models.JobPosting
):
    """Test that poll_posting_percent yields consistent values with
    database."""
    with db_session:
        posting = db_session.merge(posting)
        posting_id = posting.id

    # Get values from generator
    gen_percentages = list(
        itertools.islice(cluster.poll_posting_percent(posting_id), 5)
    )

    # All values should be the same since we're not modifying the database
    assert all(p == gen_percentages[0] for p in gen_percentages)

    # Verify against database query
    with Session() as session:
        db_percent = session.scalar(
            sa.select(models.JobPosting.percent_done).where(
                models.JobPosting.id == posting_id
            )
        )

        for gen_percent in gen_percentages:
            if db_percent is None:
                assert gen_percent == 0.0
            elif isnan(db_percent):
                assert isnan(gen_percent)
            else:
                assert gen_percent == db_percent


@given(pdo_st.posting_with_records(min_size=2, max_size=5))
@settings(
    suppress_health_check=[HealthCheck.function_scoped_fixture], deadline=None
)
def test_poll_posting_percent_range(db_session, posting: models.JobPosting):
    """Test that poll_posting_percent yields values in valid range."""
    with db_session:
        posting = db_session.merge(posting)
        posting_id = posting.id

    # Get a few values from generator
    percentages = list(
        itertools.islice(cluster.poll_posting_percent(posting_id), 3)
    )

    for percent in percentages:
        if not isnan(percent):
            # Percentage should be between 0 and 100
            assert 0.0 <= percent <= 100.0
