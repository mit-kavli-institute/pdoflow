"""Shared fixtures and utilities for cluster tests."""

import uuid
from datetime import datetime
from unittest.mock import MagicMock, Mock

import pytest

from pdoflow import models, registry, status


@pytest.fixture
def mock_job_record():
    """Factory for creating mock JobRecord objects."""

    def _create_job_record(**kwargs):
        defaults = {
            "id": uuid.uuid4(),
            "posting_id": uuid.uuid4(),
            "pos_args": [1, 2, 3],
            "kwargs": {"key": "value"},
            "priority": 0,
            "tries_remaining": 3,
            "status": status.JobStatus.waiting,
            "exited_ok": None,
            "created_on": datetime.now(),
            "work_started_on": None,
            "completed_on": None,
        }
        defaults.update(kwargs)

        job = Mock(spec=models.JobRecord)
        for key, value in defaults.items():
            setattr(job, key, value)

        # Add methods
        job.execute = Mock()
        job.mark_as_bad = Mock()
        job.posting = Mock(id=defaults["posting_id"])

        # Add time_elapsed property
        if defaults.get("work_started_on") and defaults.get("completed_on"):
            elapsed = defaults["completed_on"] - defaults["work_started_on"]
            job.time_elapsed = Mock(
                total_seconds=Mock(return_value=elapsed.total_seconds())
            )
        else:
            job.time_elapsed = Mock(total_seconds=Mock(return_value=1.5))

        return job

    return _create_job_record


@pytest.fixture
def mock_job_posting():
    """Factory for creating mock JobPosting objects."""

    def _create_job_posting(**kwargs):
        defaults = {
            "id": uuid.uuid4(),
            "target_function": "test_function",
            "entry_point": "tests.example_package.foo",
            "status": status.PostingStatus.executing,
            "poster": None,
            "created_on": datetime.now(),
            "percent_done": 0.0,
        }
        defaults.update(kwargs)

        posting = Mock(spec=models.JobPosting)
        for key, value in defaults.items():
            setattr(posting, key, value)

        return posting

    return _create_job_posting


@pytest.fixture
def mock_db_session():
    """Create a mock database session with common query patterns."""
    session = MagicMock()

    # Mock common query patterns
    session.scalar = MagicMock()
    session.scalars = MagicMock()
    session.execute = MagicMock()
    session.commit = MagicMock()
    session.rollback = MagicMock()
    session.add = MagicMock()
    session.merge = MagicMock()

    # Make it work as a context manager
    session.__enter__ = MagicMock(return_value=session)
    session.__exit__ = MagicMock(return_value=False)

    return session


@pytest.fixture
def mock_cluster_worker():
    """Factory for creating test worker processes."""

    def _create_worker(**kwargs):
        defaults = {
            "daemon": True,
            "exception_logging": "warning",
            "warning_logging": "debug",
            "batchsize": 10,
        }
        defaults.update(kwargs)

        worker = MagicMock()
        worker.is_alive = MagicMock(return_value=True)
        worker.start = MagicMock()
        worker.terminate = MagicMock()
        worker.close = MagicMock()

        for key, value in defaults.items():
            setattr(worker, key, value)

        return worker

    return _create_worker


@pytest.fixture
def cluster_test_registry():
    """Create an isolated registry for tests."""
    # Create a new registry instance
    test_registry = registry.JobRegistry()
    # Clear it to ensure it's empty
    test_registry.clear_registry()
    yield test_registry
    # Clean up after test
    test_registry.clear_registry()


@pytest.fixture
def example_function():
    """A simple test function for job registration."""

    def test_func(x, y=10):
        return x + y

    return test_func


@pytest.fixture(autouse=True)
def clear_registry():
    """Automatically clear the global registry before each test."""
    registry.Registry.clear_registry()
    yield
    registry.Registry.clear_registry()
