=======
Testing
=======

This guide covers testing strategies and practices for PDOFlow.

Testing Philosophy
-----------------

PDOFlow's test suite emphasizes:

- **Real database testing**: Using pytest-postgresql for authentic behavior
- **Property-based testing**: Using Hypothesis for edge case discovery
- **No mocking when possible**: Test against real implementations
- **Fast feedback**: Parallel test execution with pytest-xdist

Test Structure
-------------

The test suite is organized as follows:

.. code-block:: text

   tests/
   ├── conftest.py           # Pytest configuration and fixtures
   ├── strategies.py         # Hypothesis strategies
   ├── test_cli.py          # CLI command tests
   ├── test_cluster.py      # Worker and pool tests
   ├── test_models.py       # Database model tests
   ├── test_profiling.py    # Profiling system tests
   ├── test_registry.py     # Registry tests
   └── example_package/     # Test package for dynamic loading
       ├── __init__.py
       └── jobs.py

Running Tests
------------

Basic Test Execution
~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   # Run all tests
   pytest

   # Run specific test file
   pytest tests/test_models.py

   # Run specific test
   pytest tests/test_models.py::test_job_creation

   # Run with verbose output
   pytest -v

   # Run in parallel
   pytest -n auto

Using Tox
~~~~~~~~

Tox provides isolated test environments:

.. code-block:: bash

   # Run full test suite
   tox

   # Test specific Python version
   tox -e py311

   # Run with coverage
   tox -e clean,py311,report

   # Run type checking
   tox -e mypy

   # Run linting
   tox -e flake8

Coverage Reports
~~~~~~~~~~~~~~

.. code-block:: bash

   # Generate HTML coverage report
   pytest --cov=pdoflow --cov-report=html

   # View report
   open htmlcov/index.html

   # Check coverage threshold
   tox -e coverage-check

Writing Tests
------------

Basic Test Structure
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import pytest
   from pdoflow.models import JobRecord
   from pdoflow.status import JobStatus

   def test_job_status_transition():
       """Test that job status transitions correctly."""
       # Arrange
       job = JobRecord(
           positional_arguments=[1, 2],
           keyword_arguments={"key": "value"},
           status=JobStatus.waiting
       )

       # Act
       job.status = JobStatus.executing

       # Assert
       assert job.status == JobStatus.executing
       assert job.status != JobStatus.waiting

Using Fixtures
~~~~~~~~~~~~~

PDOFlow provides several test fixtures:

.. code-block:: python

   def test_with_database(db_session):
       """Test that requires database access."""
       from pdoflow.models import JobPosting

       # db_session is automatically provided
       posting = JobPosting(
           target_function="test_func",
           entry_point="module:func"
       )
       db_session.add(posting)
       db_session.commit()

       assert posting.id is not None

   def test_with_worker(cluster_process):
       """Test with a worker process."""
       # cluster_process fixture provides a configured worker
       assert cluster_process.batchsize == 10
       assert cluster_process.exception_logging == "warning"

Property-Based Testing
~~~~~~~~~~~~~~~~~~~~

Use Hypothesis for comprehensive testing:

.. code-block:: python

   from hypothesis import given, strategies as st
   from tests.strategies import job_record

   @given(job_record())
   def test_job_properties(job):
       """Test job invariants with random data."""
       # Properties that should always hold
       assert job.tries_remaining >= 0
       assert job.priority is not None
       assert isinstance(job.positional_arguments, list)
       assert isinstance(job.keyword_arguments, dict)

   @given(
       st.lists(st.integers(), min_size=1, max_size=100),
       st.dictionaries(st.text(), st.integers())
   )
   def test_job_serialization(args, kwargs):
       """Test job argument serialization."""
       job = JobRecord(
           positional_arguments=args,
           keyword_arguments=kwargs
       )

       # Should round-trip through JSON
       assert job.positional_arguments == args
       assert job.keyword_arguments == kwargs

Test Fixtures
------------

Database Fixtures
~~~~~~~~~~~~~~~

.. code-block:: python

   @pytest.fixture
   def db_session(postgresql):
       """Provide a database session for tests."""
       from pdoflow.io import make_engine
       from pdoflow.models import Base

       # Create tables
       engine = make_engine(postgresql.info)
       Base.metadata.create_all(engine)

       # Create session
       Session = sessionmaker(bind=engine)
       session = Session()

       yield session

       session.close()

   @pytest.fixture
   def sample_posting(db_session):
       """Create a sample job posting."""
       from pdoflow.models import JobPosting

       posting = JobPosting(
           target_function="test_function",
           entry_point="tests.example_package:test_function"
       )
       db_session.add(posting)
       db_session.commit()

       return posting

Worker Fixtures
~~~~~~~~~~~~~

.. code-block:: python

   @pytest.fixture
   def mock_worker():
       """Create a mock worker for testing."""
       from unittest.mock import Mock
       from pdoflow.cluster import ClusterProcess

       worker = Mock(spec=ClusterProcess)
       worker.batchsize = 10
       worker.process_job = Mock()

       return worker

   @pytest.fixture
   def worker_pool(postgresql):
       """Create a real worker pool."""
       from pdoflow.cluster import ClusterPool

       pool = ClusterPool(
           max_workers=2,
           batchsize=5,
           dbinfo=postgresql.info
       )

       yield pool

       pool.close()

Testing Strategies
-----------------

Testing Database Operations
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   def test_concurrent_job_selection(db_session):
       """Test that SKIP LOCKED prevents double processing."""
       from threading import Thread
       from pdoflow.models import JobRecord

       # Create jobs
       jobs = [
           JobRecord(positional_arguments=[i])
           for i in range(10)
       ]
       db_session.add_all(jobs)
       db_session.commit()

       selected_ids = []

       def select_jobs():
           # Simulate worker selecting jobs
           with db_session.begin():
               jobs = db_session.query(JobRecord).filter_by(
                   status=JobStatus.waiting
               ).limit(5).with_for_update(skip_locked=True).all()

               for job in jobs:
                   selected_ids.append(job.id)
                   job.status = JobStatus.executing

       # Run concurrent selections
       threads = [Thread(target=select_jobs) for _ in range(3)]
       for t in threads:
           t.start()
       for t in threads:
           t.join()

       # No job should be selected twice
       assert len(selected_ids) == len(set(selected_ids))

Testing Worker Behavior
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   def test_worker_failure_tracking(db_session):
       """Test that workers track failures correctly."""
       from pdoflow.cluster import ClusterProcess

       class FailingJob:
           def execute(self):
               raise ValueError("Intentional failure")

       worker = ClusterProcess(dbinfo=postgresql_info)
       worker._pre_run_init()

       # Process failing job
       job = FailingJob()
       job.posting_id = "test-posting"

       # Should track failure
       worker.process_job(job)
       assert worker._failure_cache._cache["test-posting"] == {job.id}

Testing CLI Commands
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   def test_cli_pool_command(cli_runner, postgresql):
       """Test the pool command."""
       from pdoflow.cli import pdoflow_main

       result = cli_runner.invoke(
           pdoflow_main,
           ['pool', '-w', '2', '--batchsize', '20']
       )

       assert result.exit_code == 0
       assert "Starting 2 workers" in result.output

   def test_cli_priority_stats(cli_runner, sample_jobs):
       """Test priority stats command."""
       result = cli_runner.invoke(
           pdoflow_main,
           ['priority-stats']
       )

       assert result.exit_code == 0
       assert "Priority Distribution" in result.output

Integration Tests
---------------

End-to-End Testing
~~~~~~~~~~~~~~~~

.. code-block:: python

   def test_complete_job_lifecycle(postgresql):
       """Test job from submission to completion."""
       from pdoflow import job, Registry
       from pdoflow.cluster import ClusterPool
       from pdoflow.io import Session
       from pdoflow.models import JobRecord

       # Define job
       @job(name="lifecycle_test")
       def test_job(x: int) -> int:
           return x * 2

       # Submit jobs
       posting_id, job_ids = Registry["lifecycle_test"].post_work(
           [(i,) for i in range(10)]
       )

       # Process with workers
       with ClusterPool(max_workers=2, dbinfo=postgresql.info) as pool:
           pool.await_posting_completion(posting_id, poll_time=0.1)

       # Verify completion
       with Session(postgresql.info) as session:
           completed = session.query(JobRecord).filter_by(
               posting_id=posting_id,
               status=JobStatus.done
           ).count()

           assert completed == 10

Performance Testing
~~~~~~~~~~~~~~~~~

.. code-block:: python

   import time
   import pytest

   @pytest.mark.slow
   def test_throughput(postgresql, large_job_set):
       """Test system throughput with many jobs."""
       from pdoflow.cluster import ClusterPool

       start_time = time.time()

       with ClusterPool(max_workers=8, batchsize=50) as pool:
           pool.await_posting_completion(
               large_job_set.posting_id,
               max_wait=60
           )

       elapsed = time.time() - start_time
       jobs_per_second = len(large_job_set.jobs) / elapsed

       # Should process at least 100 jobs/second
       assert jobs_per_second > 100

Testing Best Practices
--------------------

1. **Isolate Tests**: Each test should be independent
2. **Use Real Databases**: Avoid mocking database behavior
3. **Test Edge Cases**: Empty sets, null values, concurrent access
4. **Test Failures**: Ensure graceful error handling
5. **Performance Benchmarks**: Track performance regressions

Common Testing Patterns
---------------------

Testing Decorators
~~~~~~~~~~~~~~~~

.. code-block:: python

   def test_job_decorator():
       """Test @job decorator behavior."""
       from pdoflow import job, Registry

       @job(name="decorator_test")
       def my_function(x: int) -> int:
           return x + 1

       # Should register in Registry
       assert "decorator_test" in Registry

       # Should preserve function behavior
       assert my_function(5) == 6

Testing Profiling
~~~~~~~~~~~~~~~

.. code-block:: python

   def test_profiling_capture(db_session):
       """Test that profiling captures function calls."""
       from pdoflow.models import JobProfile, reflect_cProfile
       import cProfile

       def sample_function():
           total = 0
           for i in range(100):
               total += i
           return total

       # Profile execution
       pr = cProfile.Profile()
       pr.enable()
       result = sample_function()
       pr.disable()
       pr.create_stats()

       # Store profile
       profile = JobProfile(job_record_id=uuid4())
       db_session.add(profile)
       db_session.flush()

       reflect_cProfile(db_session, profile, pr.stats)
       db_session.commit()

       # Verify capture
       assert profile.total_calls > 0
       assert profile.total_time > 0

Debugging Tests
--------------

Using pytest debugging:

.. code-block:: bash

   # Drop into debugger on failure
   pytest --pdb

   # Show local variables on failure
   pytest -l

   # Capture print output
   pytest -s

   # Run last failed tests
   pytest --lf

   # Run failed first, then others
   pytest --ff

See Also
--------

- :doc:`contributing` - Contribution guidelines
- :doc:`architecture` - System architecture
- `pytest documentation <https://docs.pytest.org/>`_
- `Hypothesis documentation <https://hypothesis.readthedocs.io/>`_
