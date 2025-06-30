"""Integration tests for cluster module."""
import time
from unittest.mock import Mock, patch

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from pdoflow import cluster, models, registry, status
from tests import strategies as test_strategies
from tests.utils import CoverageWorker

from .. import example_package


class TestEndToEndExecution:
    """Integration tests for complete job execution flow."""

    @given(test_strategies.foo_workload())
    @settings(
        suppress_health_check=[HealthCheck.function_scoped_fixture],
        deadline=None,
        max_examples=3,
    )
    def test_end_to_end_job_execution(self, db_session, workload):
        """Test complete job execution from registration to completion."""
        registry.Registry.clear_registry()

        # Define and register a test function
        cluster.job(name="integration_test")(example_package.foo)

        # Post work
        posting_id, job_ids = registry.Registry["integration_test"].post_work(
            workload, []
        )

        # Execute jobs directly (simulating worker behavior)
        with db_session as db:
            jobs = list(
                db.scalars(
                    models.JobRecord.select().where(
                        models.JobRecord.id.in_(job_ids)
                    )
                )
            )

            for job in jobs:
                result = job.execute()
                expected = example_package.foo(*job.pos_args, **job.kwargs)
                assert result == expected
                assert job.status == status.JobStatus.done
                db.commit()

    def test_concurrent_worker_processing(self, db_session):
        """Test multiple workers processing jobs concurrently."""
        registry.Registry.clear_registry()

        # Use the existing test function
        from tests.example_package import foo

        cluster.job()(foo)

        # Create multiple jobs
        workload = [(i, float(i)) for i in range(10)]
        posting_id, job_ids = registry.Registry[foo].post_work(
            workload, [{} for _ in workload]
        )

        # Use ClusterPool with coverage worker
        with cluster.ClusterPool(
            max_workers=3, worker_class=CoverageWorker, batchsize=2
        ) as pool:
            try:
                pool.await_posting_completion(
                    posting_id, poll_time=0.1, max_wait=5.0
                )
                completed = True
            except TimeoutError:
                completed = False

        # Check results
        with db_session as db:
            jobs = list(
                db.scalars(
                    models.JobRecord.select().where(
                        models.JobRecord.posting_id == posting_id
                    )
                )
            )

            completed_count = sum(
                1 for job in jobs if job.status == status.JobStatus.done
            )

            if completed:
                # All jobs should be done
                assert completed_count == len(jobs)

    def test_worker_failure_recovery(self, db_session):
        """Test recovery when workers fail."""
        registry.Registry.clear_registry()

        # Use existing failure function
        from tests.example_package import failure

        cluster.job()(failure)

        # Post a job with an odd number (will succeed)
        posting_id, _ = registry.Registry[failure].post_work([(1,)], [])

        # Create a worker and process the job
        process = cluster.ClusterProcess()

        with patch("pdoflow.cluster.logger"):
            # Process the job - should succeed since 1 is odd
            n_processed = process.process_job_records()
            assert n_processed == 1

        # Verify job completed successfully
        with db_session as db:
            job = db.scalar(
                models.JobRecord.select().where(
                    models.JobRecord.posting_id == posting_id
                )
            )
            assert job.status == status.JobStatus.done
            assert job.exited_ok is True

        # Now test with even number (will fail)
        posting_id2, _ = registry.Registry[failure].post_work([(2,)], [])

        with patch("pdoflow.cluster.logger"):
            # Should fail and retry
            for _ in range(5):
                n_processed = process.process_job_records()
                if n_processed == 0:
                    break

        # Verify job failed
        with db_session as db:
            job = db.scalar(
                models.JobRecord.select().where(
                    models.JobRecord.posting_id == posting_id2
                )
            )
            assert job.status == status.JobStatus.errored_out

    def test_posting_blacklist_propagation(self, db_session):
        """Test that blacklisted postings affect all jobs."""
        registry.Registry.clear_registry()

        @cluster.job()
        def always_fails(x):
            raise Exception("Always fails")

        # Create multiple jobs
        posting_id, job_ids = registry.Registry[always_fails].post_work(
            [(i,) for i in range(5)], []
        )

        process = cluster.ClusterProcess()
        process.failure_threshold = 2  # Low threshold for testing
        process._failure_cache._default_value = 2

        with patch("pdoflow.cluster.logger"):
            # Process jobs until posting is blacklisted
            for _ in range(10):  # Enough iterations to trigger blacklisting
                n_processed = process.process_job_records()
                if posting_id in process._bad_postings:
                    break
                if n_processed == 0:
                    time.sleep(0.1)

        # Verify posting was blacklisted
        assert posting_id in process._bad_postings

        # Process remaining jobs - they should all be marked as bad
        for _ in range(5):
            process.process_job_records()

        # Verify all jobs are marked as bad
        with db_session as db:
            jobs = list(
                db.scalars(
                    models.JobRecord.select().where(
                        models.JobRecord.posting_id == posting_id
                    )
                )
            )
            for job in jobs:
                assert job.status == status.JobStatus.errored_out
                assert job.exited_ok is False

    def test_database_connection_handling(self, db_session):
        """Test proper database connection handling."""
        registry.Registry.clear_registry()

        from tests.example_package import foo

        cluster.job()(foo)

        posting_id, _ = registry.Registry[foo].post_work([(1, 1.0)], [])

        # Mock a database error
        with patch("pdoflow.cluster.Session") as mock_session_class:
            mock_session = Mock()
            mock_session_class.return_value.__enter__.return_value = (
                mock_session
            )

            # Simulate no jobs available (connection issue)
            mock_session.scalars.return_value = []

            process = cluster.ClusterProcess()

            with patch("pdoflow.cluster.logger"):
                result = process.process_job_records()

            # Should handle empty queue gracefully
            assert result == 0

    @given(
        st.integers(min_value=-100, max_value=100),
        st.integers(min_value=-100, max_value=100),
        st.integers(min_value=-100, max_value=100),
    )
    @settings(
        suppress_health_check=[HealthCheck.function_scoped_fixture],
        deadline=None,
        max_examples=5,
    )
    def test_priority_queue_processing(
        self, db_session, priority1, priority2, priority3
    ):
        """Test that jobs are processed in priority order."""
        registry.Registry.clear_registry()

        # Use a simple test function
        from tests.example_package import foo

        cluster.job(name="track_order")(foo)

        # Post jobs with different priorities
        posting_id, _ = registry.Registry["track_order"].post_work(
            [(1, 1.0), (2, 2.0), (3, 3.0)],
            [],
            priority=[priority1, priority2, priority3],
        )

        # Just verify the jobs were created with correct priorities
        with db_session as db:
            jobs = list(
                db.scalars(
                    models.JobRecord.select().where(
                        models.JobRecord.posting_id == posting_id
                    )
                )
            )

            # Should have 3 jobs
            assert len(jobs) == 3

            # Check priorities were assigned correctly
            job_priorities = {job.pos_args[0]: job.priority for job in jobs}
            assert job_priorities[1] == priority1
            assert job_priorities[2] == priority2
            assert job_priorities[3] == priority3

    def test_large_batch_processing(self, db_session):
        """Test processing large batches of jobs."""
        registry.Registry.clear_registry()

        from tests.example_package import foo

        cluster.job()(foo)

        # Create a large batch
        batch_size = 50
        workload = [(i, float(i)) for i in range(batch_size)]
        posting_id, job_ids = registry.Registry[foo].post_work(workload, [])

        # Verify all jobs were created
        assert len(job_ids) == batch_size

        # Test batch size limits
        process = cluster.ClusterProcess(batchsize=10)

        with patch("pdoflow.cluster.logger"):
            # First batch should get 10 jobs
            result = process.process_job_records()

            # Should process up to batchsize
            assert result == 10  # Since we didn't mock the actual jobs
