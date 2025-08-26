"""Tests for the ClusterPool class."""

import multiprocessing as mp
import time
from unittest.mock import patch

import pytest

from pdoflow import cluster, models, registry, status
from pdoflow.cluster import ClusterPool, ClusterProcess
from tests import example_package


class TestClusterPoolLifecycle:
    """Tests for ClusterPool context manager lifecycle."""

    def test_init_parameters(self):
        """Test initialization with various parameters."""
        pool = ClusterPool(
            max_workers=4,
            worker_class=ClusterProcess,
            exception_logging="error",
            warning_logging="info",
            batchsize=20,
        )

        assert pool.max_workers == 4
        assert pool.WorkerClass is ClusterProcess
        assert pool.exception_logging == "error"
        assert pool.warning_logging == "info"
        assert pool.batchsize == 20
        assert pool.workers == []

    def test_enter_starts_workers(self):
        """Test __enter__ creates and starts workers."""
        pool = ClusterPool(max_workers=3, worker_class=ClusterProcess)

        with pool as active_pool:
            assert active_pool is pool
            assert len(pool.workers) == 3

            # Verify workers are actual processes
            for worker in pool.workers:
                assert isinstance(worker, mp.Process)
                assert worker.daemon is True
                assert hasattr(worker, "exception_logging")
                assert hasattr(worker, "warning_logging")
                assert hasattr(worker, "batchsize")
                assert worker.is_alive()

    def test_exit_terminates_workers(self):
        """Test __exit__ terminates all workers."""
        pool = ClusterPool(max_workers=3, worker_class=ClusterProcess)

        workers = []
        with pool:
            workers = list(pool.workers)
            # Verify all are alive inside context
            for worker in workers:
                assert worker.is_alive()

        # Give workers time to terminate
        time.sleep(0.1)

        # Verify all workers were terminated
        for worker in workers:
            assert not worker.is_alive()

    def test_context_manager_complete_flow(self, db_session):
        """Test complete context manager flow with real job execution."""
        # Register a test function
        registry.Registry.clear_registry()
        cluster.job(name="test_flow")(example_package.foo)

        # Create work
        posting_id, job_ids = registry.Registry["test_flow"].post_work(
            [(1, 2.0), (3, 4.0)], []
        )

        # Mark posting as executing
        with db_session as db:
            posting = db.scalar(
                models.JobPosting.select().where(
                    models.JobPosting.id == posting_id
                )
            )
            posting.status = status.PostingStatus.executing
            db.commit()

        pool = ClusterPool(max_workers=2, worker_class=ClusterProcess)

        # Before entering context
        assert len(pool.workers) == 0

        with pool:
            # Inside context
            assert len(pool.workers) == 2
            for worker in pool.workers:
                assert worker.is_alive()

            # Give workers time to process jobs
            time.sleep(0.5)

        # After exiting context - workers should be terminated
        time.sleep(0.1)
        for worker in pool.workers:
            assert not worker.is_alive()

    def test_custom_worker_class(self):
        """Test using a custom worker class."""

        class CustomWorker(ClusterProcess):
            custom_attribute = "test"

        pool = ClusterPool(max_workers=1, worker_class=CustomWorker)

        with pool:
            assert len(pool.workers) == 1
            assert pool.WorkerClass is CustomWorker
            assert isinstance(pool.workers[0], CustomWorker)


class TestClusterPoolUpkeep:
    """Tests for worker management and upkeep."""

    def test_all_workers_alive(self):
        """Test upkeep when all workers are alive."""
        pool = ClusterPool(max_workers=3)

        with pool:
            initial_workers = list(pool.workers)

            # All workers should be alive
            for worker in initial_workers:
                assert worker.is_alive()

            pool.upkeep()

            # No workers should be replaced
            assert len(pool.workers) == 3
            for i, worker in enumerate(pool.workers):
                assert worker is initial_workers[i]

    def test_single_dead_worker_resurrection(self):
        """Test resurrection of a single dead worker."""
        pool = ClusterPool(max_workers=2, worker_class=ClusterProcess)

        with pool:
            initial_workers = list(pool.workers)

            # Terminate one worker to simulate death
            initial_workers[1].terminate()
            initial_workers[1].join(timeout=1.0)

            # Verify one is alive, one is dead
            assert initial_workers[0].is_alive()
            assert not initial_workers[1].is_alive()

            pool.upkeep()

            # Pool should still have 2 workers
            assert len(pool.workers) == 2

            # First worker should be unchanged
            assert pool.workers[0] is initial_workers[0]

            # Second worker should be new
            assert pool.workers[1] is not initial_workers[1]
            assert isinstance(pool.workers[1], ClusterProcess)
            assert pool.workers[1].is_alive()

    def test_worker_close_called(self):
        """Test that close is called on dead workers."""
        pool = ClusterPool(max_workers=1)

        with pool:
            # Get the worker and terminate it
            worker = pool.workers[0]
            pid = worker.pid
            worker.terminate()
            worker.join(timeout=1.0)

            assert not worker.is_alive()

            # Run upkeep
            pool.upkeep()

            # The process should be closed (can't directly test close was
            # called, but we can verify a new worker was created)
            assert len(pool.workers) == 1
            assert pool.workers[0] is not worker
            assert pool.workers[0].pid != pid

    def test_upkeep_maintains_worker_count(self, db_session):
        """Test that upkeep maintains the correct worker count."""
        pool = ClusterPool(max_workers=5, worker_class=ClusterProcess)

        with pool:
            initial_workers = list(pool.workers)

            # Kill workers at indices 2, 3, 4
            for i in [2, 3, 4]:
                initial_workers[i].terminate()
                initial_workers[i].join(timeout=1.0)

            # Verify death status
            assert initial_workers[0].is_alive()
            assert initial_workers[1].is_alive()
            assert not initial_workers[2].is_alive()
            assert not initial_workers[3].is_alive()
            assert not initial_workers[4].is_alive()

            pool.upkeep()

            # Should still have exactly 5 workers
            assert len(pool.workers) == 5

            # First 2 should be original alive workers
            assert pool.workers[0] is initial_workers[0]
            assert pool.workers[1] is initial_workers[1]

            # Last 3 should be new workers
            for i in [2, 3, 4]:
                assert pool.workers[i] is not initial_workers[i]
                assert pool.workers[i].is_alive()


class TestAwaitPostingCompletion:
    """Tests for await_posting_completion method."""

    def test_immediate_completion(self, db_session):
        """Test when posting is already complete."""
        # Create a completed posting
        registry.Registry.clear_registry()
        cluster.job(name="immediate_test")(example_package.foo)

        posting_id, job_ids = registry.Registry["immediate_test"].post_work(
            [(1, 2.0)], []
        )

        # Mark as finished
        with db_session as db:
            posting = db.scalar(
                models.JobPosting.select().where(
                    models.JobPosting.id == posting_id
                )
            )
            posting.status = status.PostingStatus.finished
            db.commit()

        pool = ClusterPool()

        with patch("pdoflow.cluster.sleep") as mock_sleep:
            pool.await_posting_completion(posting_id)
            # Should not sleep when already complete
            mock_sleep.assert_not_called()

    def test_max_wait_timeout(self, db_session):
        """Test that max_wait properly times out."""
        # Create a posting that won't complete
        registry.Registry.clear_registry()
        cluster.job(name="timeout_test")(example_package.foo)

        posting_id, job_ids = registry.Registry["timeout_test"].post_work(
            [(1, 2.0)], []
        )

        posting = db_session.scalar(
            models.JobPosting.select().where(models.JobPosting.id == posting_id)
        )
        posting.status = status.PostingStatus.executing
        db_session.commit()

        pool = ClusterPool()

        # Mock time to simulate passage
        start_time = time.time()
        time_sequence = [start_time + i * 1.0 for i in range(10)]

        with patch("pdoflow.cluster.time") as mock_time:
            mock_time.side_effect = time_sequence

            with patch("pdoflow.cluster.sleep"):
                with pytest.raises(TimeoutError):
                    pool.await_posting_completion(
                        posting_id, poll_time=0.1, max_wait=3
                    )
