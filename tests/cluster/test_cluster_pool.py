"""Tests for the ClusterPool class."""
import uuid
from unittest.mock import MagicMock, Mock, patch

import pytest

from pdoflow.cluster import ClusterPool, ClusterProcess


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

    def test_enter_starts_workers(self, mock_cluster_worker):
        """Test __enter__ creates and starts workers."""
        workers = [mock_cluster_worker() for _ in range(3)]
        mock_worker_class = Mock(side_effect=workers)

        pool = ClusterPool(max_workers=3, worker_class=mock_worker_class)

        with pool as active_pool:
            assert active_pool is pool
            assert len(pool.workers) == 3
            assert mock_worker_class.call_count == 3

            # Verify workers were created with correct params
            for call in mock_worker_class.call_args_list:
                assert call.kwargs["daemon"] is True
                assert call.kwargs["exception_logging"] == "warning"
                assert call.kwargs["warning_logging"] == "debug"
                assert call.kwargs["batchsize"] == 10

            # Verify all workers were started
            for worker in pool.workers:
                worker.start.assert_called_once()

    def test_exit_terminates_workers(self, mock_cluster_worker):
        """Test __exit__ terminates all workers."""
        workers = [mock_cluster_worker() for _ in range(3)]
        mock_worker_class = Mock(side_effect=workers)

        pool = ClusterPool(max_workers=3, worker_class=mock_worker_class)

        with pool:
            pass

        # Verify all workers were terminated
        for worker in workers:
            worker.terminate.assert_called_once()

    def test_context_manager_complete_flow(self, mock_cluster_worker):
        """Test complete context manager flow."""
        workers = [mock_cluster_worker() for _ in range(2)]
        mock_worker_class = Mock(side_effect=workers)

        pool = ClusterPool(max_workers=2, worker_class=mock_worker_class)

        # Before entering context
        assert len(pool.workers) == 0

        with pool:
            # Inside context
            assert len(pool.workers) == 2
            for worker in pool.workers:
                worker.start.assert_called_once()
                worker.terminate.assert_not_called()

        # After exiting context
        for worker in workers:
            worker.terminate.assert_called_once()

    def test_custom_worker_class(self):
        """Test using a custom worker class."""

        class CustomWorker(ClusterProcess):
            custom_attribute = "test"

        pool = ClusterPool(max_workers=1, worker_class=CustomWorker)

        with pool:
            assert len(pool.workers) == 1
            assert pool.WorkerClass is CustomWorker


class TestClusterPoolUpkeep:
    """Tests for worker management and upkeep."""

    def test_all_workers_alive(self, mock_cluster_worker):
        """Test upkeep when all workers are alive."""
        workers = [mock_cluster_worker() for _ in range(3)]
        # Workers are alive by default (fixture returns True)

        pool = ClusterPool(max_workers=3)
        pool.workers = workers

        pool.upkeep()

        # No workers should be closed or replaced
        for worker in workers:
            worker.close.assert_not_called()
        assert len(pool.workers) == 3

    def test_single_dead_worker_resurrection(self, mock_cluster_worker):
        """Test resurrection of a single dead worker."""
        alive_worker = mock_cluster_worker()
        alive_worker.is_alive.return_value = True

        dead_worker = mock_cluster_worker()
        dead_worker.is_alive.return_value = False

        new_worker = mock_cluster_worker()
        mock_worker_class = Mock(return_value=new_worker)

        pool = ClusterPool(max_workers=2, worker_class=mock_worker_class)
        pool.workers = [alive_worker, dead_worker]

        pool.upkeep()

        # Dead worker should be closed
        dead_worker.close.assert_called_once()
        alive_worker.close.assert_not_called()

        # New worker should be created and started
        mock_worker_class.assert_called_once_with(daemon=True)
        new_worker.start.assert_called_once()

        # Pool should still have 2 workers
        assert len(pool.workers) == 2
        assert pool.workers[0] is alive_worker
        assert pool.workers[1] is new_worker

    def test_worker_close_called(self, mock_cluster_worker):
        """Test that close is called on dead workers."""
        dead_worker = mock_cluster_worker()
        dead_worker.is_alive.return_value = False

        pool = ClusterPool(max_workers=1)
        pool.workers = [dead_worker]
        pool.WorkerClass = Mock(return_value=mock_cluster_worker())

        pool.upkeep()

        dead_worker.close.assert_called_once()

    def test_upkeep_maintains_worker_count(self, mock_cluster_worker):
        """Test that upkeep maintains the correct worker count."""
        # Create mix of alive and dead workers
        workers = []
        for i in range(5):
            worker = mock_cluster_worker()
            worker.is_alive.return_value = i < 2
            workers.append(worker)

        new_workers = [mock_cluster_worker() for _ in range(3)]
        mock_worker_class = Mock(side_effect=new_workers)

        pool = ClusterPool(max_workers=5, worker_class=mock_worker_class)
        pool.workers = workers

        pool.upkeep()

        # Should still have exactly 5 workers
        assert len(pool.workers) == 5
        # First 2 should be original alive workers
        assert pool.workers[0] is workers[0]
        assert pool.workers[1] is workers[1]
        # Last 3 should be new workers
        assert pool.workers[2] in new_workers
        assert pool.workers[3] in new_workers
        assert pool.workers[4] in new_workers


class TestAwaitPostingCompletion:
    """Tests for await_posting_completion method."""

    @patch("pdoflow.cluster.Session")
    @patch("pdoflow.cluster.sleep")
    def test_immediate_completion(self, mock_sleep, mock_session_class):
        """Test when posting is already complete."""
        mock_session = MagicMock()
        mock_session_class.return_value.__enter__.return_value = mock_session
        mock_session.scalar.return_value = 100.0  # 100% complete

        pool = ClusterPool()
        posting_id = uuid.uuid4()

        pool.await_posting_completion(posting_id)

        # Should not sleep when already complete
        mock_sleep.assert_not_called()

    @patch("pdoflow.cluster.Session")
    @patch("pdoflow.cluster.sleep")
    def test_gradual_completion(self, mock_sleep, mock_session_class):
        """Test waiting for gradual completion."""
        mock_session = MagicMock()
        mock_session_class.return_value.__enter__.return_value = mock_session
        # Simulate progress: 0%, 25%, 50%, 75%, 100%
        mock_session.scalar.side_effect = [0.0, 25.0, 50.0, 75.0, 100.0]

        pool = ClusterPool()
        posting_id = uuid.uuid4()

        pool.await_posting_completion(posting_id, poll_time=0.1)

        # Should sleep 4 times (not after 100%)
        assert mock_sleep.call_count == 4
        mock_sleep.assert_called_with(0.1)

    @patch("pdoflow.cluster.Session")
    def test_nonexistent_posting_error(self, mock_session_class):
        """Test ValueError for non-existent posting."""
        mock_session = MagicMock()
        mock_session_class.return_value.__enter__.return_value = mock_session
        mock_session.scalar.return_value = None  # No posting found

        pool = ClusterPool()
        posting_id = uuid.uuid4()

        with pytest.raises(ValueError) as exc_info:
            pool.await_posting_completion(posting_id)

        assert str(posting_id) in str(exc_info.value)

    @patch("pdoflow.cluster.Session")
    @patch("pdoflow.cluster.sleep")
    def test_custom_poll_time(self, mock_sleep, mock_session_class):
        """Test custom polling intervals."""
        mock_session = MagicMock()
        mock_session_class.return_value.__enter__.return_value = mock_session
        mock_session.scalar.side_effect = [0.0, 50.0, 100.0]

        pool = ClusterPool()
        posting_id = uuid.uuid4()

        pool.await_posting_completion(posting_id, poll_time=2.5)

        # Check custom poll time is used
        assert mock_sleep.call_count == 2
        mock_sleep.assert_called_with(2.5)

    @patch("pdoflow.cluster.Session")
    @patch("pdoflow.cluster.time")
    @patch("pdoflow.cluster.sleep")
    def test_max_wait_calculation(
        self, mock_sleep, mock_time, mock_session_class
    ):
        """Test that max_wait is properly calculated."""
        mock_session = MagicMock()
        mock_session_class.return_value.__enter__.return_value = mock_session
        mock_session.scalar.return_value = 99.9  # Almost complete

        # Set up time progression
        start_time = 1000.0
        time_sequence = [start_time + i * 0.5 for i in range(10)]
        mock_time.side_effect = time_sequence

        pool = ClusterPool()
        posting_id = uuid.uuid4()

        with pytest.raises(TimeoutError):
            pool.await_posting_completion(
                posting_id, poll_time=0.1, max_wait=3.0
            )

        # Should have checked time multiple times
        assert mock_time.call_count >= 2

    @patch("pdoflow.cluster.Session")
    def test_partial_completion_tracking(self, mock_session_class):
        """Test tracking partial completion percentages."""
        mock_session = MagicMock()
        mock_session_class.return_value.__enter__.return_value = mock_session

        completion_sequence = []

        def track_completion(query):
            # Progress from 0 to 100 in steps of 20
            pct = len(completion_sequence) * 20
            completion_sequence.append(pct)
            return 100.0 if pct >= 100 else pct

        mock_session.scalar.side_effect = track_completion

        pool = ClusterPool()
        posting_id = uuid.uuid4()

        pool.await_posting_completion(posting_id, poll_time=0.01)

        # Should have tracked progress from 0 to 100
        assert completion_sequence == [0, 20, 40, 60, 80, 100]
