"""Tests for the ClusterProcess class."""
from unittest.mock import MagicMock, Mock, patch

import pytest
from sqlalchemy.exc import OperationalError

from pdoflow import cluster, registry, status
from pdoflow.cluster import ClusterProcess
from tests.example_package import foo


class TestClusterProcessInit:
    """Tests for ClusterProcess initialization."""

    def test_init_default_params(self):
        """Test initialization with default parameters."""
        process = ClusterProcess()

        assert process._session is None
        assert process.failure_threshold == 10
        assert process._failure_cache._default_value == 10
        assert process._bad_postings == set()
        assert process.exception_logging == "warning"
        assert process.warning_logging == "debug"
        assert process.batchsize == 10

    def test_init_custom_params(self):
        """Test initialization with custom parameters."""
        process = ClusterProcess(
            exception_logging="error",
            warning_logging="info",
            batchsize=20,
            daemon=True,
            name="TestWorker",
        )

        assert process.exception_logging == "error"
        assert process.warning_logging == "info"
        assert process.batchsize == 20
        assert process.daemon is True
        assert process.name == "TestWorker"

    @patch("os.getpid")
    def test_repr_format(self, mock_getpid):
        """Test __repr__ returns correct format."""
        mock_getpid.return_value = 12345
        process = ClusterProcess()

        assert repr(process) == "<ClusterProcess pid=12345 />"

    @patch("pdoflow.cluster.make_warning_logger")
    def test_pre_run_init_warning_setup(self, mock_make_warning_logger):
        """Test _pre_run_init sets up warning logger."""
        mock_logger = Mock()
        mock_make_warning_logger.return_value = mock_logger

        process = ClusterProcess(warning_logging="custom_level")
        process._pre_run_init()

        mock_make_warning_logger.assert_called_once_with("custom_level")
        assert process._pre_run_init.__module__ == "pdoflow.cluster"


class TestProcessJob:
    """Tests for job processing logic."""

    def test_successful_execution(self, mock_job_record):
        """Test successful job execution."""
        job = mock_job_record()
        job.execute.return_value = "result"
        job.time_elapsed.total_seconds.return_value = 2.5

        process = ClusterProcess()
        db = Mock()

        with patch("pdoflow.cluster.logger") as mock_logger:
            process.process_job(db, job)

        job.execute.assert_called_once()
        mock_logger.success.assert_called_once()
        success_msg = mock_logger.success.call_args[0][0]
        assert str(job.id) in success_msg
        assert "2.50 seconds" in success_msg
        db.commit.assert_called_once()

    def test_bad_posting_handling(self, mock_job_record):
        """Test handling of jobs from blacklisted postings."""
        job = mock_job_record()
        process = ClusterProcess()
        process._bad_postings.add(job.posting_id)
        db = Mock()

        process.process_job(db, job)

        job.mark_as_bad.assert_called_once()
        job.execute.assert_not_called()
        db.commit.assert_called_once()

    def test_keyboard_interrupt(self, mock_job_record):
        """Test KeyboardInterrupt handling."""
        job = mock_job_record()
        job.execute.side_effect = KeyboardInterrupt()

        process = ClusterProcess()
        db = Mock()

        with patch("pdoflow.cluster.logger") as mock_logger:
            with pytest.raises(KeyboardInterrupt):
                process.process_job(db, job)

        mock_logger.warning.assert_called_once_with(
            "Encountered interrupt, releasing jobs"
        )
        db.rollback.assert_called_once()

    @patch("pdoflow.cluster.sleep")
    @patch("pdoflow.cluster.random")
    def test_operational_error_backoff(
        self, mock_random, mock_sleep, mock_job_record
    ):
        """Test database error handling with backoff."""
        job = mock_job_record()
        job.execute.side_effect = OperationalError(
            "Connection lost", None, None
        )
        mock_random.random.return_value = 0.5

        process = ClusterProcess()
        db = Mock()

        with patch("pdoflow.cluster.logger") as mock_logger:
            process.process_job(db, job)

        mock_logger.exception.assert_called_once()
        assert "database error" in mock_logger.exception.call_args[0][0]
        mock_sleep.assert_called_once_with(1.0)  # 2 * 0.5
        assert job.status == status.JobStatus.waiting
        db.commit.assert_called_once()

    def test_general_exception_logging(self, mock_job_record):
        """Test general exception handling with configurable logging."""
        job = mock_job_record()
        job.execute.side_effect = ValueError("Test error")

        process = ClusterProcess(exception_logging="error")
        process._failure_cache[job.posting_id] = 5
        db = Mock()

        with patch("pdoflow.cluster.logger") as mock_logger:
            process.process_job(db, job)

        mock_logger.error.assert_called_once()
        assert "Test error" in str(mock_logger.error.call_args[0][0])

    def test_failure_threshold_blacklisting(self, mock_job_record):
        """Test posting blacklisting when failure threshold reached."""
        job = mock_job_record()
        job.execute.side_effect = RuntimeError("Failed")

        process = ClusterProcess()
        process._failure_cache[job.posting_id] = 0  # Already at threshold
        db = Mock()

        with patch("pdoflow.cluster.logger") as mock_logger:
            process.process_job(db, job)

        # Check blacklisting occurred
        assert job.posting_id in process._bad_postings
        job.mark_as_bad.assert_called_once()
        assert any(
            "too erroneous to continue" in str(warning_call)
            for warning_call in mock_logger.warning.call_args_list
        )

    def test_retry_decrement(self, mock_job_record):
        """Test retry count decrementation."""
        job = mock_job_record(tries_remaining=3)
        job.execute.side_effect = Exception("Temporary failure")

        process = ClusterProcess()
        process._failure_cache[job.posting_id] = 10
        db = Mock()

        with patch("pdoflow.cluster.logger") as mock_logger:
            process.process_job(db, job)

        assert job.tries_remaining == 2
        assert job.status == status.JobStatus.waiting
        job.mark_as_bad.assert_not_called()
        assert any(
            "2 tries remaining" in str(warning_call)
            for warning_call in mock_logger.warning.call_args_list
        )

    def test_exception_logging_levels(self, mock_job_record):
        """Test different exception logging levels."""
        job = mock_job_record()
        job.execute.side_effect = Exception("Test")

        # Test each logging level
        for level in ["debug", "info", "warning", "error", "critical"]:
            process = ClusterProcess(exception_logging=level)
            process._failure_cache[job.posting_id] = 10
            db = Mock()

            with patch("pdoflow.cluster.logger") as mock_logger:
                process.process_job(db, job)

            # Verify correct logger method was called
            logger_method = getattr(mock_logger, level)
            if level in ["debug", "info", "error", "critical"]:
                logger_method.assert_called_once()
            else:  # warning is default
                mock_logger.warning.assert_called()


class TestProcessJobRecords:
    """Tests for batch job processing."""

    @patch("pdoflow.cluster.Session")
    def test_empty_queue_handling(self, mock_session_class):
        """Test handling when no jobs are available."""
        mock_session = MagicMock()
        mock_session_class.return_value.__enter__.return_value = mock_session
        mock_session.scalars.return_value = []

        process = ClusterProcess()

        with patch("pdoflow.cluster.logger") as mock_logger:
            result = process.process_job_records()

        assert result == 0
        mock_logger.debug.assert_called_once()
        assert "Nothing" in mock_logger.debug.call_args[0][0]

    @patch("pdoflow.cluster.time")
    def test_timing_logs(self, mock_time, db_session):
        """Test timing information in logs."""
        mock_time.side_effect = [100.0, 100.5]  # 0.5 seconds elapsed

        registry.Registry.clear_registry()
        cluster.job()(foo)

        _ = registry.Registry[foo].post_work([(1, 1.0)], [])

        process = ClusterProcess()
        with patch("pdoflow.cluster.logger") as mock_logger:
            process.process_job_records()

        mock_logger.info.assert_called_once()
        info_msg = mock_logger.info.call_args[0][0]
        assert "0.50 seconds" in info_msg
        assert "to aquire workload" in info_msg


class TestClusterProcessRun:
    """Tests for the main run loop."""

    @patch("pdoflow.cluster.sleep")
    def test_run_loop_with_jobs(self, mock_sleep):
        """Test run loop processing jobs."""
        process = ClusterProcess()

        # Mock process_job_records to return job counts then exit
        job_counts = [3, 2, 1, 0]
        process.process_job_records = Mock(
            side_effect=job_counts + [KeyboardInterrupt()]
        )

        with pytest.raises(KeyboardInterrupt):
            process.run()

        # Should have been called 5 times before interrupt
        assert process.process_job_records.call_count == 5
        # Should only sleep when no jobs (once)
        mock_sleep.assert_called_once_with(5)

    @patch("pdoflow.cluster.sleep")
    def test_run_loop_continuous_operation(self, mock_sleep):
        """Test continuous operation of run loop."""
        process = ClusterProcess()
        process._pre_run_init = Mock()

        # Simulate several cycles then exit
        call_count = 0

        def mock_process_records():
            nonlocal call_count
            call_count += 1
            if call_count > 10:
                raise KeyboardInterrupt()
            return 0 if call_count % 3 == 0 else 1

        process.process_job_records = mock_process_records

        with pytest.raises(KeyboardInterrupt):
            process.run()

        process._pre_run_init.assert_called_once()
        assert call_count == 11
        # Should sleep on cycles 3, 6, 9
        assert mock_sleep.call_count == 3
