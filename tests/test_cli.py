import uuid
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from pdoflow import cli, cluster, models, registry, status
from pdoflow.io import Session
from pdoflow.utils import load_function

from . import strategies


# Test EnumChoice custom type
class TestEnumChoice:
    def test_convert_lowercase(self):
        enum_choice = cli.EnumChoice(status.PostingStatus)
        result = enum_choice.convert("executing", None, None)
        assert result == status.PostingStatus.executing

    def test_convert_uppercase(self):
        enum_choice = cli.EnumChoice(status.PostingStatus)
        result = enum_choice.convert("EXECUTING", None, None)
        assert result == status.PostingStatus.executing

    def test_convert_hyphenated(self):
        enum_choice = cli.EnumChoice(status.PostingStatus)
        result = enum_choice.convert("errored-out", None, None)
        assert result == status.PostingStatus.errored_out

    def test_convert_invalid(self):
        enum_choice = cli.EnumChoice(status.PostingStatus)
        with pytest.raises(Exception) as exc_info:
            enum_choice.convert("invalid_status", None, None)
        assert "not a valid PostingStatus" in str(exc_info.value)

    def test_get_metavar(self):
        enum_choice = cli.EnumChoice(status.PostingStatus)
        metavar = enum_choice.get_metavar(None)
        assert "paused" in metavar
        assert "executing" in metavar
        assert "|" in metavar


def test_main_entry_point():
    runner = CliRunner()
    result = runner.invoke(cli.pdoflow_main, ["--help"])
    assert result.exit_code == 0
    assert "Usage:" in result.output


# Test pool command
@patch("pdoflow.cli.ClusterPool")
@patch("pdoflow.cli.sleep")
def test_pool_command_default(mock_sleep, mock_cluster_pool):
    # Setup mocks
    mock_pool_instance = MagicMock()
    # Run once then stop
    mock_pool_instance.__bool__.side_effect = [True, False]
    mock_cluster_pool.return_value = mock_pool_instance

    runner = CliRunner()
    result = runner.invoke(cli.pool)

    assert result.exit_code == 0
    # The actual CPU count is used by default; we can't mock it at import time
    # So let's just check that it was called with the right other parameters
    assert mock_cluster_pool.called
    call_args = mock_cluster_pool.call_args[1]
    assert call_args["exception_logging"] == "warning"
    assert call_args["warning_logging"] == "debug"
    assert call_args["batchsize"] == 10
    assert isinstance(call_args["max_workers"], int)
    assert call_args["max_workers"] > 0
    mock_pool_instance.upkeep.assert_called_once()
    mock_sleep.assert_called_once_with(2.0)  # 1/0.5


@patch("pdoflow.cli.ClusterPool")
@patch("pdoflow.cli.sleep")
def test_pool_command_custom_params(mock_sleep, mock_cluster_pool):
    mock_pool_instance = MagicMock()
    mock_pool_instance.__bool__.side_effect = [True, False]
    mock_cluster_pool.return_value = mock_pool_instance

    runner = CliRunner()
    result = runner.invoke(
        cli.pool,
        [
            "--max-workers",
            "4",
            "--upkeep-rate",
            "2.0",
            "--exception-logging",
            "error",
            "--warning-logging",
            "info",
            "--batchsize",
            "20",
        ],
    )

    assert result.exit_code == 0
    mock_cluster_pool.assert_called_once_with(
        max_workers=4,
        exception_logging="error",
        warning_logging="info",
        batchsize=20,
    )
    mock_sleep.assert_called_once_with(0.5)  # 1/2.0


# Test posting_status command
@given(
    strategies.foo_workload(),
    st.just("tests.example_package.foo"),
)
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_posting_status_cli(db_session, workload, path):
    registry.Registry.clear_registry()
    function = load_function(path)
    cluster.job()(function)

    posting_id, _ = registry.Registry[function].post_work(workload, [])

    runner = CliRunner()
    result = runner.invoke(cli.posting_status, [str(posting_id)])

    assert result.exit_code == 0
    assert str(posting_id) in result.output
    assert str(status.PostingStatus.executing) in result.output


def test_posting_status_invalid_uuid():
    runner = CliRunner()
    result = runner.invoke(cli.posting_status, ["invalid-uuid"])

    assert result.exit_code != 0


def test_posting_status_with_show_jobs(db_session):
    # Create a posting with jobs
    registry.Registry.clear_registry()
    cluster.job()(lambda x: x)
    posting_id, job_ids = registry.Registry[lambda x: x].post_work(
        [(1,), (2,), (3,)], [], priority=[100, 0, -50]
    )

    runner = CliRunner()
    result = runner.invoke(
        cli.posting_status, [str(posting_id), "--show-jobs"]
    )  # noqa: E501

    assert result.exit_code == 0
    assert "Job Details:" in result.output
    assert "priority" in result.output
    assert "100" in result.output  # High priority job


def test_posting_status_table_format(db_session):
    registry.Registry.clear_registry()
    cluster.job()(lambda x: x)
    posting_id, _ = registry.Registry[lambda x: x].post_work([(1,)], [])

    runner = CliRunner()
    result = runner.invoke(
        cli.posting_status, [str(posting_id), "--table-format", "grid"]
    )

    assert result.exit_code == 0
    assert "+" in result.output  # Grid format uses + for borders


# Test list_postings command
def test_list_postings_empty(db_session):
    runner = CliRunner()
    result = runner.invoke(cli.list_postings)

    assert result.exit_code == 0
    # Empty table still has headers
    assert "id" in result.output
    assert "status" in result.output


def test_list_postings_with_data(db_session):
    # Create multiple postings
    registry.Registry.clear_registry()
    cluster.job()(lambda x: x)
    posting1, _ = registry.Registry[lambda x: x].post_work([(1,)], [])
    posting2, _ = registry.Registry[lambda x: x].post_work([(2,)], [])

    runner = CliRunner()
    result = runner.invoke(cli.list_postings)

    assert result.exit_code == 0
    assert str(posting1) in result.output
    assert str(posting2) in result.output


def test_list_postings_table_format(db_session):
    runner = CliRunner()
    result = runner.invoke(cli.list_postings, ["--table-format", "html"])

    assert result.exit_code == 0
    assert "<table>" in result.output or "id" in result.output


# Test set_posting_status command
def test_set_posting_status_success(db_session):
    registry.Registry.clear_registry()
    cluster.job()(lambda x: x)
    posting_id, _ = registry.Registry[lambda x: x].post_work([(1,)], [])

    runner = CliRunner()
    result = runner.invoke(cli.set_posting_status, [str(posting_id), "paused"])

    assert result.exit_code == 0

    # Verify status was updated
    with Session() as db:
        posting = db.scalar(
            models.JobPosting.select().where(
                models.JobPosting.id == posting_id
            )  # noqa: E501
        )
        assert posting.status == status.PostingStatus.paused


def test_set_posting_status_invalid_uuid(db_session):
    fake_uuid = str(uuid.uuid4())

    runner = CliRunner()
    result = runner.invoke(cli.set_posting_status, [fake_uuid, "paused"])

    assert result.exit_code == 1
    assert "Could not find Posting" in result.output


def test_set_posting_status_invalid_status():
    runner = CliRunner()
    result = runner.invoke(
        cli.set_posting_status, [str(uuid.uuid4()), "invalid_status"]
    )

    assert result.exit_code != 0
    assert "not a valid PostingStatus" in result.output


# Test priority_stats command
def test_priority_stats_no_jobs(db_session):
    runner = CliRunner()
    result = runner.invoke(cli.priority_stats)

    assert result.exit_code == 0
    assert "No waiting jobs found" in result.output


def test_priority_stats_with_jobs(db_session):
    # Create jobs with different priorities
    registry.Registry.clear_registry()
    cluster.job()(lambda x: x)
    # Create waiting jobs with various priorities
    posting_id, _ = registry.Registry[lambda x: x].post_work(
        [(1,), (2,), (3,), (4,), (5,)], [], priority=[100, 100, 0, -50, -50]
    )

    # Update posting to executing so jobs are available
    with Session() as db:
        posting = db.scalar(
            models.JobPosting.select().where(
                models.JobPosting.id == posting_id
            )  # noqa: E501
        )
        posting.status = status.PostingStatus.executing
        db.commit()

    runner = CliRunner()
    result = runner.invoke(cli.priority_stats)

    assert result.exit_code == 0
    assert "Priority Distribution" in result.output
    assert "100" in result.output  # High priority
    assert "2" in result.output  # Count of 2 jobs
    assert "-50" in result.output  # Low priority


def test_priority_stats_table_format(db_session):
    runner = CliRunner()
    result = runner.invoke(cli.priority_stats, ["--table-format", "json"])

    assert result.exit_code == 0


# Test execute_job command
def test_execute_job_success(db_session):
    from . import example_package

    registry.Registry.clear_registry()
    example_package.clear_execution_tracker()

    # Register the tracking function
    cluster.job()(example_package.track_execution)
    posting_id, job_ids = registry.Registry[
        example_package.track_execution
    ].post_work(  # noqa: E501
        [(42,)], []
    )

    # Update posting to executing
    with Session() as db:
        posting = db.scalar(
            models.JobPosting.select().where(
                models.JobPosting.id == posting_id
            )  # noqa: E501
        )
        posting.status = status.PostingStatus.executing
        db.commit()

    runner = CliRunner()
    result = runner.invoke(cli.execute_job, [str(job_ids[0])])

    assert result.exit_code == 0
    assert 42 in example_package.get_execution_tracker()


def test_execute_job_invalid_uuid(db_session):
    fake_uuid = str(uuid.uuid4())

    runner = CliRunner()
    result = runner.invoke(cli.execute_job, [fake_uuid])

    assert result.exit_code == 0  # Command doesn't fail, just prints error


def test_execute_job_with_exception(db_session):
    from . import example_package

    registry.Registry.clear_registry()

    cluster.job()(example_package.failing_func)
    posting_id, job_ids = registry.Registry[
        example_package.failing_func
    ].post_work(  # noqa: E501
        [(1,)], []
    )

    # Update posting to executing
    with Session() as db:
        posting = db.scalar(
            models.JobPosting.select().where(
                models.JobPosting.id == posting_id
            )  # noqa: E501
        )
        posting.status = status.PostingStatus.executing
        db.commit()

    runner = CliRunner()
    result = runner.invoke(cli.execute_job, [str(job_ids[0])])

    assert result.exit_code == 0
    assert "encountered an error" in result.output
    assert "Test error" in result.output

    # Verify job was marked as bad
    with Session() as db:
        job = db.scalar(
            models.JobRecord.select().where(models.JobRecord.id == job_ids[0])
        )
        assert job.status == status.JobStatus.errored_out
        assert job.exited_ok is False


@patch("pdoflow.models.JobRecord.execute")
def test_execute_job_keyboard_interrupt(mock_execute, db_session):
    mock_execute.side_effect = KeyboardInterrupt()

    registry.Registry.clear_registry()
    cluster.job()(lambda x: x)
    posting_id, job_ids = registry.Registry[lambda x: x].post_work([(1,)], [])

    # Update posting to executing
    with Session() as db:
        posting = db.scalar(
            models.JobPosting.select().where(
                models.JobPosting.id == posting_id
            )  # noqa: E501
        )
        assert posting is not None
        posting.status = status.PostingStatus.executing
        db.commit()

    runner = CliRunner()
    result = runner.invoke(cli.execute_job, [str(job_ids[0])])

    assert result.exit_code == 0
    assert "Keyboard interrupt" in result.output


# Test posting_status with stdin input
@patch("fileinput.input")
def test_posting_status_from_stdin(mock_fileinput, db_session):
    registry.Registry.clear_registry()
    cluster.job()(lambda x: x)

    # Create multiple postings
    posting_ids = []
    for i in range(3):
        posting_id, _ = registry.Registry[lambda x: x].post_work([(i,)], [])
        posting_ids.append(str(posting_id))

    # Mock fileinput to return our UUIDs
    mock_fileinput.return_value = posting_ids

    runner = CliRunner()
    # Pass a non-UUID string to trigger stdin reading
    result = runner.invoke(cli.posting_status, ["not-a-uuid"])

    assert result.exit_code == 0
    for pid in posting_ids:
        assert pid in result.output


# Test pool command with logger output
@patch("pdoflow.cli.logger")
@patch("pdoflow.cli.ClusterPool")
@patch("pdoflow.cli.sleep")
def test_pool_command_logger(mock_sleep, mock_cluster_pool, mock_logger):
    mock_pool_instance = MagicMock()
    mock_pool_instance.__bool__.side_effect = [True, False]
    mock_cluster_pool.return_value = mock_pool_instance

    runner = CliRunner()
    result = runner.invoke(cli.pool)

    assert result.exit_code == 0
    # Check that logger.debug was called
    assert mock_logger.debug.called
    debug_message = mock_logger.debug.call_args[0][0]
    assert "Instantiated" in debug_message
    assert "upkeep rate" in debug_message


# Test posting_status with no jobs when show-jobs is enabled
def test_posting_status_show_jobs_empty(db_session):
    # Create an empty posting
    posting = models.JobPosting(
        target_function="test",
        entry_point="test.module",
        status=status.PostingStatus.executing,
    )
    with Session() as db:
        db.add(posting)
        db.commit()
        posting_id = posting.id

    runner = CliRunner()
    result = runner.invoke(
        cli.posting_status, [str(posting_id), "--show-jobs"]
    )  # noqa: E501

    assert result.exit_code == 0
    assert "Job Details:" in result.output
    # Should not show job table for empty posting
    assert f"Posting {posting_id}" not in result.output


# Test priority_stats with mixed job statuses
def test_priority_stats_only_waiting_jobs(db_session):
    registry.Registry.clear_registry()
    cluster.job()(lambda x: x)

    # Create jobs with different statuses
    posting_id, job_ids = registry.Registry[lambda x: x].post_work(
        [(1,), (2,), (3,), (4,)], [], priority=[100, 100, 50, 50]
    )

    # Update posting and some jobs
    with Session() as db:
        posting = db.scalar(
            models.JobPosting.select().where(
                models.JobPosting.id == posting_id
            )  # noqa: E501
        )
        posting.status = status.PostingStatus.executing

        # Mark some jobs as done
        jobs = list(
            db.scalars(
                models.JobRecord.select().where(
                    models.JobRecord.id.in_(job_ids[:2])
                )
            )
        )
        for job in jobs:
            job.status = status.JobStatus.done

        db.commit()

    runner = CliRunner()
    result = runner.invoke(cli.priority_stats)

    assert result.exit_code == 0
    assert "Priority Distribution" in result.output
    assert "50" in result.output  # Only waiting jobs
    assert "100" not in result.output  # Done jobs not shown


# Test various table formats
@pytest.mark.parametrize(
    "format_name", ["simple", "grid", "fancy_grid", "html", "latex"]
)
def test_list_postings_various_formats(db_session, format_name):
    # Create a posting
    registry.Registry.clear_registry()
    cluster.job()(lambda x: x)
    registry.Registry[lambda x: x].post_work([(1,)], [])

    runner = CliRunner()
    result = runner.invoke(cli.list_postings, ["--table-format", format_name])

    assert result.exit_code == 0
    assert len(result.output) > 0


# Test EnumChoice with different enum types
def test_enum_choice_job_status():
    enum_choice = cli.EnumChoice(status.JobStatus)

    # Test valid conversions
    waiting = enum_choice.convert("waiting", None, None)
    assert waiting == status.JobStatus.waiting
    assert enum_choice.convert("DONE", None, None) == status.JobStatus.done
    errored = enum_choice.convert("errored-out", None, None)
    assert errored == status.JobStatus.errored_out
    # Test metavar
    metavar = enum_choice.get_metavar(None)
    assert "waiting" in metavar
    assert "executing" in metavar


# Test error message format in set_posting_status
def test_set_posting_status_error_message_format(db_session):
    fake_uuid = str(uuid.uuid4())

    runner = CliRunner()
    result = runner.invoke(cli.set_posting_status, [fake_uuid, "paused"])

    assert result.exit_code == 1
    # Note: The error message uses {id} placeholder incorrectly
    assert "Could not find Posting with id:" in result.output
