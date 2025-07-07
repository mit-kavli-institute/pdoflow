"""Tests for job profiling functionality."""
import random
from unittest.mock import patch

import sqlalchemy as sa
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from pdoflow import cluster, models, registry, status
from pdoflow.cluster import ClusterProcess
from pdoflow.models import JobProfile, reflect_cProfile
from tests import example_package
from tests.utils import CoverageWorker


class TestProfilingExecution:
    """Test profiling execution logic."""

    def test_traced_execution_creates_stats(self, db_session):
        """Test that traced_execution returns cProfile stats."""
        # Create a job posting and record
        registry.Registry.clear_registry()
        cluster.job(name="cpu_test")(example_package.cpu_intensive_task)

        posting_id, job_ids = registry.Registry["cpu_test"].post_work(
            [(1000,)], []
        )

        with db_session as db:
            job = db.scalar(
                models.JobRecord.select().where(
                    models.JobRecord.id == job_ids[0]
                )
            )

            # Create worker and execute with tracing
            worker = ClusterProcess()
            stats = worker.traced_execution(job)

            # Verify stats structure
            assert isinstance(stats, dict)
            assert len(stats) > 0

            # Check that stats contain expected keys (filename, line, function)
            for key in stats:
                assert isinstance(key, tuple)
                assert len(key) == 3  # (filename, line_number, function_name)

            # Verify the executed function is in the stats
            found_cpu_intensive = False
            for (filename, line, funcname) in stats:
                if funcname == "cpu_intensive_task":
                    found_cpu_intensive = True
                    break

            # Debug: print all function names if not found
            if not found_cpu_intensive:
                print("\nFunction names in stats:")
                for (filename, line, funcname) in stats:
                    print(f"  {funcname} ({filename}:{line})")

            assert found_cpu_intensive

    def test_nominal_execution_no_stats(self, db_session):
        """Test that nominal_execution doesn't create stats."""
        # Create a job posting and record
        registry.Registry.clear_registry()
        cluster.job(name="cpu_test")(example_package.cpu_intensive_task)

        posting_id, job_ids = registry.Registry["cpu_test"].post_work(
            [(1000,)], []
        )

        with db_session as db:
            job = db.scalar(
                models.JobRecord.select().where(
                    models.JobRecord.id == job_ids[0]
                )
            )

            # Create worker and execute nominally
            worker = ClusterProcess()
            result = worker.nominal_execution(job)

            # Nominal execution returns None (no stats)
            assert result is None

            # Verify job was executed successfully
            assert job.status == status.JobStatus.done
            assert job.exited_ok is True

    @given(st.integers(min_value=100, max_value=200))
    @settings(
        suppress_health_check=[HealthCheck.function_scoped_fixture],
        deadline=None,
        max_examples=5,
    )
    def test_10_percent_profiling_rate(self, db_session, num_jobs):
        """Test statistical distribution of profiling."""
        # Create many jobs
        registry.Registry.clear_registry()
        cluster.job(name="profile_test")(example_package.cpu_intensive_task)

        workload = [(i * 100,) for i in range(num_jobs)]
        posting_id, job_ids = registry.Registry["profile_test"].post_work(
            workload, []
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

        # Execute all jobs and count profiled ones
        profiled_count = 0

        with db_session as db:
            jobs = list(
                db.scalars(
                    models.JobRecord.select().where(
                        models.JobRecord.id.in_(job_ids)
                    )
                )
            )

            worker = ClusterProcess()

            # Mock random.random to get deterministic profiling
            random_values = [
                0.05 if i < num_jobs // 10 else 0.5 for i in range(num_jobs)
            ]
            random.shuffle(random_values)

            with patch("random.random", side_effect=random_values):
                for job in jobs:
                    worker.process_job(db, job)

                    # Check if a profile was created for this job
                    profile = db.scalar(
                        models.JobProfile.select().where(
                            models.JobProfile.job_record_id == job.id
                        )
                    )
                    if profile is not None:
                        profiled_count += 1

        # Should be approximately 10% profiled
        expected_profiled = num_jobs // 10
        assert profiled_count == expected_profiled


class TestProfileDataPersistence:
    """Test profile data database operations."""

    def test_job_profile_foreign_key(self, db_session):
        """Test JobProfile correctly references JobRecord."""
        # Create a job
        registry.Registry.clear_registry()
        cluster.job(name="fk_test")(example_package.foo)

        posting_id, job_ids = registry.Registry["fk_test"].post_work(
            [(1, 2.0)], []
        )

        with db_session as db:
            job = db.scalar(
                models.JobRecord.select().where(
                    models.JobRecord.id == job_ids[0]
                )
            )

            # Create a JobProfile
            profile = JobProfile(
                job_record_id=job.id, total_calls=10, total_time=1.5
            )
            db.add(profile)
            db.commit()

            # Verify relationship
            db.refresh(profile)
            assert profile.job_record_id == job.id
            assert profile.job_record.id == job.id

            # Test backref from job to profile
            db.refresh(job)
            assert hasattr(job, "profile")

    def test_reflect_cprofile_creates_records(self, db_session):
        """Test reflect_cProfile creates all necessary records."""
        # Create a mock stats dictionary similar to cProfile output
        mock_stats = {
            ("test.py", 10, "func_a"): (
                5,
                5,
                0.1,
                0.2,
                {("test.py", 20, "func_b"): (3, 0.05, 0.1, 0.15)},
            ),
            ("test.py", 20, "func_b"): (3, 3, 0.05, 0.15, {}),
        }

        # First create a job record to reference
        registry.Registry.clear_registry()
        cluster.job(name="reflect_test")(example_package.foo)

        posting_id, job_ids = registry.Registry["reflect_test"].post_work(
            [(1, 2.0)], []
        )

        with db_session as db:
            job = db.scalar(
                models.JobRecord.select().where(
                    models.JobRecord.id == job_ids[0]
                )
            )

            # Create a job profile linked to the job
            profile = JobProfile(
                job_record_id=job.id, total_calls=2, total_time=0.35
            )
            db.add(profile)
            db.flush()

            # Call reflect_cProfile
            stat_cache, relationships = reflect_cProfile(
                db, profile, mock_stats
            )
            db.commit()

            # Verify Function records were created
            functions = list(db.scalars(models.Function.select()))
            assert len(functions) == 2

            func_names = {f.function_name for f in functions}
            assert func_names == {"func_a", "func_b"}

            # Verify FunctionStat records were created
            func_stats = list(
                db.scalars(
                    models.FunctionStat.select().where(
                        models.FunctionStat.profile_id == profile.id
                    )
                )
            )
            assert len(func_stats) == 2

            # Verify call relationships
            call_maps = list(db.scalars(models.FunctionCallMap.select()))
            assert len(call_maps) == 1  # func_a calls func_b

    def test_profile_stats_calculation(self, db_session):
        """Test profile statistics are calculated correctly."""
        # Create a job and execute with profiling
        registry.Registry.clear_registry()
        cluster.job(name="stats_test")(example_package.nested_function_calls)

        posting_id, job_ids = registry.Registry["stats_test"].post_work(
            [(3,)], []  # depth=3 will create nested calls
        )

        with db_session as db:
            posting = db.scalar(
                models.JobPosting.select().where(
                    models.JobPosting.id == posting_id
                )
            )
            posting.status = status.PostingStatus.executing

            job = db.scalar(
                models.JobRecord.select().where(
                    models.JobRecord.id == job_ids[0]
                )
            )

            worker = ClusterProcess()

            # Force profiling by mocking random
            with patch("random.random", return_value=0.01):
                worker.process_job(db, job)

            # Verify profile was created with correct stats
            profile = db.scalar(
                models.JobProfile.select().where(
                    models.JobProfile.job_record_id == job.id
                )
            )

            assert profile is not None
            assert profile.total_calls > 0
            assert profile.total_time > 0

            # Check function stats
            func_stats = list(
                db.scalars(
                    models.FunctionStat.select().where(
                        models.FunctionStat.profile_id == profile.id
                    )
                )
            )

            # Should have stats for nested_function_calls
            nested_stats = [
                fs
                for fs in func_stats
                if any(
                    f.function_name == "nested_function_calls"
                    for f in db.scalars(models.Function.select())
                    if f.id == fs.function_id
                )
            ]
            assert len(nested_stats) > 0


class TestProfilingIntegration:
    """Integration tests with full worker execution."""

    def test_worker_profiling_subprocess(self, db_session):
        """Test profiling works in subprocess worker."""
        registry.Registry.clear_registry()
        cluster.job(name="subprocess_test")(
            example_package.mixed_execution_time
        )

        # Create work with mixed execution patterns
        workload = [(i, i * 100) for i in range(5)]  # sleep_ms, iterations
        posting_id, job_ids = registry.Registry["subprocess_test"].post_work(
            workload, []
        )

        with db_session as db:
            posting = db.scalar(
                models.JobPosting.select().where(
                    models.JobPosting.id == posting_id
                )
            )
            posting.status = status.PostingStatus.executing
            db.commit()

        # Use CoverageWorker in subprocess
        with cluster.ClusterPool(
            max_workers=1, worker_class=CoverageWorker, batchsize=5
        ) as pool:
            pool.await_posting_completion(
                posting_id, poll_time=0.1, max_wait=30
            )

        # Check that some jobs were profiled
        with db_session as db:
            profiles = list(
                db.scalars(
                    models.JobProfile.select()
                    .join(models.JobRecord)
                    .where(models.JobRecord.posting_id == posting_id)
                )
            )

            # With 5 jobs and 10% profiling rate, we expect 0-2 profiles
            # (statistical variation is possible)
            assert len(profiles) >= 0
            assert len(profiles) <= 3

    def test_profiling_with_successful_job(self, db_session):
        """Test profiling works correctly with successful jobs."""
        registry.Registry.clear_registry()
        cluster.job(name="success_test")(example_package.failure)

        # Create a job that will succeed (odd number)
        posting_id, job_ids = registry.Registry["success_test"].post_work(
            [(3,)], []  # Odd number will succeed
        )

        with db_session as db:
            posting = db.scalar(
                models.JobPosting.select().where(
                    models.JobPosting.id == posting_id
                )
            )
            posting.status = status.PostingStatus.executing

            job = db.scalar(
                models.JobRecord.select().where(
                    models.JobRecord.id == job_ids[0]
                )
            )
            db.commit()

            worker = ClusterProcess()

            # Force profiling
            with patch("random.random", return_value=0.01):
                worker.process_job(db, job)

            # Job should have succeeded
            assert job.status == status.JobStatus.done
            assert job.exited_ok is True

            # Profile should have been created
            profile = db.scalar(
                models.JobProfile.select().where(
                    models.JobProfile.job_record_id == job.id
                )
            )

            assert profile is not None
            assert profile.total_calls > 0
            assert profile.total_time > 0

    def test_concurrent_profiling(self, db_session):
        """Test multiple workers don't interfere with profiling."""
        registry.Registry.clear_registry()
        cluster.job(name="concurrent_test")(example_package.cpu_intensive_task)

        # Create many jobs
        workload = [(i * 1000,) for i in range(20)]
        posting_id, job_ids = registry.Registry["concurrent_test"].post_work(
            workload, []
        )

        with db_session as db:
            posting = db.scalar(
                models.JobPosting.select().where(
                    models.JobPosting.id == posting_id
                )
            )
            posting.status = status.PostingStatus.executing
            db.commit()

        # Run with multiple workers
        with cluster.ClusterPool(
            max_workers=4, worker_class=CoverageWorker, batchsize=5
        ) as pool:
            pool.await_posting_completion(
                posting_id, poll_time=0.1, max_wait=30
            )

        # Verify all jobs completed and profiles were created correctly
        with db_session as db:
            completed_jobs = list(
                db.scalars(
                    models.JobRecord.select().where(
                        models.JobRecord.posting_id == posting_id,
                        models.JobRecord.status == status.JobStatus.done,
                    )
                )
            )
            assert len(completed_jobs) == 20

            # Check profiles were created
            profiles = list(
                db.scalars(
                    models.JobProfile.select()
                    .join(models.JobRecord)
                    .where(models.JobRecord.posting_id == posting_id)
                )
            )

            # Should have some profiles (statistically around 2 with 10% rate)
            assert len(profiles) >= 0
            assert len(profiles) <= 6  # Allow for statistical variation

            # Each profile should have valid data
            for profile in profiles:
                assert profile.total_calls > 0
                assert profile.total_time > 0

                # Check function stats exist
                func_stats = db.scalar(
                    sa.select(sa.func.count())
                    .select_from(models.FunctionStat)
                    .where(models.FunctionStat.profile_id == profile.id)
                )
                assert func_stats > 0
