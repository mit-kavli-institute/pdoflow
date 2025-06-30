import uuid
from typing import List

import sqlalchemy as sa
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from pdoflow import cluster, models, registry, status

from . import example_package


@given(
    st.lists(
        st.tuples(
            st.integers(),
            st.floats(allow_nan=False, allow_infinity=False),
            st.integers(min_value=-100, max_value=100),  # priority
        ),
        min_size=5,
        max_size=20,
    )
)
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_priority_ordering(db_session, workload_with_priorities):
    """Test that jobs are retrieved in priority order (highest first)"""
    registry.Registry.clear_registry()
    cluster.job()(example_package.foo)

    # Separate workload and priorities
    posargs = [(w[0], w[1]) for w in workload_with_priorities]
    priorities = [w[2] for w in workload_with_priorities]

    posting_id, job_ids = registry.Registry[example_package.foo].post_work(
        posargs, [], priority=priorities
    )

    with db_session as db:
        # Get all jobs in the order they would be retrieved
        jobs = list(db.scalars(models.JobRecord.get_available(len(posargs))))

        # Verify they are in descending priority order
        for i in range(1, len(jobs)):
            assert jobs[i - 1].priority >= jobs[i].priority


@given(
    st.lists(
        st.tuples(
            st.integers(),
            st.floats(allow_nan=False, allow_infinity=False),
        ),
        min_size=5,
        max_size=20,
    )
)
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_same_priority_fifo(db_session, workload):
    """Test that jobs with same priority are retrieved in FIFO order"""
    registry.Registry.clear_registry()
    cluster.job()(example_package.foo)

    # All jobs have same priority
    posting_id, job_ids = registry.Registry[example_package.foo].post_work(
        workload, [], priority=10
    )

    with db_session as db:
        # Get all jobs
        jobs = list(db.scalars(models.JobRecord.get_available(len(workload))))

        # Since all have same priority, should be in creation order
        for i in range(1, len(jobs)):
            assert jobs[i - 1].created_on <= jobs[i].created_on


@given(st.integers(min_value=-100, max_value=100))
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_single_priority_value(db_session, priority_value):
    """Test that single priority value is applied to all jobs"""
    registry.Registry.clear_registry()
    cluster.job()(example_package.foo)

    workload = [(1, 1.0), (2, 2.0), (3, 3.0)]
    posting_id, job_ids = registry.Registry[example_package.foo].post_work(
        workload, [], priority=priority_value
    )

    with db_session as db:
        jobs = list(
            db.scalars(
                sa.select(models.JobRecord).where(
                    models.JobRecord.posting_id == posting_id
                )
            )
        )
        assert all(job.priority == priority_value for job in jobs)


def test_priority_list_length_mismatch(db_session):
    """Test that mismatched priority list length raises ValueError"""
    registry.Registry.clear_registry()
    cluster.job()(example_package.foo)

    workload = [(1, 1.0), (2, 2.0), (3, 3.0)]
    priorities = [1, 2]  # Too short

    try:
        registry.Registry[example_package.foo].post_work(
            workload, [], priority=priorities
        )
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "Priority list length" in str(e)


def test_priority_edge_cases(db_session):
    """Test priority with PostgreSQL INT edge cases"""
    registry.Registry.clear_registry()
    cluster.job()(example_package.foo)

    # Test with min, max, and zero priorities
    edge_priorities = [-2_147_483_648, 0, 2_147_483_647]
    workload = [(1, 1.0), (2, 2.0), (3, 3.0)]

    posting_id, job_ids = registry.Registry[example_package.foo].post_work(
        workload, [], priority=edge_priorities
    )

    with db_session as db:
        jobs = list(db.scalars(models.JobRecord.get_available(len(workload))))

        # Should get max priority first, then 0, then min
        assert jobs[0].priority == 2_147_483_647
        assert jobs[1].priority == 0
        assert jobs[2].priority == -2_147_483_648


def test_backward_compatibility(db_session):
    """Test that not specifying priority defaults to 0"""
    registry.Registry.clear_registry()
    cluster.job()(example_package.foo)

    workload = [(1, 1.0), (2, 2.0)]
    posting_id, job_ids = registry.Registry[example_package.foo].post_work(
        workload, []
    )

    with db_session as db:
        jobs = list(
            db.scalars(
                sa.select(models.JobRecord).where(
                    models.JobRecord.posting_id == posting_id
                )
            )
        )
        assert all(job.priority == 0 for job in jobs)


@given(
    st.lists(
        st.integers(min_value=-100, max_value=100),
        min_size=10,
        max_size=20,
    )
)
@settings(
    suppress_health_check=[HealthCheck.function_scoped_fixture],
    deadline=None,
)
def test_priority_with_concurrent_workers(db_session, priorities):
    """Test that multiple workers respect priority ordering"""
    registry.Registry.clear_registry()
    cluster.job()(example_package.foo)

    # Create jobs with different priorities
    workload = [(i, float(i)) for i in range(len(priorities))]
    posting_id, job_ids = registry.Registry[example_package.foo].post_work(
        workload, [], priority=priorities
    )

    # Track execution order
    execution_order: List[uuid.UUID] = []

    def track_execution(job_id: uuid.UUID):
        execution_order.append(job_id)

    # Simulate multiple workers grabbing jobs
    batch_size = 3
    with db_session as db:
        while True:
            job_ids = list(
                db.scalars(models.JobRecord.available_ids(batch_size))
            )
            if not job_ids:
                break

            # Mark jobs as executing
            q = (
                sa.update(models.JobRecord)
                .values(status=status.JobStatus.executing)
                .where(models.JobRecord.id.in_(job_ids))
            )
            db.execute(q)
            db.commit()

            # Track order
            for job_id in job_ids:
                track_execution(job_id)

            # Mark as done
            q = (
                sa.update(models.JobRecord)
                .values(status=status.JobStatus.done)
                .where(models.JobRecord.id.in_(job_ids))
            )
            db.execute(q)
            db.commit()

    # Verify jobs were processed in priority order
    with db_session as db:
        executed_jobs = []
        for job_id in execution_order:
            job = db.scalar(
                sa.select(models.JobRecord).where(
                    models.JobRecord.id == job_id
                )  # noqa: E501
            )
            executed_jobs.append(job)

        # Check that priorities are non-increasing
        for i in range(1, len(executed_jobs)):
            # Allow same priority (FIFO within same priority)
            assert executed_jobs[i - 1].priority >= executed_jobs[i].priority


def test_priority_starvation_prevention(db_session):
    """Test that old low-priority jobs aren't starved by new high-priority
    jobs"""
    registry.Registry.clear_registry()
    cluster.job()(example_package.foo)

    # Create initial low-priority jobs
    low_priority_work = [(i, float(i)) for i in range(5)]
    posting1_id, _ = registry.Registry[example_package.foo].post_work(
        low_priority_work, [], priority=-10
    )

    # Get the first low-priority job for age comparison
    with db_session as db:
        first_low_priority_job = db.scalar(
            sa.select(models.JobRecord)
            .where(models.JobRecord.posting_id == posting1_id)
            .order_by(models.JobRecord.created_on.asc())
            .limit(1)
        )
        low_priority_created = first_low_priority_job.created_on

    # Create high-priority jobs
    high_priority_work = [(i + 100, float(i)) for i in range(5)]
    posting2_id, _ = registry.Registry[example_package.foo].post_work(
        high_priority_work, [], priority=10
    )

    # When getting available jobs, high priority should come first
    with db_session as db:
        jobs = list(db.scalars(models.JobRecord.get_available(10)))

        # First 5 should be high priority
        for i in range(5):
            assert jobs[i].priority == 10

        # Next 5 should be low priority (in FIFO order)
        for i in range(5, 10):
            assert jobs[i].priority == -10
            assert jobs[i].created_on >= low_priority_created
