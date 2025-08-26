"""
This testing module is for testing pdoflow's reaction to function
workload failures.
"""

import pytest
import sqlalchemy as sa
from hypothesis import HealthCheck, given, note, settings
from loguru import logger

from pdoflow import cluster, registry
from pdoflow.models import JobPosting, JobRecord
from pdoflow.status import JobStatus, PostingStatus
from tests import strategies
from tests.example_package import failure


@pytest.mark.timeout(0)
@given(strategies.failure_workload())
@settings(
    deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture]
)
def test_retries(db_session, workload):
    logger.remove()
    registry.Registry.clear_registry()

    db_session.execute(sa.delete(JobRecord))
    db_session.execute(sa.delete(JobPosting))
    db_session.commit()

    cluster.job()(failure)

    posting_id, _ = registry.Registry[failure].post_work(workload, [])

    with cluster.ClusterPool(max_workers=1) as pool:
        pool.await_posting_completion(posting_id, poll_time=0.01)
        cluster.await_for_status_threshold(posting_id, JobStatus.executing)

    q = JobRecord.select(
        "id",
        "tries_remaining",
        "exited_ok",
        "status",
        JobRecord.pos_args[0].label("fail_arg"),
    ).where(JobRecord.posting_id == posting_id)

    posting = db_session.scalar(
        sa.select(JobPosting).where(JobPosting.id == posting_id)
    )
    if len(workload) > 0:
        assert posting.status in (
            PostingStatus.errored_out,
            PostingStatus.finished,
        )
    results = db_session.execute(q)
    for row in results:
        note(str(row))
        if row.fail_arg % 2 == 0:
            assert row.status in (JobStatus.errored_out, JobStatus.waiting)
        else:
            assert row.status in (JobStatus.done, JobStatus.waiting)
