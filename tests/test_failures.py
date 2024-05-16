"""
This testing module is for testing pdoflow's reaction to function
workload failures.
"""

from hypothesis import HealthCheck, given, note, settings
from loguru import logger

from pdoflow import cluster, registry
from pdoflow.models import JobRecord
from pdoflow.status import JobStatus
from tests import strategies
from tests.example_package import failure


@given(strategies.failure_workload())
@settings(
    deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture]
)
def test_retries(db_session, workload):
    logger.remove()
    registry.Registry.clear_registry()

    cluster.job()(failure)

    posting_id, _ = registry.Registry[failure].post_work(workload, [])

    with cluster.ClusterPool(max_workers=1) as pool:
        try:
            pool.await_posting_completion(posting_id, max_wait=1.0)
            timed_out = False
        except TimeoutError:
            timed_out = True

    q = JobRecord.select(
        "id",
        "tries_remaining",
        "exited_ok",
        "status",
        JobRecord.pos_args[0].label("fail_arg"),
    ).where(JobRecord.posting_id == posting_id)

    with db_session as db:
        results = db.execute(q)
        for row in results:
            note(str(row.id))
            if row.fail_arg % 2 == 0:
                if timed_out:
                    assert row.status in (
                        JobStatus.errored_out,
                        JobStatus.waiting,
                    )
                else:
                    assert row.status == JobStatus.errored_out
            else:
                if timed_out:
                    assert row.status in (JobStatus.done, JobStatus.waiting)
                else:
                    assert row.status == JobStatus.done
