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
    logger.level("ERROR")
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
                if timed_out and row.tries_remaining > 0:
                    assert row.status == JobStatus.waiting
                else:
                    assert row.tries_remaining < 1
                    assert not row.exited_ok
                    assert row.status == JobStatus.errored_out
            else:
                assert row.tries_remaining > 0
                assert row.exited_ok
                assert row.status == JobStatus.done
