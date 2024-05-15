"""
This testing module is for testing pdoflow's reaction to function
workload failures.
"""

from hypothesis import HealthCheck, given, note, settings

from pdoflow import cluster, registry
from pdoflow.models import JobRecord
from tests import strategies
from tests.example_package import failure


@given(strategies.failure_workload())
@settings(
    deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture]
)
def test_retries(db_session, workload):
    registry.Registry.clear_registry()

    cluster.job()(failure)

    posting_id, _ = registry.Registry[failure].post_work(workload, [])

    with cluster.ClusterPool(max_workers=1) as pool:
        pool.await_posting_completion(posting_id)

    q = JobRecord.select(
        "tries_remaining",
        "exited_ok",
        "status",
        JobRecord.pos_args[0].label("fail_arg"),
    ).where(JobRecord.posting_id == posting_id)

    with db_session as db:
        results = db.execute(q)
        for id_, tries_remaining, exited_ok, status, fail_arg in results:
            note(str(id_))
            if fail_arg % 2 == 0:
                assert tries_remaining < 1
                assert not exited_ok
                assert status == status.JobStatus.errored_out
            else:
                assert tries_remaining > 0
                assert exited_ok
                assert status == status.JobStatus.done
