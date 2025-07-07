import sqlalchemy as sa
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from pdoflow import cluster, models, registry, status
from pdoflow.io import Session
from pdoflow.utils import load_function
from tests.utils import CoverageWorker

from . import example_package, strategies


@given(st.one_of(st.none(), st.text()))
def test_registration(name):
    registry.Registry.clear_registry()

    @cluster.job(name=name)
    def foo():
        return 1

    if name is None:
        assert foo.__name__ in registry.Registry
    else:
        assert name in registry.Registry


@given(strategies.foo_workload())
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_workupload(db_session, workload):

    registry.Registry.clear_registry()
    cluster.job()(example_package.foo)

    posting_id, job_ids = registry.Registry[example_package.foo].post_work(
        workload, []
    )

    with db_session as db:
        q = (
            sa.select(sa.func.count(models.JobRecord.id))
            .join(models.JobRecord.posting)
            .where(models.JobPosting.id == posting_id)
        )
        assert len(job_ids) == db.scalar(q)


@given(strategies.foo_workload())
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_default_status(db_session, workload):

    registry.Registry.clear_registry()
    cluster.job()(example_package.foo)

    posting_id, _ = registry.Registry[example_package.foo].post_work(
        workload, []
    )

    with db_session as db:
        q = models.JobPosting.select().where(
            models.JobPosting.id == posting_id,
            models.JobPosting.status == status.PostingStatus.executing,
        )
        remote_post = db.scalar(q)
        assert posting_id == remote_post.id
        assert all(
            job.status == status.JobStatus.waiting for job in remote_post.jobs
        )


@given(
    strategies.foo_workload(),
    st.integers(min_value=1, max_value=1000),
)
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_no_duplicate_work(db_session, workload, batchsize):
    registry.Registry.clear_registry()
    cluster.job()(example_package.foo)
    posting_id, _ = registry.Registry[example_package.foo].post_work(
        workload, []
    )

    with db_session as left_db, Session() as right_db:
        q = models.JobRecord.get_available(batchsize).where(
            models.JobRecord.posting_id == posting_id
        )

        left_results = left_db.scalars(q)
        right_results = right_db.scalars(q)

        left_ids = {job.id for job in left_results}
        right_ids = {job.id for job in right_results}

        assert left_ids.isdisjoint(right_ids)


@given(
    strategies.foo_workload(),
    st.just("tests.example_package.foo"),
)
@settings(
    suppress_health_check=[HealthCheck.function_scoped_fixture], deadline=None
)
def test_dynamic_execution(db_session, workload, path):
    registry.Registry.clear_registry()
    function = load_function(path)
    cluster.job()(function)
    posting_id, _ = registry.Registry[function].post_work(workload, [])

    q = models.JobRecord.get_available(len(workload)).where(
        models.JobRecord.posting_id == posting_id
    )

    with db_session as db:
        jobs = db.scalars(q)
        for job in jobs:
            ref = function(*job.pos_args, **job.kwargs)
            check = job.execute()

            assert ref == check
            assert job.status == status.JobStatus.done

            db.commit()

    with db_session as db:
        q = sa.select(models.JobRecord).where(
            models.JobRecord.posting_id == posting_id
        )
        jobs = db.scalars(q)

        # Assert that the job's status has been written to storage
        for job in jobs:
            assert job.status == status.JobStatus.done


@given(st.just("tests.example_package.foo"))
def test_dynamic_load(path):
    func = load_function(path)
    assert func(10, 10.0) == 10 * 10.0


@given(strategies.foo_workload())
@settings(
    suppress_health_check=[HealthCheck.function_scoped_fixture], deadline=None
)
def test_work_instantiation(db_session, workload):
    registry.Registry.clear_registry()
    cluster.job()(example_package.foo)

    posting_id, _ = registry.Registry[example_package.foo].post_work(
        workload, []
    )

    with cluster.ClusterPool(
        max_workers=1, worker_class=CoverageWorker
    ) as pool:
        try:
            pool.await_posting_completion(posting_id, max_wait=10)
            cluster.await_for_status_threshold(
                posting_id, status.JobStatus.executing
            )
            timed_out = False
        except TimeoutError:
            timed_out = True

    with db_session as db:
        q = (
            sa.select(models.JobRecord)
            .join(models.JobRecord.posting)
            .where(models.JobPosting.id == posting_id)
        )
        if timed_out:
            n_exited = 0
            n_waiting = 0
            for job in db.scalars(q):
                if job.status.exited:
                    n_exited += 1
                else:
                    n_waiting += 1

            assert n_exited > n_waiting
        else:
            for job in db.scalars(q):
                assert job.status.exited()
