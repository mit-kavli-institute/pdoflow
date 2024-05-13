import example_package
import sqlalchemy as sa
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from pdoflow import cluster, models, registry, status


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


@given(
    st.lists(
        st.tuples(
            st.integers(), st.floats(allow_nan=False, allow_infinity=False)
        ),
        min_size=1,
    )
)
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


@given(
    st.lists(
        st.tuples(
            st.integers(), st.floats(allow_nan=False, allow_infinity=False)
        ),
        min_size=1,
    )
)
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_default_status(db_session, workload):

    registry.Registry.clear_registry()
    cluster.job()(example_package.foo)

    posting_id, _ = registry.Registry[example_package.foo].post_work(
        workload, []
    )

    with db_session as db:
        q = sa.select(models.JobPosting).where(
            models.JobPosting.id == posting_id,
            models.JobPosting.status == status.PostingStatus.executing,
        )
        remote_post = db.scalar(q)
        assert posting_id == remote_post.id
        assert all(
            job.status == status.JobStatus.waiting for job in remote_post.jobs
        )
