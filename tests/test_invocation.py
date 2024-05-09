import sqlalchemy as sa
from hypothesis import strategies as st, given
from pdoflow import cluster, models, registry, status
from .reference_file import foo


@given(st.one_of(st.none(), st.text()))
def test_registration(name):
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
            st.integers(),
            st.floats(allow_nan=False, allow_infinity=False)
        ),
        min_size=1
    )
)
def test_workupload(db_session, workload):

    cluster.job()(foo)

    posting, jobs = registry.Registry[foo].post_work(workload, [])

    with db_session() as db:
        q = (
            sa.select(sa.func.count(models.JobRecord.id))
            .join(models.JobRecord.posting)
            .where(models.JobPosting.id == posting.id)
        )
        assert len(jobs) == db.scalar(q)


@given(
    st.lists(
        st.tuples(
            st.integers(),
            st.floats(allow_nan=False, allow_infinity=False)
        ),
        min_size=1
    )
)
def test_default_status(db_session, workload):

    cluster.job()(foo)

    posting, _ = registry.Registry[foo].post_work(workload, [])

    with db_session() as db:
        q = (
            sa.select(models.JobPosting)
            .where(
                models.JobPosting.id == posting.id,
                models.JobPosting.status == status.PostingStatus.executing
            )
        )
        remote_post = db.scalar(q)
        assert posting == remote_post
        assert all(job.status == status.JobStatus.waiting for job in posting.jobs)
