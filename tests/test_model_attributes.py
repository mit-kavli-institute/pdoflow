from math import isnan

import pytest
import sqlalchemy as sa
from hypothesis import HealthCheck, given, note, settings

from pdoflow import models as m

from . import strategies as pdo_st


@given(pdo_st.posting_with_records())
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_posting_percentage(db_session, posting: m.JobPosting):
    with db_session:
        posting = db_session.merge(posting)
        if len(posting) == 0:
            assert isnan(posting.percent_done)
        else:
            n_exited = 0
            n_waiting = 0

            for job in posting:
                if job.status.exited():
                    n_exited += 1
                else:
                    n_waiting += 1
            assert (n_exited / len(posting)) * 100 == posting.percent_done


@given(pdo_st.posting_with_records())
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_posting_percentage_expr(db_session, posting: m.JobPosting):
    with db_session:
        posting = db_session.merge(posting)
        q = sa.select(m.JobPosting.percent_done).where(
            m.JobPosting.id == posting.id
        )

        note(
            str(
                (
                    posting.total_jobs,
                    db_session.scalar(
                        sa.select(m.JobPosting.total_jobs).where(
                            m.JobPosting.id == posting.id
                        )
                    ),
                )
            )
        )
        note(posting.total_jobs_done)

        if isnan(posting.percent_done):
            assert isnan(db_session.scalar(q))
        else:
            assert pytest.approx(posting.percent_done) == db_session.scalar(q)


@given(pdo_st.job_postings())
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_posting_repr(db_session, posting: m.JobPosting):
    with db_session:
        db_session.merge(posting)
        # Just check that certain information is in repr
        string = repr(posting)
        assert str(posting.id) in string
        assert str(posting.status) in string
        assert posting.entry_point in string
        assert posting.target_function in string


@given(pdo_st.job_postings())
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_total_jobs_done(db_session, posting: m.JobPosting):
    with db_session:

        posting = db_session.merge(posting)
        q = sa.select(m.JobPosting.total_jobs_done).where(
            m.JobPosting.id == posting.id
        )
        sql_total_jobs_done = db_session.scalar(q)

        assert posting.total_jobs == db_session.scalar(
            sa.select(m.JobPosting.total_jobs).where(
                m.JobPosting.id == posting.id
            )
        )
        assert posting.total_jobs_done == db_session.scalar(
            sa.select(m.JobPosting.total_jobs_done).where(
                m.JobPosting.id == posting.id
            )
        )
        assert posting.total_jobs_done == sql_total_jobs_done


@given(pdo_st.job_postings())
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_total_jobs(db_session, posting: m.JobPosting):
    with db_session:
        posting = db_session.merge(posting)
        q = sa.select(m.JobPosting.total_jobs).where(
            m.JobPosting.id == posting.id
        )
        assert posting.total_jobs == db_session.scalar(q)
