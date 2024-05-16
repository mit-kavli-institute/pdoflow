import getpass
import pathlib
import typing
import uuid
from datetime import datetime
from typing import Any, Optional

import sqlalchemy as sa
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.sql.expression import Select

from pdoflow.status import JobStatus, PostingStatus
from pdoflow.utils import load_function


class PathType(sa.types.TypeDecorator):
    """
    Adapts PosixPath types to string while resolving their absolute
    paths to be stored in the database.
    """

    impl = sa.types.String

    cache_ok = True

    def process_bind_param(
        self, value: pathlib.PosixPath | None, dialect: sa.Dialect
    ):
        if value is None:
            return None
        return str(value.resolve())

    def process_result_value(self, value: Any, dialect: sa.Dialect):
        return pathlib.Path(value)


class Base(DeclarativeBase):
    """
    A Base class for PDOFlow which defines common attributes and logic
    for all dervied PDOFlow database Models.
    """

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True, server_default=sa.text("gen_random_uuid()")
    )

    @classmethod
    def select(cls, *fields: sa.ColumnElement | str) -> sa.Select:
        """
        Helper function to avoid import statements just for Select objects.
        """
        if len(fields) > 0:
            columns = []
            for field in fields:
                col = getattr(cls, field) if isinstance(field, str) else field
                columns.append(col)
            return sa.select(*columns)
        else:
            return sa.select(cls)


class CreatedOnMixin:
    created_on: Mapped[datetime] = mapped_column(
        server_default=sa.text("now()")
    )


class JobPosting(CreatedOnMixin, Base):

    __tablename__ = "job_postings"

    poster: Mapped[Optional[str]] = mapped_column(default=getpass.getuser())
    status: Mapped[PostingStatus] = mapped_column(default=PostingStatus.paused)
    target_function: Mapped[str]
    entry_point: Mapped[str]

    jobs: Mapped[list["JobRecord"]] = relationship(
        "JobRecord", back_populates="posting"
    )

    def __repr__(self):
        return (
            f"<JobPosting {self.id}: {self.status} -> "
            f"{self.entry_point}[{self.target_function}>"
        )

    def __len__(self):
        return self.total_jobs

    @hybrid_property
    def total_jobs(self) -> int:
        return len(self.jobs)

    @total_jobs.inplace.expression
    @classmethod
    def _total_jobs(cls):
        q = (
            cls.select(sa.func.count(JobRecord.id))
            .where(JobRecord.posting_id == cls.id)
            .correlate(cls)
            .label("total_jobs")
        )
        return q

    @hybrid_property
    def total_jobs_done(self):
        return len(
            list(filter(lambda j: j.status != JobStatus.waiting, self.jobs))
        )

    @total_jobs_done.inplace.expression
    @classmethod
    def _total_jobs_done(cls):
        q = (
            cls.select(sa.func.count(JobRecord.id))
            .where(
                JobRecord.posting_id == cls.id,
                JobRecord.status != JobStatus.waiting,
            )
            .correlate(cls)
            .label("total_jobs_done")
        )
        return q

    @hybrid_property
    def percent_done(self) -> float:
        n_jobs = 0
        n_stopped = 0

        for job in self.jobs:
            if job.status != JobStatus.waiting:
                n_stopped += 1
            n_jobs += 1

        if n_jobs == 0:
            return float("nan")

        return (n_stopped / n_jobs) * 100.0

    @percent_done.inplace.expression
    @classmethod
    def _percent_done(cls):
        return (cls.total_jobs_done / cls.total_jobs) * 100.0


class JobRecord(CreatedOnMixin, Base):

    __tablename__ = "job_records"

    posting_id: Mapped[uuid.UUID] = mapped_column(
        sa.ForeignKey(
            JobPosting.id,
        )
    )
    posting: Mapped[JobPosting] = relationship(
        JobPosting, back_populates="jobs"
    )

    priority: Mapped[int]
    positional_arguments: Mapped[tuple] = mapped_column(sa.JSON)
    keyword_arguments: Mapped[Optional[dict]] = mapped_column(sa.JSON)
    tries_remaining: Mapped[int]

    status: Mapped[JobStatus] = mapped_column(default=JobStatus.waiting)
    exited_ok: Mapped[Optional[bool]]
    work_started_on: Mapped[Optional[datetime]]
    completed_on: Mapped[Optional[datetime]]

    __table_args__ = (
        sa.CheckConstraint("tries_remaining >= 0", name="no_negative_tries"),
        sa.CheckConstraint(
            "completed_on IS NULL OR created_on < completed_on",
            name="no_unphysical_dates",
        ),
    )

    def __repr__(self):
        return f"<Job {self.id} f({self.pos_args}): {self.status}>"

    @hybrid_property
    def pos_args(self) -> tuple:
        return self.positional_arguments

    @hybrid_property
    def kwargs(self) -> dict[str, Any]:
        return self.keyword_arguments if self.keyword_arguments else {}

    @hybrid_property
    def waiting_time(self):
        if self.work_started_on is None:
            return None
        return self.created_on - self.work_started_on

    @waiting_time.inplace.expression
    @classmethod
    def _waiting_time(self):
        raise NotImplementedError

    @classmethod
    def get_available(cls, batchsize: int) -> Select:
        q = (
            sa.select(cls)
            .join(cls.posting)
            .where(
                JobPosting.status == PostingStatus.executing,
                JobRecord.status == JobStatus.waiting,
                JobRecord.tries_remaining > 0,
            )
            .with_for_update(skip_locked=True)
            .limit(batchsize)
        )
        return q

    def execute(self) -> typing.Any:
        function = load_function(self.posting.entry_point)
        kwargs = self.keyword_arguments if self.keyword_arguments else {}
        result = function(*self.positional_arguments, **kwargs)
        self.status = JobStatus.done
        self.exited_ok = True
        return result

    def mark_as_bad(self):
        self.exited_ok = False
        self.status = JobStatus.errored_out
        self.tries_remaining = 0
