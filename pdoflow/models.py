import getpass
import pathlib
import typing
import uuid
from datetime import datetime
from typing import Any, Optional

import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.sql.expression import Select
from sqlalchemy.util import hybridproperty

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
    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True, server_default=sa.text("gen_random_uuid()")
    )


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
    completed_on: Mapped[Optional[datetime]]

    __table_args__ = (
        sa.CheckConstraint("tries_remaining >= 0", name="no_negative_tries"),
        sa.CheckConstraint(
            "completed_on IS NULL OR created_on < completed_on",
            name="no_unphysical_dates",
        ),
    )

    @hybridproperty
    def pos_args(self) -> tuple:
        return self.positional_arguments

    @hybridproperty
    def kwargs(self) -> dict[str, Any]:
        return self.keyword_arguments if self.keyword_arguments else {}

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
