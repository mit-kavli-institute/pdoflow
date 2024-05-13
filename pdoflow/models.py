from datetime import datetime
import pathlib
import deal
import getpass
from typing import Optional
import sqlalchemy as sa
from sqlalchemy import orm
import uuid

from pdoflow.status import JobStatus, PostingStatus


class PathType(sa.types.TypeDecorator):
    """
    Adapts PosixPath types to string while resolving their absolute
    paths to be stored in the database.
    """
    impl = sa.types.String

    cache_ok = True

    def process_bind_param(self, value: pathlib.PosixPath, dialect: str):
        return str(value.resolve())

    def process_result_value(self, value: str, dialect: str):
        return pathlib.Path(value)


class Base(orm.DeclarativeBase):
    id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.Uuid,
        primary_key=True,
        server_default=sa.text("gen_random_uuid()")
    )


class CreatedOnMixin:
    created_on: orm.Mapped[datetime] = orm.mapped_column(server_default=sa.text("now()"))



class JobPosting(CreatedOnMixin, Base):

    __tablename__ = "job_postings"

    poster: orm.Mapped[Optional[str]] = orm.mapped_column(default=getpass.getuser())
    status: orm.Mapped[PostingStatus] = orm.mapped_column(default=PostingStatus.paused)
    target_function: orm.Mapped[str]
    entry_point: orm.Mapped[str] = orm.mapped_column(PathType)

    jobs: orm.Mapped[list["JobRecord"]] = orm.relationship(
        "JobRecord",
        back_populates="posting"
    )


class JobRecord(CreatedOnMixin, Base):

    __tablename__ = "job_records"

    posting_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.ForeignKey(
            JobPosting.id,
        )
    )
    posting: orm.Mapped[JobPosting] = orm.relationship(
        JobPosting,
        back_populates="jobs"
    )

    priority: orm.Mapped[int]
    positional_arguments: orm.Mapped[tuple] = orm.mapped_column(sa.JSON)
    keyword_arguments: orm.Mapped[Optional[dict]] = orm.mapped_column(sa.JSON)
    tries_remaining: orm.Mapped[int]

    status: orm.Mapped[JobStatus] = orm.mapped_column(default=JobStatus.waiting)
    exited_ok: orm.Mapped[Optional[bool]]
    completed_on: orm.Mapped[Optional[datetime]]

    __table_args__ = (
        sa.CheckConstraint(
            "tries_remaining >= 0",
            name="no_negative_tries"
        ),
        sa.CheckConstraint(
            "completed_on IS NULL OR created_on < completed_on",
            name="no_unphysical_dates"
        )
    )
