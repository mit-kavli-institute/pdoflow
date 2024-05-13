import getpass
import pathlib
import uuid
from datetime import datetime
from typing import Optional

import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

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


class Base(DeclarativeBase):
    id: Mapped[uuid.UUID] = mapped_column(
        sa.Uuid, primary_key=True, server_default=sa.text("gen_random_uuid()")
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
    entry_point: Mapped[str] = mapped_column(PathType)

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
