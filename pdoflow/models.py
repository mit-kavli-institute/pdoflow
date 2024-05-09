from datetime import datetime
import pathlib
import deal
import getpass
from typing import Optional
import sqlalchemy as sa
from sqlalchemy import orm
import uuid

from pdoflow.status import JobStatus, PostingStatus


class Base(orm.DeclarativeBase):
    id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.Uuid,
        primary_key=True,
        server_default=sa.text("uuid_generate_v4()")
    )


class CreatedOnMixin:
    created_on: orm.Mapped[datetime] = orm.mapped_column(server_default=sa.text("now()"))



class JobPosting(CreatedOnMixin, Base):
    poster: orm.Mapped[Optional[str]] = orm.mapped_column(default=getpass.getuser())
    status: orm.Mapped[PostingStatus] = orm.mapped_column(default=PostingStatus.paused)
    target_function: orm.Mapped[str]
    entry_point: orm.Mapped[pathlib.Path] = orm.mapped_column(sa.String())


@deal.inv(
    lambda inst: inst.tries_remaining >= 0 or inst.tries_remaining is None,
    message="Job instance should never have negative tries remaining."
)
@deal.inv(
    lambda inst: inst.completed_on < inst.created_on,
    message="Job should not be completed before created."
)
class JobRecord(CreatedOnMixin, Base):

    posting_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.ForeignKey(
            JobPosting.id,
            back_populates="jobs"
        )
    )
    posting: orm.Mapped[JobPosting] = orm.relationship(
        JobPosting,
        back_populates="jobs"
    )

    priority: orm.Mapped[int]
    positional_arguments: orm.Mapped[sa.JSON]
    keyword_arguments: orm.Mapped[Optional[sa.JSON]]
    tries_remaining: orm.Mapped[int]

    status: orm.Mapped[JobStatus]
    exited_ok: orm.Mapped[Optional[bool]]
    completed_on: orm.Mapped[Optional[datetime]]

    __table_args__ = (
        sa.CheckConstraint(
            "tries_remaining >= 0",
            name="no_negative_tries"
        ),
    )
