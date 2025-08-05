import getpass
import typing
import uuid
from datetime import datetime
from typing import Any, Iterable, Optional

import sqlalchemy as sa
from sqlalchemy import orm
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.sql.expression import Select

from pdoflow.status import JobStatus, PostingStatus
from pdoflow.utils import load_function


class Base(orm.DeclarativeBase):
    """
    A Base class for PDOFlow which defines common attributes and logic
    for all dervied PDOFlow database Models.
    """

    id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        primary_key=True, server_default=sa.text("gen_random_uuid()")
    )

    @classmethod
    def select(cls, *fields: typing.Union[str, sa.ColumnElement]) -> sa.Select:
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
    created_on: orm.Mapped[datetime] = orm.mapped_column(
        server_default=sa.text("now()")
    )


class JobPosting(CreatedOnMixin, Base):

    __tablename__ = "job_postings"

    poster: orm.Mapped[Optional[str]] = orm.mapped_column(
        default=getpass.getuser()
    )
    status: orm.Mapped[PostingStatus] = orm.mapped_column(
        default=PostingStatus.paused
    )
    target_function: orm.Mapped[str]
    entry_point: orm.Mapped[str]

    jobs: orm.Mapped[list["JobRecord"]] = orm.relationship(
        "JobRecord", back_populates="posting"
    )
    variables: orm.Mapped[list["JobPostingVariable"]] = orm.relationship(
        "JobPostingVariable",
        back_populates="posting",
        cascade="all, delete-orphan",
    )

    def __repr__(self):
        return (
            f"<JobPosting {self.id}: {self.status} -> "
            f"{self.entry_point}[{self.target_function}>"
        )

    def __len__(self):
        return self.total_jobs

    def __iter__(self):
        yield from self.jobs

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
            .scalar_subquery()
            .label("total_jobs")
        )
        return q

    @hybrid_property
    def total_jobs_done(self):
        return len(list(filter(lambda j: j.done, self.jobs)))

    @total_jobs_done.inplace.expression
    @classmethod
    def _total_jobs_done(cls):
        q = (
            cls.select(sa.func.count(JobRecord.id))
            .where(JobRecord.done, JobRecord.posting_id == cls.id)
            .correlate(cls)
            .scalar_subquery()
            .label("total_jobs_done")
        )
        return q

    @hybrid_property
    def percent_done(self) -> float:
        n_jobs = self.total_jobs
        n_stopped = 0

        if n_jobs == 0:
            return float("nan")

        for job in self.jobs:
            if job.done:
                n_stopped += 1

        return (n_stopped / n_jobs) * 100.0

    @percent_done.inplace.expression
    @classmethod
    def _percent_done(cls):
        total = sa.case(
            (cls.total_jobs == 0, float("nan")), else_=cls.total_jobs
        )
        return (
            sa.cast(cls.total_jobs_done, sa.Float) / sa.cast(total, sa.Float)
        ) * 100.0


class JobRecord(CreatedOnMixin, Base):

    __tablename__ = "job_records"

    posting_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.ForeignKey(JobPosting.id, ondelete="CASCADE")
    )
    posting: orm.Mapped[JobPosting] = orm.relationship(
        JobPosting, back_populates="jobs"
    )

    priority: orm.Mapped[int]
    positional_arguments: orm.Mapped[tuple] = orm.mapped_column(sa.JSON)
    keyword_arguments: orm.Mapped[Optional[dict]] = orm.mapped_column(sa.JSON)
    tries_remaining: orm.Mapped[int]

    status: orm.Mapped[JobStatus] = orm.mapped_column(
        default=JobStatus.waiting, index=True
    )
    exited_ok: orm.Mapped[Optional[bool]]
    work_started_on: orm.Mapped[Optional[datetime]]
    completed_on: orm.Mapped[Optional[datetime]]

    __table_args__ = (
        sa.CheckConstraint("tries_remaining >= 0", name="no_negative_tries"),
        sa.CheckConstraint(
            "completed_on IS NULL OR created_on < completed_on",
            name="no_unphysical_completed",
        ),
        sa.CheckConstraint(
            "(completed_on IS NULL and work_started_on IS NULL) or "
            "(work_started_on < completed_on)"
        ),
    )

    def __repr__(self):
        return f"<Job {self.id} f({self.pos_args}): {self.status}>"

    @hybrid_property
    def done(self):
        return self.status in (JobStatus.done, JobStatus.errored_out)

    @done.inplace.expression
    @classmethod
    def _done_expr(cls):
        return sa.or_(
            cls.status == JobStatus.done, cls.status == JobStatus.errored_out
        )

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
        return self.work_started_on - self.created_on

    @waiting_time.inplace.expression
    @classmethod
    def _waiting_time(cls):
        return (
            sa.func.coalesce(cls.work_started_on, sa.func.now())
            - cls.created_on
        )

    @hybrid_property
    def time_elapsed(self):
        if self.work_started_on is None:
            return None

        if self.completed_on is None:
            return datetime.now() - self.work_started_on

        return self.completed_on - self.work_started_on

    @time_elapsed.inplace.expression
    @classmethod
    def _time_elapsed(cls):
        return sa.func.coalesce(
            cls.completed_on, sa.func.now()
        ) - sa.func.coalesce(cls.work_started_on, sa.func.now())

    @classmethod
    def get_available(cls, batchsize: int) -> Select:
        q = (
            sa.select(cls)
            .join(cls.posting)
            .where(
                JobPosting.poster == getpass.getuser(),
                JobPosting.status == PostingStatus.executing,
                JobRecord.status == JobStatus.waiting,
                JobRecord.tries_remaining > 0,
            )
            .order_by(cls.priority.desc(), cls.created_on.asc())
            .limit(batchsize)
            .with_for_update(skip_locked=True, of=cls)
        )
        return q

    @classmethod
    def available_ids(
        cls, batchsize: int, posting_ids: Optional[Iterable[int]] = None
    ) -> sa.Select[tuple[uuid.UUID]]:
        q = (
            sa.select(cls.id)
            .join(cls.posting)
            .where(
                JobRecord.status == JobStatus.waiting,
                JobRecord.tries_remaining > 0,
            )
            .order_by(cls.priority.desc(), cls.created_on.asc())
            .limit(batchsize)
            .with_for_update(skip_locked=True, of=cls)
        )

        if posting_ids:
            q = q.where(JobPosting.id.in_(posting_ids))
        else:
            q = q.where(
                JobPosting.status == PostingStatus.executing,
            )
        return q

    def execute(self) -> typing.Any:
        function = load_function(self.posting.entry_point)
        kwargs = self.keyword_arguments if self.keyword_arguments else {}
        self.work_started_on = datetime.now()

        result = function(*self.positional_arguments, **kwargs)

        self.completed_on = datetime.now()
        self.status = JobStatus.done
        self.exited_ok = True
        return result

    def mark_as_bad(self):
        self.exited_ok = False
        self.status = JobStatus.errored_out
        self.tries_remaining = 0
        self.completed_on = datetime.now()


class JobPostingVariable(CreatedOnMixin, Base):
    """
    Shared variable storage for JobPosting instances.
    Provides key-value storage with row-level locking for concurrent access.
    """

    __tablename__ = "job_posting_variables"

    posting_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.ForeignKey(JobPosting.id, ondelete="CASCADE")
    )
    posting: orm.Mapped[JobPosting] = orm.relationship(
        JobPosting, back_populates="variables", cascade="all, delete"
    )
    key: orm.Mapped[str]
    value: orm.Mapped[dict] = orm.mapped_column(sa.JSON)
    updated_on: orm.Mapped[datetime] = orm.mapped_column(
        server_default=sa.text("now()"), onupdate=sa.text("now()")
    )

    __table_args__ = (
        sa.UniqueConstraint("posting_id", "key", name="unique_posting_key"),
    )

    def __repr__(self):
        return f"<JobPostingVariable {self.posting_id}:{self.key}>"


class JobProfile(CreatedOnMixin, Base):
    __tablename__ = "job_profiles"

    job_record_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.ForeignKey(JobRecord.id, ondelete="CASCADE")
    )
    job_record: orm.Mapped[JobRecord] = orm.relationship(
        JobRecord, backref="profile", cascade="all, delete"
    )

    total_calls: orm.Mapped[int] = orm.mapped_column(sa.Integer)
    total_time: orm.Mapped[float] = orm.mapped_column(sa.Float)

    function_stats = orm.relationship("FunctionStat", back_populates="profile")


# Define classes to track cProfile objects in such a manner that statistics
# can be computed at any time in the future.
class Function(CreatedOnMixin, Base):
    __tablename__ = "function_defs"

    filename: orm.Mapped[str]
    line_number: orm.Mapped[int]
    function_name: orm.Mapped[str]


class FunctionStat(CreatedOnMixin, Base):
    __tablename__ = "function_stats"

    profile_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.ForeignKey(JobProfile.id, ondelete="CASCADE")
    )
    function_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.ForeignKey(Function.id, ondelete="CASCADE")
    )

    n_calls: orm.Mapped[int]
    primitive_calls: orm.Mapped[int]
    total_time: orm.Mapped[float]
    cumulative_time: orm.Mapped[float]

    profile: orm.Mapped[JobProfile] = orm.relationship(
        JobProfile, back_populates="function_stats"
    )
    callers: orm.Mapped[list["FunctionStat"]] = orm.relationship(
        "FunctionStat",
        secondary="function_call_map",
        primaryjoin=lambda: FunctionStat.id == FunctionCallMap.callee_id,
        secondaryjoin=lambda: FunctionStat.id == FunctionCallMap.caller_id,
        back_populates="callees",
    )
    callees: orm.Mapped[list["FunctionStat"]] = orm.relationship(
        "FunctionStat",
        secondary="function_call_map",
        primaryjoin=lambda: FunctionStat.id == FunctionCallMap.caller_id,
        secondaryjoin=lambda: FunctionStat.id == FunctionCallMap.callee_id,
        back_populates="callers",
    )


class FunctionCallMap(Base):
    __tablename__ = "function_call_map"

    caller_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.ForeignKey(Function.id, ondelete="CASCADE")
    )
    callee_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.ForeignKey(Function.id, ondelete="CASCADE")
    )

    n_calls: orm.Mapped[int]


# Define some convenience functions to push cProfile to the database.
def reflect_cProfile(db: orm.Session, job_profile: JobProfile, stats):
    stat_cache: dict[tuple[str, int, str], FunctionStat] = {}
    func_q = sa.select(Function)

    for label, stat in stats.items():
        filename, line_number, function_name = label
        q = func_q.where(
            Function.filename == filename,
            Function.line_number == line_number,
            Function.function_name == function_name,
        )
        obj: Optional[Function] = db.scalar(q)

        if obj is None:
            obj = Function(
                filename=filename,
                line_number=line_number,
                function_name=function_name,
            )
            db.add(obj)
            db.flush()

        # Obj is now a Function instance with a provided primary key
        stat_inst = FunctionStat(
            profile_id=job_profile.id,
            function_id=obj.id,
            n_calls=stat[0],
            primitive_calls=stat[1],
            total_time=stat[2],
            cumulative_time=stat[3],
        )
        stat_cache[label] = stat_inst

    # Two phase push, one to flush statistics objects in bulk, returning pks
    # the second phase being to establish the call relationships
    for stat_inst in stat_cache.values():
        db.add(stat_inst)
    db.flush()

    relationship_payload: list[FunctionCallMap] = []
    for label, stat in stats.items():
        # Build relationship graph
        call_tree = stat[-1]
        callee = stat_cache[label]
        for caller_label, t2_stat in call_tree.items():
            caller = stat_cache[caller_label]
            n_calls = t2_stat[0]

            map_ = FunctionCallMap(
                caller_id=caller.function_id,
                callee_id=callee.function_id,
                n_calls=n_calls,
            )
            relationship_payload.append(map_)

    db.bulk_save_objects(relationship_payload)

    # Leave it to the developer to commit
    return stat_cache, relationship_payload
