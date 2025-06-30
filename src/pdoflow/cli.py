import enum
import fileinput
import os
import typing
from time import sleep
from uuid import UUID

import click
import sqlalchemy as sa
import tabulate
from loguru import logger

from pdoflow.cluster import ClusterPool
from pdoflow.io import Session
from pdoflow.models import JobPosting, JobRecord
from pdoflow.status import JobStatus, PostingStatus


# define custom click types
class EnumChoice(click.ParamType):
    """Generic click type that maps human-readable strings → Enum members."""

    name = "enum"

    def __init__(self, enum_cls: type[enum.Enum]) -> None:
        self.enum_cls = enum_cls
        self._lookup: dict[str, enum.Enum] = {
            e.name.lower(): e for e in enum_cls
        } | {e.name.replace("_", "-"): e for e in enum_cls}

    def convert(
        self,
        value: str,
        param: typing.Optional[click.Parameter],
        ctx: typing.Optional[click.Context],
    ) -> enum.Enum:
        key = value.lower()
        if key in self._lookup:
            return self._lookup[key]
        self.fail(
            f"'{value}' is not a valid {self.enum_cls.__name__}. "
            f"Choose from: {', '.join(self._lookup)}",
            param,
            ctx,
        )

    def get_metavar(self, param: click.Parameter) -> str:
        return "[" + "|".join(self._lookup) + "]"


@click.group(name="pdoflow")
def pdoflow_main():
    pass


@pdoflow_main.command()
@click.option("--max-workers", default=os.cpu_count(), type=int)
@click.option("--upkeep-rate", default=0.5, type=float)
@click.option("--exception-logging", default="warning")
@click.option("--warning-logging", default="debug")
@click.option("--batchsize", type=int, default=10)
def pool(
    max_workers: int,
    upkeep_rate: float,
    exception_logging: str,
    warning_logging: str,
    batchsize: int = 10,
):
    upkeep_time = 1 / upkeep_rate
    worker_pool = ClusterPool(
        max_workers=max_workers,
        exception_logging=exception_logging,
        warning_logging=warning_logging,
        batchsize=batchsize,
    )

    logger.debug(
        f"Instantiated {worker_pool} with upkeep rate of {upkeep_time:03f}s"
    )
    with worker_pool:
        while worker_pool:
            worker_pool.upkeep()
            sleep(upkeep_time)


@pdoflow_main.command()
@click.argument("uuid", type=str)
@click.option("--table-format", type=str, default="simple")
@click.option("--show-jobs", is_flag=True, help="Show individual job details")
def posting_status(uuid, table_format, show_jobs):
    try:
        ids = [UUID(uuid)]
    except ValueError:
        # IDs should be read from stdin
        ids = [UUID(line) for line in fileinput.input(uuid)]
    fields = ["id", "created_on", "status", "percent_done"]
    with Session() as db:
        q = JobPosting.select(*fields).where(JobPosting.id.in_(ids))
        results = db.execute(q)
        table = tabulate.tabulate(results, fields, tablefmt=table_format)
    click.echo(table)

    if show_jobs:
        # Show job details including priority
        click.echo("\nJob Details:")
        job_fields = ["id", "priority", "status", "created_on"]
        for posting_id in ids:
            q = (
                JobRecord.select(*job_fields)
                .where(JobRecord.posting_id == posting_id)
                .order_by(
                    JobRecord.priority.desc(), JobRecord.created_on.asc()
                )  # noqa: E501
            )
            results = list(db.execute(q))
            if len(results) > 0:
                click.echo(f"\nPosting {posting_id}:")
                job_table = tabulate.tabulate(
                    results, job_fields, tablefmt=table_format
                )
                click.echo(job_table)


@pdoflow_main.command()
@click.option("--table-format", type=str, default="simple")
def list_postings(table_format: str):
    fields = ["id", "created_on", "status", "percent_done"]
    with Session() as db:
        q = JobPosting.select(*fields)
        results = db.execute(q)
        table = tabulate.tabulate(results, fields, tablefmt=table_format)
    click.echo(table)


@pdoflow_main.command()
@click.argument("uuid", type=str)
@click.argument("status", type=EnumChoice(PostingStatus))
def set_posting_status(uuid: str, status: PostingStatus):
    with Session() as db:
        q = JobPosting.select().where(JobPosting.id == uuid)
        obj = db.scalar(q)
        if obj is None:
            click.echo(f"Could not find Posting with id: {id}", err=True)
            raise click.Abort()
        obj.status = status
        db.commit()


@pdoflow_main.command()
@click.option("--table-format", type=str, default="simple")
def priority_stats(table_format: str):
    """Show priority statistics for waiting jobs"""
    with Session() as db:
        # Get priority distribution
        q = (
            sa.select(
                JobRecord.priority,
                sa.func.count(JobRecord.id).label("count"),
                sa.func.min(JobRecord.created_on).label("oldest"),
            )
            .where(
                JobRecord.status == JobStatus.waiting,
                JobRecord.tries_remaining > 0,
            )
            .group_by(JobRecord.priority)
            .order_by(JobRecord.priority.desc())
        )
        results = list(db.execute(q))

        if len(results) == 0:
            click.echo("No waiting jobs found.")
            return

        click.echo("Priority Distribution for Waiting Jobs:")
        fields = ["priority", "count", "oldest"]
        table = tabulate.tabulate(results, fields, tablefmt=table_format)
        click.echo(table)


@pdoflow_main.command()
@click.argument("uuid", type=str)
def execute_job(uuid: str):
    with Session() as db:
        q = (
            sa.update(JobRecord)
            .values(status=JobStatus.executing)
            .where(JobRecord.id == uuid)
        )
        db.execute(q)
        db.commit()

        job = db.scalar(sa.select(JobRecord).where(JobRecord.id == uuid))
        if job is None:
            click.echo(
                click.style(
                    f"No Job record {uuid} not found",
                    fg="red",
                )
            )
            return
        try:
            job.execute()
            click.echo(
                click.style(
                    f"Job record {uuid} successfully executed",
                    fg="green",
                )
            )
        except KeyboardInterrupt:
            db.rollback()
            click.echo("Keyboard interrupt, exiting")
        except Exception as e:
            click.echo(
                click.style(
                    f"Job record {uuid} encountered an error: {e}",
                    fg="white",
                    bg="red",
                    blink=True,
                )
            )
            job.mark_as_bad()
        finally:
            db.commit()
