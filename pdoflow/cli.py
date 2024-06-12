import fileinput
import os
from time import sleep
from uuid import UUID

import click
import tabulate
from loguru import logger

from pdoflow.cluster import ClusterPool
from pdoflow.io import Session
from pdoflow.models import JobPosting


@click.group(name="pdoflow")
def pdoflow_main():
    pass


@pdoflow_main.command()
@click.option("--max-workers", default=os.cpu_count(), type=int)
@click.option("--upkeep-rate", default=0.5, type=float)
@click.option("--exception-logging", default="warning")
def pool(max_workers: int, upkeep_rate: float, exception_logging: str):
    upkeep_time = 1 / upkeep_rate
    worker_pool = ClusterPool(
        max_workers=max_workers, exception_logging=exception_logging
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
def posting_status(uuid, table_format):
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
