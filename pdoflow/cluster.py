"""
This module implements the main entrypoint for PDOFlow.
"""
import contextlib
import multiprocessing as mp
from time import sleep
from typing import Optional
from uuid import UUID

import sqlalchemy as sa

from pdoflow.io import Session
from pdoflow.models import JobPosting, JobRecord
from pdoflow.registry import JobRegistry, Registry


def job(name: Optional[str] = None, registry: JobRegistry = Registry):
    def __internal(func):
        registry.add_job(func, name)
        return func

    return __internal


class ClusterProcess(mp.Process):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._session = Session()

    def process_job_records(self, jobs: list[JobRecord]):
        for job in jobs:
            job.execute()
            # Successfully executed
            self._session.commit()

    def obtain_jobs(self, max_batchsize: int) -> list[JobRecord]:
        q = JobRecord.get_available(max_batchsize)
        jobs = self._session.scalars(q)
        return list(jobs)

    def run(self):
        with self._session:
            while True:
                jobs = self.obtain_jobs(1)
                self.process_job_records(jobs)


class ClusterPool(contextlib.AbstractContextManager):
    """
    Main entrypoint for executing jobs. This Pool manages instantiation,
    execution, and cleanup of multiprocess workers. Dependencies are
    loaded and cached within the context of this pool.
    """

    def __init__(
        self,
        max_workers: int = 1,
        worker_class: type[ClusterProcess] = ClusterProcess,
    ):
        """
        Create a new pool with an upper limit of how many workers
        may be spawned.
        """
        self.max_workers = max_workers
        self.workers: list[mp.Process] = []
        self.WorkerClass = worker_class

    def __enter__(self):
        for _ in range(self.max_workers):
            self.workers.append(self.WorkerClass(daemon=True))
            self.workers[-1].start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        for worker in self.workers:
            worker.terminate()

    def upkeep(self):
        """
        This method should be called within an active pool. This causes
        the pool to remove dead workers and 'resurrect' now empty
        spots in the worker pool.
        """
        dead_idx = []
        for i, worker in enumerate(self.workers):
            if not worker.is_alive:
                dead_idx.append(i)
                worker.close()

        for idx in dead_idx:
            self.workers[idx] = self.WorkerClass(daemon=True)
            self.workers[idx].start()

    def await_posting_completion(self, posting_id: UUID, poll_time=0.5):

        executing = True

        with Session() as db:
            q = sa.select(JobPosting.percent_done).where(
                JobPosting.id == posting_id
            )

            while executing:
                amount_finished = db.scalar(q)

                if amount_finished is None:
                    raise ValueError(f"No post found for {posting_id}")

                executing = amount_finished < 100.0

                if executing:
                    sleep(poll_time)
        return
