"""
This module implements the main entrypoint for PDOFlow.
"""
import contextlib
import multiprocessing as mp
from collections import defaultdict
from time import sleep, time
from typing import Optional
from uuid import UUID

import sqlalchemy as sa
from loguru import logger

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
        self.failure_threshold = 10
        self._failure_cache: dict[UUID, int] = defaultdict(
            lambda: self.failure_threshold
        )
        self._bad_postings: set[UUID] = set()

    def process_job_records(self, jobs: list[JobRecord]):
        for job in jobs:
            if job.posting_id in self._bad_postings:
                # Short circuit out, too many failures for the given job
                # posting
                job.mark_as_bad()
                continue
            try:
                job.execute()
            except KeyboardInterrupt:
                logger.warning("Encountered user interrupt, releasing jobs")
                self._session.rollback()
                raise
            except Exception as e:
                logger.warning(f"Worker encountered {e}")

                remaining_failures = self._failure_cache[job.posting_id]

                if remaining_failures <= 0:
                    logger.warning(
                        f"Worker deemed {job.posting} as"
                        " too erroneous to continue it's work."
                    )
                    self._bad_postings.add(job.posting_id)
                    job.mark_as_bad()
                    continue

                if job.tries_remaining <= 1:
                    logger.warning(
                        f"Worker is deeming {job} too erroneous to "
                        " try it again."
                    )
                    job.mark_as_bad()
                    self._failure_cache[job.posting_id] -= 1
                else:
                    job.tries_remaining -= 1
                    logger.warning(
                        f"{job} encountered {e}, "
                        f"{job.tries_remaining} tries remaining"
                    )

        self._session.commit()

    def obtain_jobs(self, max_batchsize: int) -> list[JobRecord]:
        q = JobRecord.get_available(max_batchsize)

        # ignore postings which the worker has deemed "bad"
        if len(self._bad_postings) > 0:
            q = q.where(~JobRecord.posting_id.in_(self._bad_postings))
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

    def await_posting_completion(
        self, posting_id: UUID, poll_time=0.5, max_wait=None
    ):

        executing = True

        with Session() as db:
            q = sa.select(JobPosting.percent_done).where(
                JobPosting.id == posting_id
            )

            t0 = time()

            while executing:
                amount_finished = db.scalar(q)

                if amount_finished is None:
                    raise ValueError(f"No post found for {posting_id}")

                executing = amount_finished < 100.0

                if executing:
                    sleep(poll_time)

                if max_wait and (time() - t0) > max_wait:
                    raise TimeoutError
