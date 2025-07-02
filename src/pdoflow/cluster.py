"""
This module defines the runtime logic for pdoflow worker pools and how
jobs are managed.
"""
import contextlib
import cProfile
import multiprocessing as mp
import os
import random
import warnings
from time import sleep, time
from typing import Optional
from uuid import UUID

import sqlalchemy as sa
from loguru import logger
from sqlalchemy import orm
from sqlalchemy.exc import OperationalError

from pdoflow.io import Session
from pdoflow.models import JobPosting, JobProfile, JobRecord, reflect_cProfile
from pdoflow.registry import JobRegistry, Registry
from pdoflow.status import JobStatus
from pdoflow.utils import make_warning_logger


def job(name: Optional[str] = None, registry: JobRegistry = Registry):
    """
    Register a function as a entrypoint for cluster operations. By
    default the function's `__name__` is used as an identifier within
    a Registry. A provided `name` will override this behavior to whatever
    string a developer uses.

    The decorated function must be recallable by an import statement.
    Statements made within an interactive Python shell will not execute
    as they cannot be called from an independent Python instance.

    Paramters
    ---------
    name: Optional[str]
        Override the name that will be used to recall this function
        when posting work.
    registry: JobRegistry
        The reigstry to associate the job with. By default the global
        `Registry` is used.
    """

    def __internal(func):
        registry.add_job(func, name)
        return func

    return __internal


class _FailureCache:
    """ """

    def __init__(self, default_value: int):
        self._default_value = default_value
        self._cache: dict[UUID, int] = {}

    def __getitem__(self, key: UUID):
        try:
            return self._cache[key]
        except KeyError:
            self._cache[key] = self._default_value
            return self._cache[key]

    def __setitem__(self, key: UUID, value: int):
        self._cache[key] = value


class ClusterProcess(mp.Process):
    """
    A multiprocess Process with logic to pull work and dynamically
    import required code in order to execute it's work.

    These processes create and manage their own Database connections.
    And update JobRecords when either erroring out or completing it's
    workload. A consistently failing JobPosting will result in that
    posting's blacklisting and cancellation of future operations.

    Notes
    -----
    Each process maintains a consistent connection to it's database.
    This is required in order to maintain PostgreSQL's `SKIP LOCKED`
    feature in which an active transaction must be kept alive.
    """

    def __init__(
        self,
        *args,
        exception_logging: str = "warning",
        warning_logging: str = "debug",
        batchsize=10,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._session = None
        self.failure_threshold = 10
        self._failure_cache = _FailureCache(self.failure_threshold)
        self._bad_postings: set[UUID] = set()
        self.exception_logging = exception_logging
        self.warning_logging = warning_logging
        self.batchsize = batchsize

    def __repr__(self):
        return f"<ClusterProcess pid={os.getpid()} />"

    def _pre_run_init(self):
        warnings.showwarning = make_warning_logger(self.warning_logging)

    def _get_records(self, db: orm.Session) -> list[JobRecord]:
        q = JobRecord.get_available(self.batchsize)
        jobs = list(db.scalars(q))
        return jobs

    def nominal_execution(self, job: JobRecord):
        job.execute()

    def traced_execution(self, job: JobRecord):
        pr = cProfile.Profile()
        pr.enable()
        job.execute()
        pr.disable()
        pr.create_stats()
        return pr.stats

    def process_job(self, db: orm.Session, job: JobRecord):
        if job.posting_id in self._bad_postings:
            job.mark_as_bad()
            db.commit()
            return
        try:
            if random.random() < 0.1:  # 10% chance for traced execution
                stats = self.traced_execution(job)
                # Create JobProfile and reflect the cProfile stats
                job_profile = JobProfile(
                    job_record_id=job.id,
                    total_calls=len(stats),
                    total_time=sum(
                        stat[3] for stat in stats.values()
                    ),  # cumulative time
                )
                db.add(job_profile)
                db.flush()  # Get the profile ID
                reflect_cProfile(db, job_profile, stats)
            else:  # 90% chance for nominal execution
                self.nominal_execution(job)

            logger.success(
                f"Executed {job.id} took "
                f"{job.time_elapsed.total_seconds():.2f} seconds"
            )
        except KeyboardInterrupt:
            logger.warning("Encountered interrupt, releasing jobs")
            db.rollback()
            raise
        except OperationalError:
            logger.exception(
                f"Worker {self} encountered database error, backing off..."
            )
            sleep(2 * random.random())
            job.status = JobStatus.waiting
        except Exception as e:
            log_func = getattr(logger, self.exception_logging, logger.warning)
            log_func(f"Worker encountered {e}")

            remaining_failures = self._failure_cache[job.posting_id]

            if remaining_failures <= 0:
                logger.warning(
                    f"Worker deemed {job.posting} as"
                    " too erroneous to continue it's work."
                )
                self._bad_postings.add(job.posting_id)
                job.mark_as_bad()
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
                job.status = JobStatus.waiting
        finally:
            db.commit()

    def process_job_records(self) -> int:
        with Session() as db:
            t0 = time()
            ids = [j.id for j in self._get_records(db)]
            time_to_obtain = time() - t0
            logger.info(
                f"Worker {self} took {time_to_obtain:.2f} seconds "
                "to aquire workload"
            )
            q = (
                sa.update(JobRecord)
                .values(status=JobStatus.executing)
                .where(JobRecord.id.in_(ids))
            )
            db.execute(q)
            db.commit()

        if len(ids) == 0:
            logger.debug(f"Nothing {self}'s job queue, waiting...")
            return 0

        with Session() as db:
            job_q = sa.select(JobRecord).where(JobRecord.id.in_(ids))
            jobs = list(db.scalars(job_q))

            for job in jobs:
                self.process_job(db, job)

        return len(jobs)

    def run(self):
        self._pre_run_init()
        while True:
            n_processed = self.process_job_records()
            if n_processed == 0:
                sleep(5)


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
        exception_logging="warning",
        warning_logging="debug",
        batchsize: int = 10,
    ):
        """
        Create a new pool with an upper limit of how many workers
        may be spawned.
        """
        self.max_workers = max_workers
        self.workers: list[mp.Process] = []
        self.WorkerClass = worker_class
        self.exception_logging = exception_logging
        self.warning_logging = warning_logging
        self.batchsize = batchsize

    def __enter__(self):
        for _ in range(self.max_workers):
            self.workers.append(
                self.WorkerClass(
                    daemon=True,
                    exception_logging=self.exception_logging,
                    warning_logging=self.warning_logging,
                    batchsize=self.batchsize,
                )
            )
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
            if not worker.is_alive():
                dead_idx.append(i)
                worker.close()

        for idx in dead_idx:
            self.workers[idx] = self.WorkerClass(daemon=True)
            self.workers[idx].start()

    def await_posting_completion(
        self, posting_id: UUID, poll_time=0.5, max_wait=None
    ):
        """
        Wait for the posting to finish execution or until an optional
        maximum wait time. This method will block until it returns.

        Parameters
        ----------
        posting_id: UUID
            The UUID V4 unique identifier for the JobPosting that
            this method will wait for.
        poll_time: float
            We must poll the database to check for the JobPosting's
            status. To avoid spamming the database with requests and
            CPU overhead, this method will sleep for this many seconds
            before attempting to talk to the database again.

            This is not be considered an accurate time between pollings
            and very small polling time ~10ms are not to be considered
            reliable unless utilizing specific hardware and real-time
            kernel packages are used.
        max_wait: Optional[float]
            If the amount of time waiting for the execution of the
            JobPosting exceeds this amount then raise a TimeoutError.

            Note that the total time waited will exceed the specified
            maximum wait time due to poll time resolution.

        Raises
        ------
        TimeoutError:
            Raised if `max_wait` is not `None` and total wait time
            exceeds this value (in seconds).

        ValueError:
            Raised if the provided `posting_id` resulted in no
            JobPosting.
        """

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
