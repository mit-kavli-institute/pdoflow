"""
This module defines the runtime logic for pdoflow worker pools and how
jobs are managed.
"""
import contextlib
import cProfile
import multiprocessing as mp
import os
import random
import signal
import warnings
from datetime import datetime
from time import sleep, time
from typing import Callable, Generator, Optional, Tuple
from uuid import UUID

import sqlalchemy as sa
from loguru import logger
from sqlalchemy import orm
from sqlalchemy.exc import OperationalError

from pdoflow.io import Session
from pdoflow.models import JobPosting, JobProfile, JobRecord, reflect_cProfile
from pdoflow.registry import JobRegistry, Registry
from pdoflow.status import JobStatus, PostingStatus
from pdoflow.utils import make_warning_logger


def timeout(signum, frame):
    raise TimeoutError("Timeout occured")


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


def poll_posting(
    posting_id: UUID,
) -> Generator[Tuple[datetime, int, int, PostingStatus], None, None]:
    """
    Monitor a job posting's execution progress yielding periodic
    status updates.

    This generator continually queries the database for the current status of a
    job posting identified by its UUID. It yields tuples containing timestamp
    and progress information until the posting completes execution or is no
    longer found in the database.

    If at any time the number of completed jobs is equal or somehow greater
    than the total jobs listed in a posting, the posting is set to the finish
    state and this function will return.

    Parameters
    ----------
    posting_id : UUID
        The unique identifier of the job posting to monitor.

    Yields
    ------
    tuple of (datetime, int, int, PostingStatus)
        A tuple containing:
        - query_time: The timestamp when the status was queried
        - total_jobs: Total number of jobs in the posting
        - jobs_completed: Number of jobs completed so far
        - status: Current status of the posting

    Notes
    -----
    The generator exits when:
    1. The posting status changes from 'executing' to any other status
    2. The posting is not found in the database (returns early)
    3. The number of finished jobs is equal to the total amount of work

    The function performs continuous database queries, so it should be used
    with appropriate delays between iterations to avoid excessive load.

    Examples
    --------
    >>> posting_id = UUID('12345678-1234-5678-1234-567812345678')
    >>> for timestamp, total, done, status in poll_posting(posting_id):
    ...     print(f"Progress: {done}/{total} jobs completed")
    ...     time.sleep(1)  # Add delay between iterations
    """
    # Define the query once to avoid repetition
    status_query = sa.select(
        sa.func.now().label("query_time"),
        JobPosting.total_jobs,
        JobPosting.total_jobs_done,
        JobPosting.status,
    ).where(JobPosting.id == posting_id)

    with Session() as session:
        # Helper function to fetch and unpack posting data
        def fetch_posting_status():
            result = session.execute(status_query).first()
            if result is None:
                return None
            return (
                result.query_time,
                result.total_jobs,
                result.total_jobs_done,
                result.status,
            )

        # Initial fetch
        posting_data = fetch_posting_status()
        if posting_data is None:
            return

        query_time, total_jobs, jobs_completed, status = posting_data

        # Continue polling while posting is executing
        while status == PostingStatus.executing:
            yield query_time, total_jobs, jobs_completed, status

            # Fetch updated status
            posting_data = fetch_posting_status()
            if posting_data is None:
                return

            if jobs_completed >= total_jobs:
                session.execute(
                    sa.update(JobPosting)
                    .where(JobPosting.id == posting_id)
                    .values(status=PostingStatus.finished)
                )
                session.commit()
                return

            query_time, total_jobs, jobs_completed, status = posting_data


def poll_job_status_count(posting_id: UUID, status: JobStatus):
    """
    Generate a continuous stream of job counts for a specific status.

    This generator continuously queries the database to count jobs with a
    specific status for a given posting. It yields the count indefinitely,
    allowing consumers to monitor changes in job status distribution over time.

    Parameters
    ----------
    posting_id : UUID
        The unique identifier of the job posting to monitor.
    status : JobStatus
        The job status to count (e.g., JobStatus.waiting, JobStatus.done).

    Yields
    ------
    int
        The current count of jobs with the specified status. Returns 0 if
        no jobs match or if the count is None.

    Notes
    -----
    This is an infinite generator that will continue yielding values until
    the consumer stops iteration. Each yield performs a database query, so
    consumers should implement appropriate delays between iterations to
    avoid excessive database load.

    The generator does not check if the posting exists - it will simply
    yield 0 if no jobs are found.

    Examples
    --------
    >>> # Monitor waiting jobs
    >>> posting_id = UUID('12345678-1234-5678-1234-567812345678')
    >>> for count in poll_job_status_count(posting_id, JobStatus.waiting):
    ...     print(f"Waiting jobs: {count}")
    ...     if count == 0:
    ...         break
    ...     time.sleep(1)

    >>> # Use with itertools for limited iterations
    >>> import itertools
    >>> for count in itertools.islice(
    ...     poll_job_status_count(posting_id, JobStatus.executing), 5
    ... ):
    ...     print(f"Executing: {count}")
    """
    count_query = sa.select(sa.func.count(JobRecord.id)).where(
        JobRecord.posting_id == posting_id, JobRecord.status == status
    )

    def fetch_status() -> int:
        with Session() as session:
            count = session.scalar(count_query)
            if not count:
                return 0
            return count

    n_records = fetch_status()
    while True:
        yield n_records
        n_records = fetch_status()


def poll_posting_percent(posting_id: UUID) -> Generator[float, None, None]:
    """
    Generate a continuous stream of completion percentages for a job posting.

    This generator continuously queries the database to retrieve the completion
    percentage of a job posting. It yields float values representing the
    percentage of jobs completed (0.0 to 100.0), making it ideal for feeding
    into progress bar implementations for visual display.

    Parameters
    ----------
    posting_id : UUID
        The unique identifier of the job posting to monitor.

    Yields
    ------
    float
        The current completion percentage (0.0 to 100.0). Returns NaN
        (float('nan')) if the posting has no jobs (total_jobs == 0).
        Returns 0.0 if the posting is not found.

    Notes
    -----
    This is an infinite generator that will continue yielding values until
    the consumer stops iteration. Each yield performs a database query, so
    consumers should implement appropriate delays between iterations to
    avoid excessive database load.

    The percentage is calculated as: (total_jobs_done / total_jobs) * 100.0

    Special cases:
    - If posting doesn't exist: yields 0.0
    - If total_jobs is 0: yields NaN
    - If all jobs complete: continues yielding 100.0

    Examples
    --------
    >>> # Basic progress monitoring
    >>> posting_id = UUID('12345678-1234-5678-1234-567812345678')
    >>> for percent in poll_posting_percent(posting_id):
    ...     print(f"Progress: {percent:.1f}%")
    ...     if percent >= 100.0:
    ...         break
    ...     time.sleep(0.5)

    >>> # Integration with tqdm progress bar
    >>> from tqdm import tqdm
    >>> pbar = tqdm(total=100, desc="Processing")
    >>> last_percent = 0.0
    >>> for percent in poll_posting_percent(posting_id):
    ...     pbar.update(percent - last_percent)
    ...     last_percent = percent
    ...     if percent >= 100.0:
    ...         break
    ...     time.sleep(0.5)
    >>> pbar.close()

    >>> # Use with itertools for limited iterations
    >>> import itertools
    >>> for percent in itertools.islice(poll_posting_percent(posting_id), 10):
    ...     print(f"Completion: {percent:.2f}%")
    """
    percent_query = sa.select(JobPosting.percent_done).where(
        JobPosting.id == posting_id
    )

    def fetch_percent() -> float:
        with Session() as session:
            percent = session.scalar(percent_query)
            if percent is None:
                return 0.0
            return percent

    percent = fetch_percent()
    while True:
        yield percent
        percent = fetch_percent()


def await_posting_completion(
    posting_id: UUID,
    poll_time: float = 0.5,
    max_wait: Optional[int] = None,
):
    """
    Wait for the posting to finish execution or until an optional
    maximum wait time. This function will block until it returns.

    Parameters
    ----------
    posting_id: UUID
        The UUID V4 unique identifier for the JobPosting that
        this function will wait for.
    poll_time: float
        We must poll the database to check for the JobPosting's
        status. To avoid spamming the database with requests and
        CPU overhead, this function will sleep for this many seconds
        before attempting to talk to the database again.

        This is not be considered an accurate time between pollings
        and very small polling time ~10ms are not to be considered
        reliable unless utilizing specific hardware and real-time
        kernel packages are used.
    max_wait: Optional[int]
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

    Examples
    --------
    >>> posting_id = UUID('12345678-1234-5678-1234-567812345678')
    >>> await_posting_completion(posting_id, poll_time=1.0, max_wait=60)
    """
    if max_wait:
        signal.signal(signal.SIGALRM, timeout)
        signal.alarm(max_wait)
    for _, *_ in poll_posting(posting_id):
        sleep(poll_time)

    # Exited, disable alarm if it was set
    if max_wait:
        signal.alarm(0)


def await_for_status_threshold(
    posting_id: UUID,
    status: JobStatus,
    poll_time: float = 0.5,
    max_wait: Optional[int] = None,
    threshold_func: Callable[[int], bool] = lambda c: c <= 0,
):
    """
    Wait until the count of jobs with a specific status meets a threshold.

    This function blocks execution until the number of jobs with the specified
    status satisfies the threshold condition defined by threshold_func. It
    continuously polls the database at regular intervals to check the current
    count.

    Parameters
    ----------
    posting_id : UUID
        The unique identifier of the job posting to monitor.
    status : JobStatus
        The job status to monitor (e.g., JobStatus.executing,
        JobStatus.errored_out).
    poll_time : float, optional
        Time in seconds to sleep between database polls.
        Default is 0.5 seconds.
        Lower values provide more responsive monitoring but
        increase database load.
    max_wait : Optional[int], optional
        Maximum time in seconds to wait before raising TimeoutError. If None
        (default), the function will wait indefinitely until
        the threshold is met.
    threshold_func : Callable[[int], bool], optional
        A function that takes the current count as input and returns True when
        the threshold condition is met. Default is `lambda c: c <= 0`, which
        waits until no jobs have the specified status.

    Raises
    ------
    TimeoutError
        Raised if max_wait is specified and the threshold is not met within
        the specified time limit.

    Notes
    -----
    The function uses signal.SIGALRM for timeout implementation, which may not
    work on all platforms (particularly Windows). The timeout is approximate
    and may exceed max_wait by up to poll_time seconds.

    Common threshold functions:
    - Wait for zero: `lambda c: c <= 0` (default)
    - Wait for all done: `lambda c: c == total_jobs`
    - Wait for threshold: `lambda c: c >= min_required`
    - Wait for percentage: `lambda c: c / total >= 0.95`

    Examples
    --------
    >>> # Wait until no jobs are executing
    >>> posting_id = UUID('12345678-1234-5678-1234-567812345678')
    >>> await_for_status_threshold(
    ...     posting_id,
    ...     JobStatus.executing,
    ...     poll_time=1.0
    ... )

    >>> # Wait for at least 10 jobs to complete with timeout
    >>> await_for_status_threshold(
    ...     posting_id,
    ...     JobStatus.done,
    ...     threshold_func=lambda count: count >= 10,
    ...     max_wait=60
    ... )

    >>> # Wait until error count drops below 5
    >>> await_for_status_threshold(
    ...     posting_id,
    ...     JobStatus.errored_out,
    ...     threshold_func=lambda count: count < 5,
    ...     poll_time=2.0
    ... )
    """
    if max_wait:
        signal.signal(signal.SIGALRM, timeout)
        signal.alarm(max_wait)

    for count in poll_job_status_count(posting_id, status):
        if threshold_func(count):
            break
        sleep(poll_time)

    # Exited, disable alarm if it was set
    if max_wait:
        signal.alarm(0)


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
                job.posting.status = PostingStatus.errored_out
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
        # Retry logic for database connections
        max_retries = 3
        retry_delay = 1.0

        for attempt in range(max_retries):
            try:
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
                break
            except OperationalError as e:
                if attempt < max_retries - 1:
                    logger.warning(
                        f"Database connection failed "
                        f"(attempt {attempt + 1}/{max_retries}): {e}"
                    )
                    sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    raise

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
        self,
        posting_id: UUID,
        poll_time: float = 0.5,
        max_wait: Optional[int] = None,
    ):
        """
        Wait for the posting to finish execution or until an optional
        maximum wait time. This method will block until it returns.

        This method delegates to the module-level `await_posting_completion`
        function. See that function for full documentation.

        Parameters
        ----------
        posting_id: UUID
            The UUID V4 unique identifier for the JobPosting that
            this method will wait for.
        poll_time: float
            Time in seconds between database polls.
        max_wait: Optional[int]
            Maximum time in seconds to wait before raising TimeoutError.

        Raises
        ------
        TimeoutError:
            If max_wait is exceeded.
        ValueError:
            If the posting_id is not found.
        """
        await_posting_completion(posting_id, poll_time, max_wait)
