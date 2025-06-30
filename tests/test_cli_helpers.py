"""Helper functions and fixtures for CLI tests."""

import uuid
from typing import List, Tuple

from pdoflow import cluster, models, registry, status
from pdoflow.io import Session


def create_test_posting(
    num_jobs: int = 1,
    priorities: List[int] = None,
    posting_status: status.PostingStatus = status.PostingStatus.executing,
    job_status: status.JobStatus = status.JobStatus.waiting,
) -> Tuple[uuid.UUID, List[uuid.UUID]]:
    """Create a test posting with specified number of jobs."""
    registry.Registry.clear_registry()

    # Simple test function
    def test_func(x):
        return x * 2

    cluster.job()(test_func)

    # Create work items
    work_items = [(i,) for i in range(num_jobs)]

    # Use provided priorities or default to 0
    if priorities is None:
        priorities = 0

    # Post work
    posting_id, job_ids = registry.Registry[test_func].post_work(
        work_items, [], priority=priorities
    )

    # Update posting status if needed
    if posting_status != status.PostingStatus.executing:
        with Session() as db:
            posting = db.scalar(
                models.JobPosting.select().where(
                    models.JobPosting.id == posting_id
                )
            )
            posting.status = posting_status
            db.commit()

    # Update job statuses if needed
    if job_status != status.JobStatus.waiting:
        with Session() as db:
            jobs = list(
                db.scalars(
                    models.JobRecord.select().where(
                        models.JobRecord.id.in_(job_ids)
                    )
                )
            )
            for job in jobs:
                job.status = job_status
            db.commit()

    return posting_id, job_ids


def create_mixed_priority_posting() -> uuid.UUID:
    """Create a posting with mixed priority jobs for testing priority_stats."""
    registry.Registry.clear_registry()
    cluster.job()(lambda x: x)

    # Create jobs with various priorities
    posting_id, _ = registry.Registry[lambda x: x].post_work(
        [(i,) for i in range(10)],
        [],
        priority=[100, 100, 100, 50, 50, 0, 0, -25, -50, -50],
    )

    return posting_id
