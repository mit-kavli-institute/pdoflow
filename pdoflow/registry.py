import deal
from itertools import zip_longest
from dataclasses import dataclass
from typing import Callable, Optional

from pdoflow.models import JobPosting, JobRecord
from pdoflow.io import Session
from pdoflow.utils import get_module_path
from pdoflow.status import JobStatus, PostingStatus


@dataclass
class Job:
    target: Callable

    @deal.post(
        lambda posting, jobs: all(job.posting_id == posting.id for job in jobs),
        message="All jobs must be related to the returned posting",
    )
    @deal.post(
        lambda posting, _: posting.entry_point.exists(),
        message="Posting must have an existing entrypoint"
    )
    def post_work(self, positional_args: list[tuple], keyword_args: list[dict]):
        posting = JobPosting(
            target_function=self.target,
            entry_point=get_module_path(self.target).resolve(),
            status=PostingStatus.executing
        )

        with Session() as db:
            db.add(posting)
            db.flush()

            payload = [
                JobRecord(
                    posting=posting,
                    priority=1,
                    positional_args=args,
                    keyword_arguments=kwargs,
                    tries_remaining=3,
                    status=JobStatus.waiting
                )
                for args, kwargs in zip_longest(positional_args, keyword_args)
            ]
            db.add_all(payload)
            db.commit()
        return posting, payload


@dataclass
class JobRegistry:
    _job_defs: dict[str, Job] = {}

    def __getitem__(self, key: str | Callable) -> Job:
        lookup_name = self.resolve_name(key)
        return self._job_defs[lookup_name]

    def __contains__(self, key: str | Callable) -> bool:
        lookup_name = self.resolve_name(key)
        return lookup_name in self._job_defs

    @deal.ensure(
        lambda args, _: args.__name__ in args.self or args.name_override in args.self,
        message="Decorated jobs must always be in some way, registered."
    )
    def add_job(self, func: Callable, name_override: Optional[str] = None):
        name = name_override if name_override else func.__name__

        if name in self:
            raise ValueError(
                f"Job name {name} already defined in registery!"
            )

        self._job_defs[name] = Job(target=func)
        return func

    @staticmethod
    def resolve_name(key: str | Callable) -> str:
        return key if isinstance(key, str) else key.__name__


Registry = JobRegistry()
