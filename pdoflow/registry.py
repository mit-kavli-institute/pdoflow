from dataclasses import dataclass, field
from itertools import zip_longest
from typing import Callable, Optional

from pdoflow.io import Session
from pdoflow.models import JobPosting, JobRecord
from pdoflow.status import JobStatus, PostingStatus
from pdoflow.utils import get_module_path


@dataclass
class Job:
    target: Callable

    def post_work(self, posargs: list[tuple], kwargs: list[dict]):
        posting = JobPosting(
            target_function=self.target.__name__,
            entry_point=get_module_path(self.target).resolve(),
            status=PostingStatus.executing,
        )

        with Session() as db:
            db.add(posting)
            db.flush()

            payload = [
                JobRecord(
                    posting=posting,
                    priority=1,
                    positional_arguments=args,
                    keyword_arguments=kwargs,
                    tries_remaining=3,
                    status=JobStatus.waiting,
                )
                for args, kwargs in zip_longest(posargs, kwargs)
            ]
            db.add_all(payload)
            db.commit()
            return posting.id, [job.id for job in payload]


@dataclass
class JobRegistry:
    _job_defs: dict[str, Job] = field(default_factory=dict)

    def __getitem__(self, key: str | Callable) -> Job:
        lookup_name = self.resolve_name(key)
        return self._job_defs[lookup_name]

    def __contains__(self, key: str | Callable) -> bool:
        lookup_name = self.resolve_name(key)
        return lookup_name in self._job_defs

    def add_job(self, func: Callable, name_override: Optional[str] = None):
        name = name_override if name_override is not None else func.__name__

        if name in self:
            raise ValueError(f"Job name {name} already defined in registery!")

        self._job_defs[name] = Job(target=func)
        return func

    @staticmethod
    def resolve_name(key: str | Callable) -> str:
        return key if isinstance(key, str) else key.__name__

    def clear_registry(self):
        self._job_defs = {}


Registry = JobRegistry()
