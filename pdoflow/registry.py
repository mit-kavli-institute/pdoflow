"""
This module defines Registries which are convenience classes designed to
easily reference functions to post jobs on.
"""

from dataclasses import dataclass, field
from itertools import zip_longest
from typing import Callable, Optional, Union

from pdoflow.io import Session
from pdoflow.models import JobPosting, JobRecord
from pdoflow.status import JobStatus, PostingStatus
from pdoflow.utils import get_module_path


@dataclass
class _JobDataClass:
    """
    A in internal dataclass to encapsulate logic for posting work to
    the database with an arbitrary callable.
    """

    target: Callable

    def post_work(self, posargs: list[tuple], kwargs: list[dict]):
        """
        Post the given work to the remote database.

        Parameters
        ----------
        posargs: list[tuple]
            A list of tuples representing the positional arguments
            that will be pushed to the database. These arguments must
            be JSON serializable.

        kwargs: list[dict]
            A list of dictionaries representing keyword arguments
            that will be pushed to the database. The ordinal position
            of each dictionary is in direct correlation to the
            positional tuple list.

        Notes
        -----
        You may pass an empty list for keyword lists.
        The work posted will iterate over positional arguments and draw
        NULL for keyword arguments in such an occasion.
        """
        posting = JobPosting(
            target_function=self.target.__name__,
            entry_point=get_module_path(self.target),
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
    """
    This dataclass tracks all registered functions and allows easier
    reference to those functions when posting work to the database.
    Attemps are made to keep the registered function names unique to
    reduce ambiguity when reading database records.

    Multiple Registries may be instantiated and referenced when
    functions are decorated. This form of registration, again, is not
    a database requirement but purely attempts to keep things organized.

    Notes
    -----
    It isn't a strict database requiement that all job postings have
    unique function names. This is simply inplace for human readibility
    and organizational cleanliness.
    """

    _job_defs: dict[str, _JobDataClass] = field(default_factory=dict)

    def __getitem__(self, key: Union[str, Callable]) -> _JobDataClass:
        """
        Attempt to resolve the function or function alias as
        _JobDataClass.

        Raises
        ------
        KeyError:
            The key was not found within the Registry.
        """
        lookup_name = self.resolve_name(key)
        return self._job_defs[lookup_name]

    def __contains__(self, key: Union[str, Callable]) -> bool:
        """
        Returns True if the given function or function alias is
        found in the Registry instance.
        """
        lookup_name = self.resolve_name(key)
        return lookup_name in self._job_defs

    def add_job(self, func: Callable, name_override: Optional[str] = None):
        """
        Registers the provided function within the Registry instance.

        Parameters
        ----------
        func: Callable
            A function which is executable. This function must be defined
            on a file which can be loaded by the Pool in the future.
            This method will not work on functions defined dynamically
            or within an interative Python shell.
        name_override: Optional[str]
            Instead of using the defined function name you may pass an
            override.
        """
        name = name_override if name_override is not None else func.__name__

        if name in self:
            raise ValueError(f"Job name {name} already defined in registery!")

        self._job_defs[name] = _JobDataClass(target=func)
        return func

    @staticmethod
    def resolve_name(key: Union[str, Callable]) -> str:
        return key if isinstance(key, str) else key.__name__

    def clear_registry(self):
        """
        Clear the registry.
        """
        self._job_defs = {}


Registry = JobRegistry()
