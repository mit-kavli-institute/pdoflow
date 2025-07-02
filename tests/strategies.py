import inspect
from datetime import datetime, timedelta
from typing import Callable

from hypothesis import strategies as st

from pdoflow import models as m
from pdoflow import status
from pdoflow.io import Session
from pdoflow.utils import load_function


def foo_workload():
    return st.lists(
        st.tuples(
            st.integers(), st.floats(allow_nan=False, allow_infinity=False)
        ),
        min_size=1,
    )


def failure_workload():
    return st.lists(st.tuples(st.integers()), min_size=1)


def failure_entrypoint():
    return st.just("tests.example_package.failure")


def psql_valid_text(**kwargs):
    return st.text(
        alphabet=st.characters(
            exclude_characters="\x00", exclude_categories=("Cs",)
        ),
        **kwargs,
    )


def get_strategy_for_type(annotation):
    if annotation == int:
        return st.integers()
    elif annotation == float:
        return st.floats()
    elif annotation == str:
        return psql_valid_text()
    else:
        return st.none()


def positional_arguments(func: Callable):
    signature = inspect.signature(func)
    parameters = signature.parameters

    strategies = []
    for param in parameters.values():
        strategy = get_strategy_for_type(param.annotation)
        if param.default == inspect.Parameter.empty:
            strategy = get_strategy_for_type(param.annotation)
        strategies.append(strategy)

    return st.tuples(*strategies)


def keyword_arguments(func: Callable):
    signature = inspect.signature(func)
    parameters = signature.parameters

    strategies = {}
    for param in parameters.values():
        if param.default != inspect.Parameter.empty:
            strategy = get_strategy_for_type(param.annotation)
            strategies[param.name] = strategy

    return st.fixed_dictionaries(strategies)


def workloads(func: Callable):
    return st.tuples(positional_arguments(func), keyword_arguments(func))


@st.composite
def job_postings(draw, entrypoint=failure_entrypoint()):
    posting = draw(
        st.builds(
            m.JobPosting,
            poster=st.one_of(st.none(), psql_valid_text()),
            status=st.sampled_from(status.PostingStatus),
            target_function=psql_valid_text(),
            entry_point=entrypoint,
        )
    )
    with Session() as db:
        db.add(posting)
        db.commit()
        return posting


@st.composite
def _waiting_job(draw, posting: m.JobPosting):

    function = load_function(posting.entry_point)
    return draw(
        st.builds(
            m.JobRecord,
            posting=st.just(posting),
            priority=st.integers(
                min_value=-2_147_483_648, max_value=2_147_483_647
            ),
            tries_remaining=st.integers(min_value=1, max_value=2_147_483_647),
            status=st.just(status.JobStatus.waiting),
            exited_ok=st.none(),
            work_started_on=st.none(),
            completed_on=st.none(),
            positional_arguments=positional_arguments(function),
            keyword_arguments=keyword_arguments(function),
        )
    )


@st.composite
def _finished_job(draw, posting: m.JobPosting):
    function = load_function(posting.entry_point)

    completed_on = draw(st.datetimes(max_value=datetime.now()))
    work_started_on = draw(
        st.datetimes(max_value=completed_on - timedelta(microseconds=1))
    )
    created_on = draw(
        st.datetimes(max_value=work_started_on - timedelta(microseconds=1))
    )

    return draw(
        st.builds(
            m.JobRecord,
            posting=st.just(posting),
            priority=st.integers(
                min_value=-2_147_483_648, max_value=2_147_483_647
            ),
            tries_remaining=st.integers(min_value=0, max_value=2_147_483_647),
            status=st.just(status.JobStatus.done),
            exited_ok=st.just(True),
            created_on=st.just(created_on),
            work_started_on=st.just(work_started_on),
            completed_on=st.just(completed_on),
            positional_arguments=positional_arguments(function),
            keyword_arguments=keyword_arguments(function),
        )
    )


@st.composite
def _errored_job(draw, posting: m.JobPosting):
    function = load_function(posting.entry_point)
    completed_on = draw(st.datetimes(max_value=datetime.now()))
    work_started_on = draw(
        st.datetimes(max_value=completed_on - timedelta(microseconds=1))
    )
    created_on = draw(
        st.datetimes(max_value=work_started_on - timedelta(microseconds=1))
    )

    return draw(
        st.builds(
            m.JobRecord,
            posting=st.just(posting),
            priority=st.integers(
                min_value=-2_147_483_648, max_value=2_147_483_647
            ),
            tries_remaining=st.just(0),
            status=st.just(status.JobStatus.errored_out),
            exited_ok=st.just(False),
            created_on=st.just(created_on),
            work_started_on=st.just(work_started_on),
            completed_on=st.just(completed_on),
            positional_arguments=positional_arguments(function),
            keyword_arguments=keyword_arguments(function),
        )
    )


@st.composite
def job_records(draw, posting: m.JobPosting):
    with Session() as db:
        posting = db.merge(posting)
        record = draw(
            st.one_of(
                _waiting_job(posting),
                _finished_job(posting),
                _errored_job(posting),
            )
        )
        db.add(record)
        db.commit()
        return record


@st.composite
def posting_with_records(draw, **list_kwargs):
    posting = draw(job_postings())
    draw(st.lists(job_records(posting), **list_kwargs))
    return posting


@st.composite
def job_profile_data(draw):
    """Generate JobProfile test data."""
    total_calls = draw(st.integers(min_value=1, max_value=1000))
    total_time = draw(st.floats(min_value=0.001, max_value=100.0))
    return {"total_calls": total_calls, "total_time": total_time}


@st.composite
def profiling_workload(draw):
    """Generate workload for profiling tests."""
    return draw(
        st.lists(
            st.tuples(
                st.integers(min_value=100, max_value=10000),  # cpu iterations
                st.integers(min_value=0, max_value=10),  # sleep ms
                st.integers(min_value=1, max_value=5),  # recursion depth
            ),
            min_size=1,
            max_size=20,
        )
    )


def cpu_intensive_entrypoint():
    return st.just("tests.example_package.cpu_intensive_task")


def nested_calls_entrypoint():
    return st.just("tests.example_package.nested_function_calls")


def mixed_execution_entrypoint():
    return st.just("tests.example_package.mixed_execution_time")


name = "no_unphysical_elapsed"
