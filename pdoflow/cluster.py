"""
This module implements the main entrypoint for PDOFlow.
"""

from typing import Optional

from pdoflow.registry import JobRegistry, Registry


def job(name: Optional[str] = None, registry: JobRegistry = Registry):
    def __internal(func):
        registry.add_job(func, name)
        return func
    return __internal
