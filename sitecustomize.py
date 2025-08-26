"""Site customization to enable coverage in subprocesses.

This file is imported automatically by Python when placed in the site-packages
directory or when the directory containing it is in PYTHONPATH.
"""
import os

if os.environ.get("COVERAGE_PROCESS_START"):
    import coverage

    coverage.process_startup()
