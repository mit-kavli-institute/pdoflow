"""Initialize coverage for subprocess tracking in tests.

This module ensures coverage is properly configured for multiprocessing
when tests spawn subprocesses directly (not through pytest-xdist).
"""
import os

import coverage

# Start coverage if running under pytest with coverage
if os.environ.get("COVERAGE_PROCESS_START"):
    coverage.process_startup()
