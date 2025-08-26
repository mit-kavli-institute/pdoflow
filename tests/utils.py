import os

import coverage

from pdoflow.cluster import ClusterProcess


class CoverageWorker(ClusterProcess):
    """ClusterProcess subclass that initializes coverage in subprocess."""

    def _pre_run_init(self):
        """Initialize coverage measurement in this subprocess."""
        # Only start coverage if COVERAGE_PROCESS_START is set
        if os.environ.get("COVERAGE_PROCESS_START"):
            coverage.process_startup()
