import coverage

from pdoflow.cluster import ClusterProcess


class CoverageWorker(ClusterProcess):
    def _pre_run_init(self):
        coverage.process_startup()
