import coverage

from pdoflow.cluster import ClusterProcess


class CoverageWorker(ClusterProcess):
    def run(self):
        coverage.process_startup()
        super().run()
