from datetime import time
from dask.distributed import LocalCluster
from logging import Logger


class DaskCluster:
    def __init__(self, logger: Logger, nb_workers: int = 4, exit_delay: int = 0):
        self._nb_workers = nb_workers
        self._logger = logger
        self._exit_delay = exit_delay

    def __enter__(self):
        self._logger.info(f"==> Starting local Dask with {self._nb_workers} workers.")
        self._cluster = LocalCluster(n_workers=self._nb_workers, processes=True)
        self._logger.info(
            f"==> local Dask started on  {self._cluster.scheduler_address}."
        )
        self._logger.info(
            f"==> local Dask dashboard available on  {self._cluster.dashboard_link}"
        )
        return self._cluster

    def __exit__(self, *args):
        if self._exit_delay > 0:
            time_info = f"==> Stopping local Dask in {self._exit_delay:,}s."
            self._logger.info(time_info)
            time.sleep(self._exit_delay)
        else:
            self._logger.info("==> Stopping local Dask.")

        self._cluster.close()
        self._logger.info("==> Stopped local Dask.")
