echo "==> Stop Dask workers"
pkill -f dask-worker

echo "==> Stop Dask scheduler"
pkill -f dask-scheduler


echo "==> Stop Prefect agent"
pkill -f "prefect agent local"