export PREFECT__BACKEND=server
export PREFECT__CLOUD__ENDPOINT=http://localhost

echo "==> Start local agent"
prefect agent local start --name local_agent --label local  &

echo "==> Start Dask Scheduler"
dask-scheduler --dashboard > /dev/null 2>&1 &

echo "==> Start 4 Dask Workers"
dask-worker --name agent_1 tcp://192.168.190.252:8786 > /dev/null 2>&1 &
dask-worker --name agent_2 tcp://192.168.190.252:8786 > /dev/null 2>&1 &
dask-worker --name agent_3 tcp://192.168.190.252:8786 > /dev/null 2>&1 &
dask-worker --name agent_4 tcp://192.168.190.252:8786 > /dev/null 2>&1 &