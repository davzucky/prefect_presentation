echo "==> Start prefect server"
prefect server start --use-volume --use-volume --detach

export PREFECT__BACKEND=server
export PREFECT__CLOUD__ENDPOINT=http://localhost
echo "==> Create tenant market"
prefect server create-tenant --name market
prefect create project --skip-if-exists market_data
