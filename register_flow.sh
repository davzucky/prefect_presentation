export PREFECT__BACKEND=server
export PREFECT__CLOUD__ENDPOINT=http://localhost

echo "==> Register Job Extract_market_data"
prefect register --project market_data -p flows/extract_market_data.py