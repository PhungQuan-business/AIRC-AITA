# Check if you have enough memory
docker run --rm "debian:bookworm-slim" bash -c 'numfmt --to iec $(echo $(($(getconf _PHYS_PAGES) * $(getconf PAGE_SIZE))))'

# Fetching docker-compose.yaml
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.10.5/docker-compose.yaml'

# Create nescessary directory
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" >.env

sleep 5

# Initialize Airflow
docker compose up airflow-init

# Running Airflow
docker compose up -d
