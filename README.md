# DataEng2025
project repository of Data Engineering course

## Environment variables
Copy `.env.example` to `.env` and fill in your own values if needed (default values are provided):
```bash
cp .env.example .env
```

## Run
Start the necessary services
```bash
docker compose up -d
```
Create the tables
```bash
docker exec -it clickhouse-server clickhouse-client --multiquery --queries-file=/sql/01_create_db_and_tables.sql
```

[Clickhouse](http://localhost:8123)

[Airflow](http://localhost:8080)

## Troubleshooting
If Airflow can't be accessed, this might help:
```bash
mkdir logs dags plugins
sudo chown 50000:0 logs dags plugins
docker compose up airflow-init
```