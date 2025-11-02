# DataEng2025
Here you can find the instructions on how to run the Data Engineering 2025 project for team 11

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
## Airflow DAGs
DAG citibike_monthly_ingest:

<img width="1155" height="153" alt="image" src="https://github.com/user-attachments/assets/10ed9bcf-58cf-43f2-abd6-62ae1b5ce3e2" />

DAG weather_monthly_ingest:

<img width="647" height="153" alt="image" src="https://github.com/user-attachments/assets/b11d71ff-46ed-4db1-8c8f-0a780b7de3a2" />



