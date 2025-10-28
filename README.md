# DataEng2025
project repository of Data Engineering course

## Environment variables
Copy `.env.example` to `.env` and fill in your own values if needed (default values are provided):
```bash
cp .env.example .env
```

## Run
```bash
docker compose up -d
```

## Troubleshooting
If Airflow can't be accessed, this might help:
```bash
mkdir logs dags plugins
sudo chown 50000:0 logs dags plugins
docker compose up airflow-init
```