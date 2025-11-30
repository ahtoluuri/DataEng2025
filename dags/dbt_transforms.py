import os
import shlex
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

from data_assets import CITIBIKE_TRIPS_DATASET, WEATHER_DATASET

DBT_PROJECT_DIR: str = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt_project")
DBT_PROFILES_DIR: str = os.environ.get("DBT_PROFILES_DIR", DBT_PROJECT_DIR)
DBT_LOG_PATH: str = os.environ.get("DBT_LOG_PATH", "/tmp/dbt_logs")
DBT_TARGET_PATH: str = os.environ.get("DBT_TARGET_PATH", "/tmp/dbt_target")

# Ensure dbt installed in the Airflow image is reachable on PATH
PATH_WITH_USER_BIN = f"{os.environ.get('PATH', '')}:/home/airflow/.local/bin"
COMMON_ENV = {
    **os.environ,
    "PATH": PATH_WITH_USER_BIN,
    "DBT_PROFILES_DIR": DBT_PROFILES_DIR,
    "DBT_LOG_PATH": DBT_LOG_PATH,
    "DBT_TARGET_PATH": DBT_TARGET_PATH,
}

with DAG(
    dag_id="dbt_transforms",
    description="Run dbt models once both raw datasets are refreshed",
    schedule=[CITIBIKE_TRIPS_DATASET, WEATHER_DATASET],
    start_date=days_ago(1),
    catchup=False,
) as dag:
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"mkdir -p {shlex.quote(DBT_LOG_PATH)} {shlex.quote(DBT_TARGET_PATH)} && "
            f"cd {shlex.quote(DBT_PROJECT_DIR)} && "
            f"dbt run --profiles-dir {shlex.quote(DBT_PROFILES_DIR)} "
            f"--target-path {shlex.quote(DBT_TARGET_PATH)} --no-partial-parse"
        ),
        env=COMMON_ENV,
        retries=2,
        retry_delay=timedelta(minutes=1),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"mkdir -p {shlex.quote(DBT_LOG_PATH)} {shlex.quote(DBT_TARGET_PATH)} && "
            f"cd {shlex.quote(DBT_PROJECT_DIR)} && "
            f"dbt test --profiles-dir {shlex.quote(DBT_PROFILES_DIR)} "
            f"--target-path {shlex.quote(DBT_TARGET_PATH)} --no-partial-parse"
        ),
        env=COMMON_ENV,
        retries=1,
        retry_delay=timedelta(minutes=1),
    )

    dbt_run >> dbt_test
