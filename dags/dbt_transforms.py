from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from data_assets import CITIBIKE_TRIPS_DATASET, WEATHER_DATASET
import os

DBT_CONTAINER_NAME: str = os.environ.get("DBT_CONTAINER_NAME", "dbt")
DBT_PROJECT_DIR: str = os.environ.get("DBT_PROJECT_DIR_IN_CONTAINER", "/dbt")

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
            f"docker exec {DBT_CONTAINER_NAME} "
            f"bash -lc 'cd {DBT_PROJECT_DIR} && dbt run --profiles-dir {DBT_PROJECT_DIR}'"
        ),
        env={"PATH": os.environ.get("PATH", "")},
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"docker exec {DBT_CONTAINER_NAME} "
            f"bash -lc 'cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROJECT_DIR}'"
        ),
        env={"PATH": os.environ.get("PATH", "")},
    )

    dbt_run >> dbt_test
