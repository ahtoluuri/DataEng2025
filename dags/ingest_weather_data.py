from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
import requests

CLICKHOUSE_CONN_ID: str = "clickhouse_default"
TABLE_NAME: str = "citibike.raw_weather"

def get_date_range(execution_date: datetime):
    start = (execution_date - relativedelta(months=2)).replace(day=1) - timedelta(days=1)
    end = execution_date.replace(day=1) #- timedelta(days=1)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")

def fetch_weather(**context):
    exec_date = context["logical_date"]
    start_date, end_date = get_date_range(exec_date)

    url = (
        "https://api.open-meteo.com/v1/forecast?"
        "latitude=40.7143&longitude=-74.006&"
        "hourly=temperature_2m,wind_speed_10m,precipitation,precipitation_probability,apparent_temperature,relative_humidity_2m,wind_direction_10m,cloud_cover,uv_index&"
        f"start_date={start_date}&end_date={end_date}"
        )
    
    print(f"Fetching weather data for {start_date} - {end_date}")
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    df = pd.DataFrame({
        "observation_time": data["hourly"]["time"],
        "temperature": data["hourly"]["temperature_2m"],
        "apparent_temperature": data["hourly"]["apparent_temperature"],
        "relative_humidity_2m": data["hourly"]["relative_humidity_2m"],
        "precipitation_probability": data["hourly"]["precipitation_probability"],
        "precipitation_mm": data["hourly"]["precipitation"],
        "cloud_coverage": data["hourly"]["cloud_cover"],
        "uv_index": data["hourly"]["uv_index"],
        "wind_speed_10m": data["hourly"]["wind_speed_10m"],
        "wind_direction_deg": data["hourly"]["wind_direction_10m"],
    })
    df["observation_time"] = pd.to_datetime(df["observation_time"]).dt.strftime("%Y-%m-%d %H:%M:%S")

    tmp_path = f"/tmp/weather_data{start_date[:7].replace('-', '')}.csv"
    df.to_csv(tmp_path, index=False)
    context["ti"].xcom_push(key="weather_csv_path", value=tmp_path)
    print(f"Saved weather CSV to {tmp_path}")

def load_to_clickhouse(**context):
    file_path = context["ti"].xcom_pull(key="weather_csv_path", task_ids="fetch_weather")
    url = "http://clickhouse-server:8123/"

    df = pd.read_csv(file_path)
    start_date = df["observation_time"].min()
    end_date = df["observation_time"].max()
    delete_query = f"""
        ALTER TABLE {TABLE_NAME}
        DELETE WHERE observation_time >= toDateTime('{start_date}') AND observation_time <= toDateTime('{end_date}')
    """
    print(f"Deleting existing rows for {start_date} - {end_date}")
    requests.post(url, params={"query": delete_query}, auth=("admin", ""))
    
    with open(file_path, "rb") as f:
        r = requests.post(
            url + f"?query=INSERT INTO {TABLE_NAME} FORMAT CSVWithNames",
            data=f,
            auth=("admin", ""),
            headers={"Content-Type": "text/plain"}
        )
    print(r.text)

with DAG(
    dag_id="weather_monthly_ingest",
    description="Monthly ingestion of Open-Meteo weather data into Clickhouse",
    schedule_interval="0 0 10 * *",
    start_date=days_ago(1),
    catchup=False
) as dag:
    fetch_weather_task = PythonOperator(
        task_id="fetch_weather",
        python_callable=fetch_weather,
        provide_context=True
    )

    load_data_task = PythonOperator(
        task_id="load_weather_to_clickhouse",
        python_callable=load_to_clickhouse,
        provide_context=True
    )
    
    fetch_weather_task >> load_data_task