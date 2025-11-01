from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import requests
import zipfile
import io

CITIBIKE_BASE_URL: str = "https://s3.amazonaws.com/tripdata/"
CLICKHOUSE_CONN_ID: str = "clickhouse_default"
TABLE_NAME: str = "citibike.raw_citibike_trips"

def get_file_name(execution_date: datetime, months_ago=1) -> str:
    # The monthly trip data bucket links are in the following format:
    # https://s3.amazonaws.com/tripdata/YYYYMM-citibike-tripdata.zip
    # Returns the correct file name for the execution date
    target_month = execution_date - relativedelta(months=months_ago)
    return f"{target_month.strftime('%Y%m')}-citibike-tripdata.zip"

def download_and_extract(**context) -> None:
    execution_date = context['logical_date']
    hook = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONN_ID)

    for m in [1,2]:
        check_month = (execution_date - relativedelta(months=m))
        month_str = check_month.strftime('%Y%m')
        print(f"Checking if data already exists for {month_str}")
        query = f"""
            SELECT COUNT(*) FROM {TABLE_NAME} WHERE toYYYYMM(started_at) = {month_str}
        """
        result = hook.execute(query)
        count = result[0][0]
        if count > 0:
            print(f"Data for {month_str} already exists in {TABLE_NAME}. Skipping download.")
            context['ti'].xcom_push(key='csv_path', value=None)
            return
    
    response = None
    downloaded_month = None

    for m in [1,2]:
        file_name = get_file_name(execution_date, months_ago=m)
        url = CITIBIKE_BASE_URL + file_name
        print(f"Trying to download {url}")
        r = requests.get(url)
        if r.status_code == 200:
            response = r
            downloaded_month = execution_date - relativedelta(months=m)
            break
        elif r.status_code == 404:
            print(f"Data not found for {file_name}, trying earlier month")
        else:
            r.raise_for_status()
    
    if response is None:
        raise FileNotFoundError("No data found for the last 2 months.")
   
    dfs = []
    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        csv_files = [n for n in z.namelist() if n.endswith('.csv') and not n.startswith('__MACOSX/')]
        print(f"Found {len(csv_files)} CSV files: {csv_files}")
        for csv_file in csv_files:
            with z.open(csv_file) as f:
                df_part = pd.read_csv(f)
                dfs.append(df_part)
        df = pd.concat(dfs, ignore_index=True)
        print(f"Combined {len(dfs)} CSV files, total rows: {len(df)}")

    #df['started_at'] = pd.to_datetime(df['started_at']).dt.strftime('%Y-%m-%d %H:%M:%S')
    #df['ended_at'] = pd.to_datetime(df['ended_at']).dt.strftime('%Y-%m-%d %H:%M:%S')

    tmp_path = f"/tmp/citibike_trip_data_{downloaded_month.strftime('%Y%m')}.csv"
    df.to_csv(tmp_path, index=False)
    context['ti'].xcom_push(key='csv_path', value=tmp_path)
    print(f"Saved merged CSV to {tmp_path}")

# This is slow
# def load_to_clickhouse(**context):
#     file_path = context['ti'].xcom_pull(key='csv_path', task_ids='download_and_extract')
#     df = pd.read_csv(file_path)
#     hook = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONN_ID)
    
#     print("Converting date data types")
#     df['started_at'] = pd.to_datetime(df['started_at']).dt.to_pydatetime()
#     df['ended_at'] = pd.to_datetime(df['ended_at']).dt.to_pydatetime()

#     print("Filling missing values for string columns")
#     for col in ['ride_id', 'rideable_type', 'start_station_name', 'start_station_id', 'end_station_name', 'end_station_id', 'member_casual']:
#         df[col] = df[col].fillna("").astype(str)

#     print("Filling missing values for numeric columns")
#     for col in ['start_lat', 'start_lng', 'end_lat', 'end_lng']:
#         df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

#     # Duplicate runs are inserted, but Clickhouse will optimize it eventually
#     # just selecting shows all rows with duplicates, "select * from table_name FINAL" shows the most recent
#     insert_sql = f"""
#         INSERT INTO {TABLE_NAME} (
#             ride_id, rideable_type, started_at, ended_at,
#             start_station_name, start_station_id,
#             end_station_name, end_station_id,
#             start_lat, start_lng, end_lat, end_lng,
#             member_casual
#         ) VALUES
#     """
#     print("Creating tuples")
#     data = [tuple(x) for x in df.to_numpy()]
#     print(f"Inserting {len(df)} rows into {TABLE_NAME}")
#     hook.execute(insert_sql, data)

def load_to_clickhouse(**context):
    file_path = context['ti'].xcom_pull(key='csv_path', task_ids='download_and_extract')

    if not file_path:
        print("No new CSV file to load.")
        return
    
    url = "http://clickhouse-server:8123/"
    table = TABLE_NAME
    with open(file_path, 'rb') as f:
        r = requests.post(
            url + f"?query=INSERT INTO {table} FORMAT CSVWithNames",
            data=f,
            auth=('admin', ''),
            headers={'Content-Type': 'text/plain'}
        )
    print(r.text)


def check_is_ride_id_null(**context):
    hook = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONN_ID)
    query = f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE ride_id IS NULL"
    result = hook.execute(query)
    null_count = result[0][0]
    if null_count > 0:
        print(f"Data quality check: {null_count} NULL ride_id values found")
        hook.execute(f"ALTER TABLE {TABLE_NAME} DELETE WHERE ride_id IS NULL")
        #raise ValueError(f"Data quality check failed: {null_count} NULL ride_id values found")
    else:
        print("No NULL ride_id values found")

def check_is_end_station_id_null(**context):
    hook = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONN_ID)
    query = f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE end_station_id = ''"
    result = hook.execute(query)
    null_count = result[0][0]
    if null_count > 0:
        print(f"Data quality check: {null_count} missing end_station_id values found")
        hook.execute(f"ALTER TABLE {TABLE_NAME} DELETE WHERE end_station_id = ''")
        print("Removed rows where end_station_id is missing")

with DAG(
    dag_id='citibike_monthly_ingest',
    description='Monthly ingestion of raw Citibike data into Clickhouse',
    schedule_interval='0 0 10 * *',
    start_date=days_ago(1)
) as dag:
    download_and_extract_task = PythonOperator(
        task_id='download_and_extract',
        python_callable=download_and_extract,
        provide_context=True
    )

    load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_to_clickhouse,
        provide_context=True
    )

    check_ride_id_null = PythonOperator(
        task_id='check_ride_id_null',
        python_callable=check_is_ride_id_null
    )

    check_end_station_id_null = PythonOperator(
        task_id='check_end_station_id_null',
        python_callable=check_is_end_station_id_null
    )

    download_and_extract_task >> load_data >> check_ride_id_null >> check_end_station_id_null