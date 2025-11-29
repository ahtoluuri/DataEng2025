from airflow.datasets import Dataset

CITIBIKE_TRIPS_DATASET = Dataset("clickhouse://citibike/raw_citibike_trips")
WEATHER_DATASET = Dataset("clickhouse://citibike/raw_weather")
