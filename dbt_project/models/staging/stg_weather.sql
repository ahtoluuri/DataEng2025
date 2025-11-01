{{ config(materialized='view') }}

select
    observation_time,
    temperature,
    apparent_temperature,
    relative_humidity_2m as relative_humidity,
    precipitation_probability,
    precipitation_mm,
    cloud_coverage,
    wind_speed_10m,
    wind_direction_deg
from {{ source('citibike', 'raw_weather') }}
