{{
  config(
    materialized='view'
  )
}}

SELECT
    apparent_temperature,
    relative_humidity_2m,
    precipitation_probability,
    cloud_coverage,
    wind_speed_10m,
    observation_time
FROM {{ source('citibike', 'raw_weather') }}